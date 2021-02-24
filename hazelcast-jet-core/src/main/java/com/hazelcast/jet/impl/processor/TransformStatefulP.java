/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class TransformStatefulP<T, K, S, R> extends AbstractProcessor {
    public static final String STATE_IMAP_NAMES_LIST_NAME = "statemapnames";
    public static final String VERTEX_TO_SS_IMAP_NAME = "snapshotmapnames";

    private static final int HASH_MAP_INITIAL_CAPACITY = 16;
    private static final float HASH_MAP_LOAD_FACTOR = 0.75f;
    private static final Watermark FLUSHING_WATERMARK = new Watermark(Long.MAX_VALUE);

    @Probe(name = "lateEventsDropped")
    private final Counter lateEventsDropped = SwCounter.newSwCounter();

    private final long ttl;
    private final Function<? super T, ? extends K> keyFn;
    private final ToLongFunction<? super T> timestampFn;
    private final Function<K, TimestampedItem<S>> createIfAbsentFn;
    private final TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn;
    @Nullable
    private final TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn;
    private final Map<K, TimestampedItem<S>> keyToState =
            new LinkedHashMap<>(HASH_MAP_INITIAL_CAPACITY, HASH_MAP_LOAD_FACTOR, true);
    private IMap<K, S> keyToStateIMap;
    private IMap<SnapshotIMapKey<K>, S> snapshotIMap;
    private final FlatMapper<T, R> flatMapper = flatMapper(this::flatMapEvent);

    private final FlatMapper<Watermark, Object> wmFlatMapper = flatMapper(this::flatMapWm);
    private final EvictingTraverser evictingTraverser = new EvictingTraverser();
    private final Traverser<?> evictingTraverserFlattened = evictingTraverser.flatMap(x -> x);

    private long currentWm = Long.MIN_VALUE;
    private Traverser<? extends Entry<?, ?>> snapshotTraverser;
    private boolean inComplete;

    // Stores vertex name
    private String vertexName;

    // Timer variables
    private long snapshotIMapStartTime;
    private long snapshotTraverserStartTime;

    // Snapshot variables
    private long snapshotId; // Snapshot ID
    private CompletableFuture<Void> snapshotFuture; // To IMap snapshot future

    private static class SnapshotQueueItem<K, S> {
        public enum Operation {
            PUT,
            DELETE
        }

        public final Operation operation;
        public final Entry<SnapshotIMapKey<K>, S> entry;

        SnapshotQueueItem(Operation operation, Entry<SnapshotIMapKey<K>, S> entry) {
            this.operation = operation;
            this.entry = entry;
        }
    }

    // Queue for snapshot entries when they cannot immediately be put to the map
    private final Queue<SnapshotQueueItem<K, S>> snapshotQueue = new ArrayDeque<>();
    // True when using queue for snapshot IMap updates, so the full state put can be done first
    private boolean useQueue;

    public TransformStatefulP(
            long ttl,
            @Nonnull Function<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunction<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        this.ttl = ttl > 0 ? ttl : Long.MAX_VALUE;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
        this.createIfAbsentFn = k -> new TimestampedItem<>(Long.MIN_VALUE, createFn.get());
        this.statefulFlatMapFn = statefulFlatMapFn;
        this.onEvictFn = onEvictFn;
    }

    /**
     * Helper method populating the name Lists.
     * @param hz Hazelcast instance
     * @param stateMapName State IMap name
     * @param snapshotMapName Snapshot IMap name
     */
    private void populateNameLists(HazelcastInstance hz, String stateMapName, String snapshotMapName) {
        List<String> stateMapNames = hz.getList(STATE_IMAP_NAMES_LIST_NAME);
        stateMapNames.add(stateMapName);
        Map<String, String> snapshotMapNames = hz.getMap(VERTEX_TO_SS_IMAP_NAME);
        snapshotMapNames.put(vertexName, snapshotMapName);
    }



    /**
     * Helper for getting an IMap config.
     * @param mapName Name of the IMap
     * @return The MapConfig with the correct name
     */
    private MapConfig getMapConfig(String mapName) {
        final MapConfig[] mapConfig = {new MapConfig()
                .setName(mapName)
                .setBackupCount(0)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.BINARY)
                .setStatisticsEnabled(false)
                .setReadBackupData(false)};
        return mapConfig[0];
    }

    /**
     * Helper method for state IMap name.
     * @param hz The hazelcast instance
     * @return The IMap name consisting of node name, vertex name, and memory address of processor
     */
    private String getStateImapName(HazelcastInstance hz) {
//        // Get local node name
//        String hzInstanceName = hz.getName();
//        try {
//            hzInstanceName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        // Return IMap name as combination of node, vertex, and processor address
//        //return String.format("%s-%s-%s", hzInstanceName, vertexName, super.toString().split("@")[1]);
        return vertexName;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        // Get HazelCastInstance
        Collection<HazelcastInstance> hzs = Hazelcast.getAllHazelcastInstances();
        HazelcastInstance hz = hzs.toArray(new HazelcastInstance[0])[0];

        // Get Map names
        this.vertexName = context.vertexName();
        String mapName = getStateImapName(hz);
        String snapshotMapName = MessageFormat.format("snapshot-{0}", mapName);

        // Add map config
        Config config = hz.getConfig();
        MapConfig stateMapConfig = getMapConfig(mapName);
        MapConfig snapshotMapConfig0 = getMapConfig(snapshotMapName);
        config.addMapConfig(stateMapConfig);
        config.addMapConfig(snapshotMapConfig0);

        // Add map names to Distributed List
        populateNameLists(hz, mapName, snapshotMapName);

        keyToStateIMap = hz.getMap(mapName);
        snapshotIMap = hz.getMap(snapshotMapName);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
    }

    /**
     * Helper method that processes the internal snapshot Queue until it is empty.
     */
    private void processQueue() {
        // Process the entire queue first
        while (!snapshotQueue.isEmpty()) {
            SnapshotQueueItem<K, S> item = snapshotQueue.remove();
            if (item.operation == SnapshotQueueItem.Operation.PUT) {
                // PUT operation
                snapshotIMap.set(item.entry.getKey(), item.entry.getValue()); // Put to snapshot
            } else {
                // DELETE operation
                snapshotIMap.evict(item.entry.getKey()); // Evict from snapshot
            }
        }
        // Queue emptied, disable useQueue state
        useQueue = false;
    }


    /**
     * Helper method for processing items to snapshot IMap.
     * @param key Key of the item
     * @param state State object
     * @param snapshotId Snapshot ID corresponding to the state entry
     * @param operation Put or DELETE operation
     */
    private void processSnapshotItem(@Nonnull K key,
                                     S state,
                                     long snapshotId,
                                     @Nonnull SnapshotQueueItem.Operation operation) {
        checkSnapshotFuture(true);
        if (snapshotFuture == null) {
            if (useQueue) {
                processQueue();
            }
            // Putting all state in snapshot and queue is emptied so we can directly put/delete the state to the IMap
            if (operation == SnapshotQueueItem.Operation.PUT) {
                snapshotIMap.set(new SnapshotIMapKey<>(key, snapshotId), state);
            } else {
                snapshotIMap.evict(new SnapshotIMapKey<>(key, snapshotId));
            }
        } else {
            // Putting all state in snapshot is still in progress so instead put entry to queue
            if (!useQueue) {
                // Enable useQueue state
                useQueue = true;
                if (!snapshotQueue.isEmpty()) {
                    getLogger().severe("Invalid state, snapshotQueue should be empty but was: " + snapshotQueue.size());
                }
            }
            // Offer entry to queue
            snapshotQueue.offer(
                    new SnapshotQueueItem<>(
                            operation,
                            new AbstractMap.SimpleEntry<>(
                                    new SnapshotIMapKey<>(key, snapshotId),
                                    state)));
        }
    }

    @Nonnull
    private Traverser<R> flatMapEvent(T event) {
        long timestamp = timestampFn.applyAsLong(event);
        if (timestamp < currentWm && ttl != Long.MAX_VALUE) {
            logLateEvent(getLogger(), currentWm, event);
            lateEventsDropped.inc();
            return Traversers.empty();
        }
        K key = keyFn.apply(event);
        TimestampedItem<S> tsAndState = keyToState.computeIfAbsent(key, createIfAbsentFn);
        tsAndState.setTimestamp(max(tsAndState.timestamp(), timestamp));
        S state = tsAndState.item();
        Traverser<R> result = statefulFlatMapFn.apply(state, key, event);
        keyToStateIMap.set(key, state); // Put to live state IMap
        processSnapshotItem(key, state, snapshotId, SnapshotQueueItem.Operation.PUT); // Put state to snapshot IMap
        return result;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return wmFlatMapper.tryProcess(watermark);
    }

    private Traverser<?> flatMapWm(Watermark wm) {
        currentWm = wm.timestamp();
        evictingTraverser.reset(wm);
        return evictingTraverserFlattened;
    }

    @Override
    public boolean complete() {
        inComplete = true;
        // flush everything with a terminal watermark
        return tryProcessWatermark(FLUSHING_WATERMARK);
    }

    private class EvictingTraverser implements Traverser<Traverser<?>> {
        private Iterator<Entry<K, TimestampedItem<S>>> keyToStateIterator;
        private final ResettableSingletonTraverser<Watermark> wmTraverser = new ResettableSingletonTraverser<>();

        void reset(Watermark wm) {
            keyToStateIterator = keyToState.entrySet().iterator();
            if (wm == FLUSHING_WATERMARK) {
                // don't forward the flushing watermark
                return;
            }
            wmTraverser.accept(wm);
        }

        @Override
        public Traverser<?> next() {
            if (keyToStateIterator == null) {
                return null;
            }
            while (keyToStateIterator.hasNext()) {
                Entry<K, TimestampedItem<S>> entry = keyToStateIterator.next();
                long lastTouched = entry.getValue().timestamp();
                if (lastTouched >= Util.subtractClamped(currentWm, ttl)) {
                    break;
                }
                // Evict from live state IMap
                keyToStateIMap.evict(entry.getKey());
                // Evict from snapshot IMap
                processSnapshotItem(entry.getKey(), null, snapshotId, SnapshotQueueItem.Operation.DELETE);
                keyToStateIterator.remove();
                if (onEvictFn != null) {
                    getLogger().info(String.format(
                            "Evicting key '%s' with value: %s",
                            entry.getKey(),
                            entry.getValue().item()));
                    return onEvictFn.apply(entry.getValue().item(), entry.getKey(), currentWm);
                }
            }
            keyToStateIterator = null;
            return wmTraverser;
        }
    }

    private enum SnapshotKeys {
        WATERMARK
    }

    /**
     * Helper checking snapshot future, prints the time it took to complete the future.
     * @param clearFuture If true it will set the future to null in case it is done, otherwise it will do nothing
     */
    private void checkSnapshotFuture(boolean clearFuture) {
        // When snapshot future is done
        if (snapshotFuture != null && snapshotFuture.isDone()) {
            long snapshotFinish = System.nanoTime() - snapshotIMapStartTime;
            getLogger().info("Snapshot IMap time: " + snapshotFinish);
            if (clearFuture) {
                snapshotFuture = null;
            }
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (inComplete) {
            // If we are in completing phase, we can have a half-emitted item. Instead of finishing it and
            // writing a snapshot, we finish the final items and save no state.
            return complete();
        }

        // Check if snapshot future is not cleared while traverser is finished
        if (snapshotFuture != null && snapshotTraverser == null) {
            // Check if previous snapshot is done and clear it if done
            checkSnapshotFuture(true);
            if (snapshotFuture == null) {
                // No puts/deletes were made to snapshot IMap, so snapshot future was never cleared, no problem
                getLogger().info("No changes to snapshot since last snapshot");
            } else {
                // The previous snapshot was not yet finished,
                // big problem as snapshots can apparently not keep up with snapshot rate
                getLogger().severe("Invalid state, got to saveToSnapshot() again while previous one was " +
                        "not finished! Consider increasing time between snapshots!");
            }
        }

        // Only execute the remove and snapshot once
        if (snapshotFuture == null) {
            snapshotIMapStartTime = System.nanoTime();
            snapshotIMap.executeOnEntries(new EvictSnapshotProcessor<>(snapshotId)); // Evict older snapshot entries
            snapshotId++; // Increment snapshot ID
            // Put current state in IMap as starting point, do not wait for this
            snapshotFuture = snapshotIMap.setAllAsync(keyToState.entrySet().stream().collect(Collectors.toMap(
                    entry -> new SnapshotIMapKey<>(entry.getKey(), snapshotId),
                    entry -> entry.getValue().item()))).toCompletableFuture();
        }

        // Only print if it is done, don't clear snapshot future yet
        checkSnapshotFuture(false);

        // Traditional snapshot traverser
        if (snapshotTraverser == null) {
            snapshotTraverserStartTime = System.nanoTime();
            snapshotTraverser = Traversers.<Entry<?, ?>>traverseIterable(keyToState.entrySet())
                    .append(entry(broadcastKey(SnapshotKeys.WATERMARK), currentWm))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        long snapshotTraverserTime = System.nanoTime() - snapshotTraverserStartTime;
                        getLogger().info("Snapshot traverser time: " + snapshotTraverserTime);
                    });
        }
        if (emitFromTraverserToSnapshot(snapshotTraverser)) {
            // Done with emitting from traverse to snapshot
            checkSnapshotFuture(true);
            return true;
        }
        // Not done yet
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            assert ((BroadcastKey<?>) key).key() == SnapshotKeys.WATERMARK : "Unexpected " + key;
            long wm = (long) value;
            currentWm = (currentWm == Long.MIN_VALUE) ? wm : min(currentWm, wm);
        } else {
            TimestampedItem<S> old = keyToState.put((K) key, (TimestampedItem<S>) value);
            keyToStateIMap.set((K) key, (S) value);
            assert old == null : "Duplicate key '" + key + '\'';
        }
    }
}
