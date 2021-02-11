/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

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
    private final Set<K> evictKeys = new HashSet<>();
    private final Map<SnapshotIMapKey<K>, S> snapshotDelta = new HashMap<>();
    private IMap<K, S> keyToStateIMap;
    private IMap<SnapshotIMapKey<K>, S> snapshotIMap;
    private final FlatMapper<T, R> flatMapper = flatMapper(this::flatMapEvent);

    private final FlatMapper<Watermark, Object> wmFlatMapper = flatMapper(this::flatMapWm);
    private final EvictingTraverser evictingTraverser = new EvictingTraverser();
    private final Traverser<?> evictingTraverserFlattened = evictingTraverser.flatMap(x -> x);

    private long currentWm = Long.MIN_VALUE;
    private Traverser<? extends Entry<?, ?>> snapshotTraverser;
    private CompletableFuture<Void> snapshotFuture;
    private boolean inComplete;

    // Stores vertex name
    private String vertexName;

    // Timer variables
    private long snapshotIMapStartTime;
    private long snapshotTraverserStartTime;

    // Snapshot id
    private long snapshotId;

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
        String snapshotMapName0 = MessageFormat.format("snapshot-{0}-0", mapName);
        String snapshotMapName1 = MessageFormat.format("snapshot-{0}-1", mapName);

        // Add map config
        Config config = hz.getConfig();
        MapConfig stateMapConfig = getMapConfig(mapName);
        MapConfig snapshotMapConfig0 = getMapConfig(snapshotMapName0);
        MapConfig snapshotMapConfig1 = getMapConfig(snapshotMapName1);
        config.addMapConfig(stateMapConfig);
        config.addMapConfig(snapshotMapConfig0);
        config.addMapConfig(snapshotMapConfig1);

        // Add map names to Distributed List
        populateNameLists(hz, mapName, snapshotMapName0);

        keyToStateIMap = hz.getMap(mapName);
        snapshotIMap = hz.getMap(snapshotMapName0);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return flatMapper.tryProcess((T) item);
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
        snapshotDelta.put(new SnapshotIMapKey<>(key, snapshotId), state); // Put to snapshot delta
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
                keyToStateIMap.evict(entry.getKey());
                evictKeys.add(entry.getKey());
                snapshotDelta.remove(new SnapshotIMapKey<>(entry.getKey(), snapshotId));
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

    @Override
    public boolean saveToSnapshot() {
        if (inComplete) {
            // If we are in completing phase, we can have a half-emitted item. Instead of finishing it and
            // writing a snapshot, we finish the final items and save no state.
            return complete();
        }

        // Snapshot IMap which has the same structure as the key to state IMap
        if (snapshotFuture == null) {
            snapshotIMapStartTime = System.nanoTime();
            snapshotIMap.executeOnEntries(new EvictingEntryProcessor<>(evictKeys));
            evictKeys.clear();
            snapshotFuture = snapshotIMap.setAllAsync(
                    snapshotDelta)
                    .toCompletableFuture();
            snapshotId++;
        }
        if (snapshotFuture.isDone()) {
            snapshotDelta.clear();
            long snapshotFinish = System.nanoTime() - snapshotIMapStartTime;
            getLogger().info("Snapshot IMap time: " + snapshotFinish);
            snapshotFuture = null;
        }

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
        return (snapshotFuture == null) && emitFromTraverserToSnapshot(snapshotTraverser);
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
