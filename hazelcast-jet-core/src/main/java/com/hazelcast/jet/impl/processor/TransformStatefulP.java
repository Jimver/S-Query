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
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
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
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
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
    // Name of IMap containing all live state Imap names, where key = vertex name and value = imap name
    public static final String VERTEX_TO_LIVE_STATE_IMAP_NAME = "statemapnames";
    // Name of IMap containing all snapshot state Imap names, where key = vertex name and value = imap name
    public static final String VERTEX_TO_SS_STATE_IMAP_NAME = "snapshotmapnames";
    // Name of IMap containing all snapshot ID AtomicLong names, where key = vertex name and value = AtomicLong name
    public static final String VERTEX_TO_SS_ID_IMAP_NAME = "snapshotidnames";

    // True to verify local and in memory key set are equal
    private static final boolean VERIFY_KEYSET = false;
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

    // Store vertex and job name
    private String vertexName;
    private String jobName;

    // Timer variables
    private long snapshotIMapStartTime;
    private long snapshotIMapEndTime;
    private long snapshotTraverserStartTime;
    private long snapshotTraverserEndTime;
    private long distributedSsIdStartTime;
    private long distributedSsIdEndTime;
    private long countDownStartTime;
    private long countDownEndTime;

    // Snapshot variables
    private long snapshotId; // Snapshot ID
    private IAtomicLong distributedSnapshotId; // Snapshot Id counter distributed
    private ICountDownLatch ssCountDownLatch; // Snapshot countdown latch
    private CompletableFuture<Void> snapshotFuture; // To IMap snapshot future
    private CompletableFuture<Void> distributedSnapshotIdFuture; // Distributed atomic long future
    private CompletableFuture<Void> countDownFuture; // Snapshot countdown latch future
    private final boolean waitForFutures; // Wait for futures
    private final long snapshotDelayMillis; // Delay snapshot future by this amount of milliseconds, used for testing

    static class SnapshotQueueItem<K, S> {
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
        this(ttl, keyFn, timestampFn, createFn, statefulFlatMapFn, onEvictFn,
                true,
                0L);
    }

    public TransformStatefulP(
            long ttl,
            @Nonnull Function<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunction<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn,
            boolean waitForFutures,
            long snapshotDelayMillis
    ) {
        this.ttl = ttl > 0 ? ttl : Long.MAX_VALUE;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
        this.createIfAbsentFn = k -> new TimestampedItem<>(Long.MIN_VALUE, createFn.get());
        this.statefulFlatMapFn = statefulFlatMapFn;
        this.onEvictFn = onEvictFn;
        this.waitForFutures = waitForFutures;
        this.snapshotDelayMillis = snapshotDelayMillis;
    }

    /**
     * Getter for snapshot queue size.
     *
     * @return The size of the snapshot queue
     */
    public int getQueueSize() {
        return snapshotQueue.size();
    }

    /**
     * Getter for useQueue.
     *
     * @return true if queue is used, false otherwise
     */
    public boolean isUseQueue() {
        return useQueue;
    }

    /**
     * Getter for snapshot id.
     *
     * @return Current snapshot id
     */
    public long getSnapshotId() {
        return snapshotId;
    }

    public boolean snapshotFutureIsNull() {
        return snapshotFuture == null;
    }

    /**
     * Helper method populating the vertex to name lookup maps.
     *
     * @param hz              Hazelcast instance
     * @param stateMapName    Live state IMap name
     * @param snapshotMapName Snapshot state IMap name
     * @param snapshotIdName  Snapshot ID AtomicLong name
     */
    private void populateVertexLookupImaps(HazelcastInstance hz,
                                           String stateMapName,
                                           String snapshotMapName,
                                           String snapshotIdName) {
        Map<String, String> stateMapNames = hz.getMap(VERTEX_TO_LIVE_STATE_IMAP_NAME);
        stateMapNames.put(vertexName, stateMapName);
        Map<String, String> snapshotMapNames = hz.getMap(VERTEX_TO_SS_STATE_IMAP_NAME);
        snapshotMapNames.put(vertexName, snapshotMapName);
        Map<String, String> snapshotIdMapNames = hz.getMap(VERTEX_TO_SS_ID_IMAP_NAME);
        snapshotIdMapNames.put(vertexName, snapshotIdName);
    }


    /**
     * Helper for getting an IMap config.
     *
     * @param mapName Name of the IMap
     * @return The MapConfig with the correct name
     */
    public static MapConfig getMapConfig(String mapName) {
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
     *
     * @return The IMap name consisting of node name, vertex name, and memory address of processor
     */
    private String getLiveStateImapName() {
        return vertexName;
    }

    public static String getSnapshotMapName(String liveMapName) {
        return MessageFormat.format("snapshot-{0}", liveMapName);
    }

    public static String getSnapshotIdName(String liveMapName) {
        return MessageFormat.format("ssid-{0}", liveMapName);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        // Get HazelCastInstance
        Collection<HazelcastInstance> hzs = Hazelcast.getAllHazelcastInstances();
        HazelcastInstance hz = hzs.toArray(new HazelcastInstance[0])[0];

        // Get IMap, AtomicLong and CountDownLatch names
        this.vertexName = context.vertexName();
        this.jobName = context.jobConfig().getName();
        String liveMapName = getLiveStateImapName();
        String snapshotMapName = getSnapshotMapName(liveMapName);
        String snapshotIdName = getSnapshotIdName(liveMapName);
        String countDownLatchName = ExecutionContext.memberCountdownLatchHelper(
                context.jetInstance().getCluster().getLocalMember().getUuid(),
                jobName
        );
        getLogger().info("Getting member countdown latch name: " + countDownLatchName);

        // Add map config
        Config config = hz.getConfig();
        MapConfig stateMapConfig = getMapConfig(liveMapName);
        MapConfig snapshotMapConfig0 = getMapConfig(snapshotMapName);
        config.addMapConfig(stateMapConfig);
        config.addMapConfig(snapshotMapConfig0);

        // Add map names to Distributed List
        populateVertexLookupImaps(hz, liveMapName, snapshotMapName, snapshotIdName);

        keyToStateIMap = hz.getMap(liveMapName);
        snapshotIMap = hz.getMap(snapshotMapName);

        // Initialize distributed snapshot Id
        distributedSnapshotId = hz.getCPSubsystem().getAtomicLong(snapshotIdName);

        // Initialize snapshot countdown latch
        ssCountDownLatch = hz.getCPSubsystem().getCountDownLatch(countDownLatchName);
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
     *
     * @param key        Key of the item
     * @param state      State object
     * @param snapshotId Snapshot ID corresponding to the state entry
     * @param operation  Put or DELETE operation
     */
    void processSnapshotItem(@Nonnull K key,
                             S state,
                             long snapshotId,
                             @Nonnull SnapshotQueueItem.Operation operation) {
        checkSnapshotFutureClear();
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
     * Helper method checking if all keys in memory are also in the local key set.
     */
    private void checkKeys() {
        if (VERIFY_KEYSET) {
            Set<K> localKeySet = snapshotIMap.localKeySet().stream().map(SnapshotIMapKey::getPartitionKey)
                    .collect(Collectors.toSet());
            Set<K> memoryKeySet = keyToState.keySet();
            if (!localKeySet.containsAll(memoryKeySet)) {
                getLogger().severe("Memory keys are not in local key set!");
                Set<K> memoryRemain = new HashSet<>(memoryKeySet);
                memoryRemain.removeIf(localKeySet::contains);
                memoryRemain.forEach(k ->
                        getLogger().severe("Key in memory but not in local set: " + k.toString()));
            }
        }
    }

    /**
     * Helper checking snapshot future, prints the time it took to complete the future.
     * Also returns the result
     *
     * @param clearFuture If true it will set the future to null in case it is done, otherwise it will do nothing
     * @return true if snapshotFuture is not null and done, false otherwise
     */
    boolean checkSnapshotFutureReturn(boolean clearFuture) {
        // When snapshot future is done
        if (snapshotFuture != null && snapshotFuture.isDone()) {
            // In sync mode launch countdown as soon as snapshot Future is done.
            if (countDownFuture == null) {
                countDownAsync();
                checkKeys();
            }
            if (clearFuture) {
                snapshotFuture = null;
            }
            return true;
        }
        return false;
    }

    /**
     * Helper checking snapshot id future, prints the time it took to complete the future.
     * Also returns the result
     *
     * @return true if distributedSnapshotIdFuture is not null and done, false otherwise
     */
    private boolean checkSsIdFuture() {
        // When snapshot future is done
        if (distributedSnapshotIdFuture != null && distributedSnapshotIdFuture.isDone()) {
            return true;
        }
        return false;
    }

    /**
     * Checks the status of the countdown latch future. Sets countDownFuture to null in case it is done.
     *
     * @return true if the future is not null and done, false otherwise
     */
    private boolean checkCountDownFuture() {
        if (countDownFuture != null && countDownFuture.isDone()) {
            countDownFuture = null;
            return true;
        }
        return false;
    }

    /**
     * Helper method initializing count down future.
     */
    private void countDownAsync() {
        countDownFuture = CompletableFuture.runAsync(() -> {
            countDownStartTime = System.nanoTime();
            ssCountDownLatch.countDown();
            countDownEndTime = System.nanoTime();
            getLogger().info("Latch counted down");
        });
    }

    /**
     * Helper checking snapshot future.
     */
    private void checkSnapshotFutureClear() {
        checkSnapshotFutureReturn(true);
    }

    /**
     * Helper checking if previous snapshot was finished correctly.
     */
    private void checkPreviousSnapshotFinished() {
        // Entered saveToSnapshot() before previous non blocking snapshotFuture was done
        boolean traverserDone = snapshotTraverser == null;
        boolean snapshotFutureNotDone = snapshotFuture != null;
        boolean ssIdFutureNotDone = distributedSnapshotIdFuture != null && !distributedSnapshotIdFuture.isDone();
        checkCountDownFuture(); // Check and clear countDownFuture
        boolean cdlFutureNotDone = countDownFuture != null && !countDownFuture.isDone();
        if (!waitForFutures
                && traverserDone
                && (snapshotFutureNotDone || ssIdFutureNotDone || cdlFutureNotDone)) {
            // snapshot Future was not null yet
            checkSnapshotFutureClear();
            if (snapshotFuture == null) {
                // Now it is null so it was never cleared because process() was never called
                getLogger().info("snapshotFuture was never cleared");
            } else {
                // Invalid state, should never go here as the snapshotFuture was not yet finished before next call to
                // saveSnapshot().
                getLogger().severe("Invalid state, got to saveToSnapshot() before previous snapshotFuture could " +
                        "finish! Consider increasing snapshot interval.");
            }
            if (ssIdFutureNotDone) {
                getLogger().severe("ssId future not done from previous saveToSnapshot() invocation");
            }
            if (cdlFutureNotDone) {
                getLogger().severe("count down latch future not done from previous saveToSnapshot() invocation");
            }
        }
    }

    /**
     * Logs the execution times of tasks in snapshot phase.
     */
    private void logSnapshotExecutionTimes() {
//        getLogger().info("Execute on entries took: " + (distributedSsIdStartTime - snapshotIMapStartTime));
        if (waitForFutures) {
            getLogger().info("SS id time: " + (distributedSsIdEndTime - distributedSsIdStartTime));
            getLogger().info("Snapshot IMap time: " + (snapshotIMapEndTime - snapshotIMapStartTime));
            getLogger().info("Countdown latch time: " + (countDownEndTime - countDownStartTime));
        }
        getLogger().info("Snapshot traverser time: " + (snapshotTraverserEndTime - snapshotTraverserStartTime));
    }

    /**
     * Helper to check if snapshot is done.
     *
     * @return true if snapshot is done, false otherwise
     */
    private boolean isSnapshotDone() {
        if (emitFromTraverserToSnapshot(snapshotTraverser)) {
            boolean ssImapDone = checkSnapshotFutureReturn(false);
            boolean distSsIdDone = checkSsIdFuture();
            boolean cdlIsDone = checkCountDownFuture();

            // We don't wait for IMap ss future so mark as done
            if (!waitForFutures) {
                ssImapDone = true;
                distSsIdDone = true;
                cdlIsDone = true;
            }
            // Once both are done
            if (ssImapDone && distSsIdDone && cdlIsDone) {
                // Reset futures when waiting for them and they are done
                if (waitForFutures) {
                    snapshotFuture = null;
                    distributedSnapshotIdFuture = null;
                    countDownFuture = null;
                }
                // Log execution times
                logSnapshotExecutionTimes();
                // Traverse and both futures are done so return true
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (inComplete) {
            // If we are in completing phase, we can have a half-emitted item. Instead of finishing it and
            // writing a snapshot, we finish the final items and save no state.
            return complete();
        }

        // Check if previous snapshot was finished correctly
        checkPreviousSnapshotFinished();

        // Only execute the remove and snapshot once
        if (snapshotFuture == null) {
//            snapshotIMapStartTime = System.nanoTime();
//            snapshotIMap.executeOnEntries(new EvictSnapshotProcessor<>(snapshotId)); // Evict older snapshot entries
            snapshotId++; // Increment snapshot ID
            // Alter distributed snapshot Id to be the most recent snapshot Id
            distributedSsIdStartTime = System.nanoTime();
            distributedSnapshotIdFuture =
                    distributedSnapshotId.alterAsync(new AlterSafe(snapshotId)).toCompletableFuture()
                            .thenRun(() -> distributedSsIdEndTime = System.nanoTime());
            // Put current state in IMap as starting point
            snapshotIMapStartTime = System.nanoTime();
            snapshotFuture = snapshotIMap.setAllAsync(keyToState.entrySet().stream().collect(Collectors.toMap(
                    entry -> new SnapshotIMapKey<>(entry.getKey(), snapshotId),
                    entry -> entry.getValue().item()))).toCompletableFuture()
                    .thenRun(() -> snapshotIMapEndTime = System.nanoTime());
            if (snapshotDelayMillis != 0L) {
                snapshotFuture = snapshotFuture.thenRun(() -> {
                    try {
                        Thread.sleep(snapshotDelayMillis);
                        snapshotIMapEndTime = System.nanoTime();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }

        // Traditional snapshot traverser
        if (snapshotTraverser == null) {
            snapshotTraverserStartTime = System.nanoTime();
            snapshotTraverser = Traversers.<Entry<?, ?>>traverseIterable(keyToState.entrySet())
                    .append(entry(broadcastKey(SnapshotKeys.WATERMARK), currentWm))
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        snapshotTraverserEndTime = System.nanoTime();
                    });
        }

        // Return true if snapshot is done, false otherwise
        return isSnapshotDone();
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
