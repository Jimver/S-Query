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

import com.hazelcast.map.EntryProcessor;

import java.io.Serializable;
import java.util.Map;

/**
 * Snapshot eviction processor.
 * This entry processor evicts all keys from the IMap that are older that the
 * snapshotId given in the constructor by AMOUNT_TO_KEEP amount.
 *
 * For example if the snapshotId is 10, and AMOUNT_TO_KEEP is 2:
 * All entries with snapshot id 8 or less will be deleted, so 9 and 10 will stay.
 *
 * @param <K> Key type of the original state IMap
 * @param <S> State type
 */
public class EvictSnapshotProcessor<K, S>
        implements EntryProcessor<SnapshotIMapKey<K>, S, Boolean>, Serializable {
    // Amount of snapshot IDs to keep in the snapshot IMap.
    private static final long AMOUNT_TO_KEEP = 2;

    private final long snapshotId;

    /**
     * Constructor
     * @param snapshotId Current snapshot ID
     */
    public EvictSnapshotProcessor(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public Boolean process(Map.Entry<SnapshotIMapKey<K>, S> entry) {
        // Remove all snapshot entries from AMOUNT_TO_KEEP or more snapshots ago
        if (entry.getKey().getSnapshotId() <= (snapshotId - AMOUNT_TO_KEEP + 1)) {
            entry.setValue(null);
        }
        return true;
    }
}
