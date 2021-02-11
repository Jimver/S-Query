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
import java.util.Set;

public class EvictingEntryProcessor<K, S> implements EntryProcessor<SnapshotIMapKey<K>, S, Boolean>, Serializable {
    private final Set<K> keysToEvict;

    public EvictingEntryProcessor(Set<K> keysToEvict) {
        this.keysToEvict = keysToEvict;
    }

    @Override
    public Boolean process(Map.Entry<SnapshotIMapKey<K>, S> entry) {
        if (keysToEvict.contains(entry.getKey().getPartitionKey())) {
            entry.setValue(null);
        }
        return true;
    }
}
