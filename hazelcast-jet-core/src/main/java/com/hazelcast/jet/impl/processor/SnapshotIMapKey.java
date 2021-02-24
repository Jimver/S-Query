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

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;

import java.io.IOException;
import java.util.Objects;

public final class SnapshotIMapKey<K> implements IdentifiedDataSerializable, PartitionAware<K> {
    private K key;


    private long snapshotId;

    // For deserialization
    public SnapshotIMapKey() {

    }

    public SnapshotIMapKey(K key, long snapshotId) {
        this.key = key;
        this.snapshotId = snapshotId;
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public K getPartitionKey() {
        return key;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SNAPSHOTIMAP_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeLong(snapshotId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        snapshotId = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotIMapKey<?> that = (SnapshotIMapKey<?>) o;
        return snapshotId == that.snapshotId &&
                Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, snapshotId);
    }
}
