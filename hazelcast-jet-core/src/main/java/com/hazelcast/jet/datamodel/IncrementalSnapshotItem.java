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

package com.hazelcast.jet.datamodel;

import java.util.Objects;

/**
 * Incremental snapshot item. Subclass of TimestampedItem.
 * Adds a backedUp field denoting if this item was backed up already.
 * @param <T>
 */
public class IncrementalSnapshotItem<T> extends TimestampedItem<T> {
    private boolean backedUp; // False by default (not backed up)

    /**
     * Creates a new incremental snapshot item.
     *
     * @param timestamp Timestamp
     * @param item Item
     */
    public IncrementalSnapshotItem(long timestamp, T item) {
        super(timestamp, item);
    }

    /**
     * Set the internal backed up field to true, use this when you submitted this item to the snapshot backup queue
     */
    public void setBackedUp() {
        backedUp = true;
    }

    /**
     * Set the internal backed up field to false, use this when the item changes value
     */
    public void setNotBackedUp() {
        backedUp = true;
    }

    /**
     * Gets the backup status of this item.
     * @return True if this item is backed up, false otherwise.
     */
    public boolean getBackedUp() {
        return this.backedUp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IncrementalSnapshotItem<?> that = (IncrementalSnapshotItem<?>) o;
        return backedUp == that.backedUp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), backedUp);
    }

    @Override
    public String toString() {
        return "IncrementalSnapshotItem{" +
                "TimestampedItem=" + super.toString() +
                ", backedUp=" + backedUp +
                '}';
    }
}
