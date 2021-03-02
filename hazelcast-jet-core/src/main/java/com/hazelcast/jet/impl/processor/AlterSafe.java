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

import com.hazelcast.core.IFunction;

public class AlterSafe implements IFunction<Long, Long> {
    private final long snapshotId;

    public AlterSafe(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public Long apply(Long input) {
        if (input < snapshotId) {
            input = snapshotId;
        }
        return input;
    }
}
