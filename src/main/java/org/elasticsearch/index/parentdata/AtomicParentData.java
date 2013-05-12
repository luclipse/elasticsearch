/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.parentdata;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.shard.ShardId;

/**
 * Per segment holder for {@link ParentValues} instances for all parent types.
 */
public interface AtomicParentData {

    @Nullable
    ShardId shardId();

    /**
     * @return The size in bytes the parent data takes into memory for all parent types.
     */
    long sizeInBytes();

    /**
     * @param type The parent type to load the {@link ParentValues} for
     * @return {@link ParentValues} for the specified type or {@link ParentValues#EMPTY} if no parent values are available.
     */
    ParentValues getValues(String type);
}
