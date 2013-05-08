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

package org.elasticsearch.index.parentdata.concrete;

import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.parentdata.AtomicParentData;
import org.elasticsearch.index.parentdata.ParentValues;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 */
public class ConcreteAtomicParentData implements AtomicParentData {

    private final ShardId shardId;
    private final Map<String, Type> types;

    private long sizeInBytes = -1;

    public ConcreteAtomicParentData(Map<String, Type> types, ShardId shardId) {
        this.types = types;
        this.shardId = shardId;
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = 0;
            for (Map.Entry<String, Type> entry : types.entrySet()) {
                sizeInBytes += entry.getValue().sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public ParentValues getValues(String type) {
        Type foundType = types.get(type);
        if (foundType != null) {
            return new ConcreteParentValues(foundType);
        } else {
            return ParentValues.EMPTY;
        }
    }

    static class Type {

        final String type;
        final HashedBytesRef[] parentIds;
        final PackedInts.Reader docIdToParentIdOrd;
        final PackedInts.Reader docIdToUidOrd;

        Type(String type, HashedBytesRef[] parentIds, PackedInts.Reader docIdToParentIdOrd, PackedInts.Reader docIdToUidOrd) {
            this.type = type;
            this.parentIds = parentIds;
            this.docIdToParentIdOrd = docIdToParentIdOrd;
            this.docIdToUidOrd = docIdToUidOrd;
        }

        long sizeInBytes() {
            long size = parentIds.length * RamUsage.NUM_BYTES_OBJECT_REF + RamUsage.NUM_BYTES_ARRAY_HEADER;
            for (HashedBytesRef parentId : parentIds) {
                size += parentId.bytes.length + (3 * RamUsage.NUM_BYTES_INT) + RamUsage.NUM_BYTES_OBJECT_REF;
            }
            size += docIdToParentIdOrd.ramBytesUsed();
            size += docIdToUidOrd.ramBytesUsed();
            return size;
        }

    }
}
