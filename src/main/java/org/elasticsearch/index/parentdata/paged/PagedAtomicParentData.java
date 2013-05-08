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

package org.elasticsearch.index.parentdata.paged;

import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.parentdata.AtomicParentData;
import org.elasticsearch.index.parentdata.ParentValues;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 */
public class PagedAtomicParentData implements AtomicParentData {

    private final ShardId shardId;
    private final Map<String, Type> types;

    private long sizeInBytes = -1;

    public PagedAtomicParentData(Map<String, Type> types, ShardId shardId) {
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
            return new PagedParentValues(foundType);
        } else {
            return ParentValues.EMPTY;
        }
    }

    static class Type {

        final String type;
        final PagedBytes.Reader parentIds;
        final long parentIdsSizeInBytes;
        final PackedInts.Reader docIdToParentIdOffset;
        final PackedInts.Reader docIdToUidOffset;
        final int[] hashes;

        Type(String type, PagedBytes.Reader parentIds, long parentIdsSizeInBytes, PackedInts.Reader docIdToParentIdOffset, PackedInts.Reader docIdToUidOffset, int[] hashes) {
            this.type = type;
            this.parentIds = parentIds;
            this.parentIdsSizeInBytes = parentIdsSizeInBytes;
            this.docIdToParentIdOffset = docIdToParentIdOffset;
            this.docIdToUidOffset = docIdToUidOffset;
            this.hashes = hashes;
        }

        long sizeInBytes() {
            long size = parentIdsSizeInBytes;
            size += docIdToParentIdOffset.ramBytesUsed();
            size += docIdToUidOffset.ramBytesUsed();
            size += hashes.length * RamUsage.NUM_BYTES_INT + RamUsage.NUM_BYTES_ARRAY_HEADER;
            return size;
        }

    }
}
