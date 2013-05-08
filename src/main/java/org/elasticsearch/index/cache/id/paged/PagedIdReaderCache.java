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

package org.elasticsearch.index.cache.id.paged;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.cache.id.IdReaderCache;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.shard.ShardId;

/**
 *
 */
public class PagedIdReaderCache implements IdReaderCache {

    private final ImmutableMap<String, PagedIdReaderTypeCache> types;

    @Nullable
    public final ShardId shardId;

    public PagedIdReaderCache(ImmutableMap<String, PagedIdReaderTypeCache> types, @Nullable ShardId shardId) {
        this.types = types;
        this.shardId = shardId;
    }

    @Override
    public IdReaderTypeCache type(String type) {
        return types.get(type);
    }

    @Override
    public BytesReference parentIdByDoc(String type, int docId) {
        PagedIdReaderTypeCache typeCache = types.get(type);
        if (typeCache != null) {
            return typeCache.parentIdByDoc(docId);
        }
        return null;
    }

    @Override
    public int docById(String type, BytesReference id) {
        PagedIdReaderTypeCache typeCache = types.get(type);
        if (typeCache != null) {
            return typeCache.docById(id);
        }
        return -1;
    }

    public long sizeInBytes() {
        long sizeInBytes = 0;
        for (PagedIdReaderTypeCache readerTypeCache : types.values()) {
            sizeInBytes += readerTypeCache.sizeInBytes();
        }
        return sizeInBytes;
    }

}
