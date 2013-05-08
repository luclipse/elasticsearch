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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

/**
 *
 */
public class PagedIdReaderTypeCache implements IdReaderTypeCache {

    private final String type;

    private final PagedBytes.Reader parentIds;
    private final long parentIdsSizeInBytes;
    private final PackedInts.Reader childDocIdToParentIdOffset;
    private final PackedInts.Reader parentDocIdToUidOffset;

    private long sizeInBytes = -1;

    public PagedIdReaderTypeCache(String type, PagedBytes.Reader parentIds, long parentIdsSizeInBytes, PackedInts.Reader childDocIdToParentIdOffset, PackedInts.Reader parentDocIdToUidOffset) {
        this.type = type;
        this.parentIds = parentIds;
        this.parentIdsSizeInBytes = parentIdsSizeInBytes;
        this.childDocIdToParentIdOffset = childDocIdToParentIdOffset;
        this.parentDocIdToUidOffset = parentDocIdToUidOffset;
    }

    public String type() {
        return this.type;
    }

    public BytesReference parentIdByDoc(int docId) {
        int parentIdOffset = (int) childDocIdToParentIdOffset.get(docId);
        BytesRef ref = new BytesRef();
        parentIds.fill(ref, parentIdOffset);
        return new BytesArray(ref);
    }

    public int docById(BytesReference uid) {
        throw new UnsupportedOperationException();
//        return idToDoc.get(uid);
    }

    public BytesReference idByDoc(int docId) {
        int parentIdOffset = (int) parentDocIdToUidOffset.get(docId);
        BytesRef ref = new BytesRef();
        parentIds.fill(ref, parentIdOffset);
        return new BytesArray(ref);
    }

    public void idByDoc(int docId, BytesRef ref) {
        int parentIdOffset = (int) parentDocIdToUidOffset.get(docId);
        parentIds.fill(ref, parentIdOffset);
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    long computeSizeInBytes() {
        long sizeInBytes = parentIdsSizeInBytes;
        sizeInBytes += childDocIdToParentIdOffset.ramBytesUsed();
        sizeInBytes += parentDocIdToUidOffset.ramBytesUsed();
        return sizeInBytes;
    }

}
