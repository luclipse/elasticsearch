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

package org.elasticsearch.index.cache.id.concrete;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

/**
 *
 */
public class ConcreteIdReaderTypeCache implements IdReaderTypeCache {

    private final String type;
    private final BytesRef[] parentIds;
    private final PackedInts.Reader docIdToParentIdOrd;
    private final PackedInts.Reader docIdToUidOrd;

    private long sizeInBytes = -1;

    public ConcreteIdReaderTypeCache(String type, BytesRef[] parentIds, PackedInts.Reader docIdToParentIdOrd, PackedInts.Reader docIdToUidOrd) {
        this.type = type;
        this.parentIds = parentIds;
        this.docIdToParentIdOrd = docIdToParentIdOrd;
        this.docIdToUidOrd = docIdToUidOrd;
    }

    public String type() {
        return this.type;
    }

    public BytesReference parentIdByDoc(int docId) {
        int ord = (int) docIdToParentIdOrd.get(docId);
        BytesRef uid = parentIds[ord];
        if (uid == null) {
            return null;
        } else {
            return new BytesArray(uid);
        }
    }

    // only used by top_children query! Find a different solution for this query
    public int docById(BytesReference uid) {
        throw new UnsupportedOperationException();
//        return idToDoc.get(uid);
    }

    public BytesReference idByDoc(int docId) {
        int ord = (int) docIdToUidOrd.get(docId);
        BytesRef uid = parentIds[ord];
        if (uid == null) {
            return null;
        } else {
            return new BytesArray(uid);
        }
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    long computeSizeInBytes() {
        long sizeInBytes = RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_OBJECT_REF * parentIds.length);
        for (BytesRef bytesArray : parentIds) {
            if (bytesArray != null) {
                sizeInBytes += RamUsage.NUM_BYTES_OBJECT_HEADER + (bytesArray.length + RamUsage.NUM_BYTES_INT);
            }
        }
        sizeInBytes += docIdToParentIdOrd.ramBytesUsed();
        sizeInBytes += docIdToUidOrd.ramBytesUsed();
        return sizeInBytes;
    }

}
