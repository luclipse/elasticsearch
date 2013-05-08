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

package org.elasticsearch.index.cache.id.fst;

import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

import java.io.IOException;

/**
 *
 */
public class FSTReaderTypeCache implements IdReaderTypeCache {

    private final String type;

    private final FST<Long> idToDoc;

    private final HashedBytesArray[] docIdToId;

    private final FST<BytesReference> parentIdsValues;

    private final PackedInts.Reader parentIdsOrdinals;

    private long sizeInBytes = -1;

    public FSTReaderTypeCache(String type, FST<Long> idToDoc, HashedBytesArray[] docIdToId,
                              FST<BytesReference> parentIdsValues, PackedInts.Reader parentIdsOrdinals) {
        this.type = type;
        this.idToDoc = idToDoc;
        this.docIdToId = docIdToId;
        this.parentIdsValues = parentIdsValues;
        this.parentIdsOrdinals = parentIdsOrdinals;
    }

    public String type() {
        return this.type;
    }

    public BytesReference parentIdByDoc(int docId) {
        IntsRef intsRef = new IntsRef(1);
        intsRef.length = 1;
        try {
            intsRef.ints[0] = (int) parentIdsOrdinals.get(docId);
            return Util.get(parentIdsValues, intsRef);
        } catch (IOException e) {
            throw new ElasticSearchException("", e);
        }
    }

    public int docById(BytesReference uid) {
        try {
            return Util.get(idToDoc, uid.toBytesRef()).intValue();
        } catch (IOException e) {
            throw new ElasticSearchException("", e);
        }
    }

    public BytesReference idByDoc(int docId) {
        return docIdToId[docId];
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    /**
     * Returns an already stored instance if exists, if not, returns null;
     */
    public HashedBytesArray canReuse(HashedBytesArray id) {
//        return idToDoc.key(id);
        return null;
    }

    long computeSizeInBytes() {
        long sizeInBytes = 0;
        // Ignore type field
        //  sizeInBytes += ((type.length() * RamUsage.NUM_BYTES_CHAR) + (3 * RamUsage.NUM_BYTES_INT)) + RamUsage.NUM_BYTES_OBJECT_HEADER;
        sizeInBytes += RamUsage.NUM_BYTES_ARRAY_HEADER + idToDoc.sizeInBytes();

        // The docIdToId array contains references to idToDoc for this segment or other segments, so we can use OBJECT_REF
        sizeInBytes += RamUsage.NUM_BYTES_ARRAY_HEADER + (RamUsage.NUM_BYTES_OBJECT_REF * docIdToId.length);
        sizeInBytes += parentIdsValues.sizeInBytes();
        sizeInBytes += parentIdsOrdinals.ramBytesUsed();

        return sizeInBytes;
    }

}
