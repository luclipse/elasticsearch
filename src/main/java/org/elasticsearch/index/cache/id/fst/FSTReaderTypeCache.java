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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;

import java.io.IOException;

/**
 *
 */
public class FSTReaderTypeCache implements IdReaderTypeCache {

    private final String type;
    private final FST<Long> parentIdsValues;
    private final PackedInts.Reader docIdToParentIdOrd;
    private final PackedInts.Reader docIdToUidOrd;

    private long sizeInBytes = -1;

    public FSTReaderTypeCache(String type, FST<Long> parentIdsValues, PackedInts.Reader docIdToParentIdOrd, PackedInts.Reader docIdToUidOrd) {
        this.type = type;
        this.parentIdsValues = parentIdsValues;
        this.docIdToParentIdOrd = docIdToParentIdOrd;
        this.docIdToUidOrd = docIdToUidOrd;
    }

    public String type() {
        return this.type;
    }

    public BytesReference parentIdByDoc(int docId) {
        int ord = (int) docIdToParentIdOrd.get(docId);
        if (ord == 0) {
            return null;
        }
        FST.BytesReader in = parentIdsValues.getBytesReader();
        FST.Arc<Long> firstArc = new FST.Arc<Long>();
        FST.Arc<Long> scratchArc = new FST.Arc<Long>();
        IntsRef scratchInts = new IntsRef();
        in.setPosition(0);
        parentIdsValues.getFirstArc(firstArc);
        BytesRef uid = new BytesRef();
        try {
            IntsRef output = Util.getByOutput(parentIdsValues, ord, in, firstArc, scratchArc, scratchInts);
            uid.grow(output.length);
            uid.length = uid.offset = 0;
            Util.toBytesRef(output, uid);
            return new BytesArray(uid);
        } catch (IOException e) {
            throw new ElasticSearchException("", e);
        }
    }

    // only used by top_children query! Find a different solution for this query
    public int docById(BytesReference uid) {
        throw new UnsupportedOperationException();
    }

    public BytesReference idByDoc(int docId) {
        int ord = (int) docIdToUidOrd.get(docId);
        if (ord == 0) {
            return null;
        }
        FST.BytesReader in = parentIdsValues.getBytesReader();
        FST.Arc<Long> firstArc = new FST.Arc<Long>();
        FST.Arc<Long> scratchArc = new FST.Arc<Long>();
        IntsRef scratchInts = new IntsRef();
        in.setPosition(0);
        parentIdsValues.getFirstArc(firstArc);
        BytesRef uid = new BytesRef();
        try {
            IntsRef output = Util.getByOutput(parentIdsValues, ord, in, firstArc, scratchArc, scratchInts);
            uid.grow(output.length);
            uid.length = uid.offset = 0;
            Util.toBytesRef(output, uid);
            return new BytesArray(uid);
        } catch (IOException e) {
            throw new ElasticSearchException("", e);
        }
    }

    public long sizeInBytes() {
        if (sizeInBytes == -1) {
            sizeInBytes = computeSizeInBytes();
        }
        return sizeInBytes;
    }

    long computeSizeInBytes() {
        long sizeInBytes = parentIdsValues.sizeInBytes();
        sizeInBytes += docIdToParentIdOrd.ramBytesUsed();
        sizeInBytes += docIdToUidOrd.ramBytesUsed();
        return sizeInBytes;
    }

}
