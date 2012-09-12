/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.field.data1.strings.stored;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.index.field.data1.strings.SingleValuedStringOrdinalFieldData;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.io.IOException;

/**
 *
 */
public class StoredFieldSingleValuedStringOrdinalFieldData extends StoredFieldSingleValuedStringFieldData implements SingleValuedStringOrdinalFieldData {

    private final int[] docToOrd;

    public StoredFieldSingleValuedStringOrdinalFieldData(IndexReader indexReader, StringFieldMapper fieldMapper, int[] docToOrd) {
        super(indexReader, fieldMapper);
        this.docToOrd = docToOrd;
    }

    public boolean hasValue(int docId) {
        return docToOrd[docId] != 0;
    }

    public long computeSizeInBytes() {
        return RamUsage.NUM_BYTES_ARRAY_HEADER + (docToOrd.length * RamUsage.NUM_BYTES_INT);
    }

    public int ordinal(int docId) {
        return docToOrd[docId];
    }

    public BytesRef lookup(int ordinal) {
        return null;
    }

    public int lookup(BytesRef bytesRef) {
        return 0;
    }

    public static StoredFieldSingleValuedStringOrdinalFieldData load(IndexReader reader, StringFieldMapper fieldMapper) throws IOException {
        BytesRefHash values = new BytesRefHash();
        int[] docToOrd = new int[reader.maxDoc()];
        for (int docId = 0; docId < reader.maxDoc(); docId++) {
            if (!reader.isDeleted(docId)) {
                Fieldable field = reader.document(docId).getFieldable(fieldMapper.names().indexName());
                if (field != null) {
                    int ord = values.add(new BytesRef(field.stringValue()));
                    if (ord < 0) {
                        ord = -ord - 1;
                    }
                    docToOrd[docId] = ord;
                }
            }
        }
        return new StoredFieldSingleValuedStringOrdinalFieldData(reader, fieldMapper, docToOrd);
    }

}
