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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.index.field.data1.strings.SingleValuedStringFieldData;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

import java.io.IOException;

/**
 *
 */
public class StoredFieldSingleValuedStringFieldData implements SingleValuedStringFieldData {

    protected final IndexReader indexReader;
    protected final String fieldName;
    protected final StringFieldMapper fieldMapper;
    protected final SingleFieldSelector fieldSelector;

    public StoredFieldSingleValuedStringFieldData(IndexReader indexReader, StringFieldMapper fieldMapper) {
        this.fieldMapper = fieldMapper;
        this.fieldName = fieldMapper.names().indexName();
        this.indexReader = indexReader;
        this.fieldSelector = new SingleFieldSelector(fieldName);
    }

    public BytesRef value(int docId) {
        try {
            Document doc = indexReader.document(docId, fieldSelector);
            Fieldable field = doc.getFieldable(fieldName);
            return field != null ? new BytesRef(field.stringValue()) : null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String fieldName() {
        return fieldName;
    }

    public boolean hasValue(int docId) {
        try {
            Document doc = indexReader.document(docId, fieldSelector);
            return doc.getFieldable(fieldName) != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long computeSizeInBytes() {
        return 0L;
    }

    public static StoredFieldSingleValuedStringFieldData load(IndexReader reader, StringFieldMapper fieldMapper) throws IOException {
        return new StoredFieldSingleValuedStringFieldData(reader, fieldMapper);
    }

}
