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

package org.elasticsearch.index.field.data1.strings;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldSelector;

import java.io.IOException;

/**
 *
 */
public class StoredFieldStringOrdinalFieldData implements StringOrdinalFieldData {

    private final IndexReader indexReader;
    private final String fieldName;

    public StoredFieldStringOrdinalFieldData(String fieldName, IndexReader indexReader) {
        this.fieldName = fieldName;
        this.indexReader = indexReader;
    }

    public String value(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable field = doc.getFieldable(SourceFieldMapper.NAME);
            return field != null ? field.stringValue() : null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String[] values(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            Fieldable[] sourceField = doc.getFieldables(SourceFieldMapper.NAME);
            String[] result;
            if (sourceField.length == 0) {
                result = Strings.EMPTY_ARRAY;
            } else {
                result = new String[sourceField.length];
                for (int i = 0; i < sourceField.length; i++) {
                    result[i] = sourceField[i].stringValue();
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int ord(int doc) {
        return 0;
    }

    @Override
    public int[] ords(int doc) {
        return new int[0];
    }

    public String fieldName() {
        return fieldName;
    }

    public boolean hasValue(int docId) {
        try {
            Document doc = indexReader.document(docId, SourceFieldSelector.INSTANCE);
            return doc.getFieldable(SourceFieldMapper.NAME) != null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long computeSizeInBytes() {
        return 0L;
    }

    public StoredFieldStringOrdinalFieldData load(IndexReader reader, String fieldName) {
        return new StoredFieldStringOrdinalFieldData(fieldName, reader);
    }

}
