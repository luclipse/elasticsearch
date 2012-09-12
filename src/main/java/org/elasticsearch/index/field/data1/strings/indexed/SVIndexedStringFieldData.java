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

package org.elasticsearch.index.field.data1.strings.indexed;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.RamUsage;
import org.elasticsearch.index.field.data1.strings.SingleValuedStringFieldData;

/**
 */
public class SVIndexedStringFieldData implements SingleValuedStringFieldData {

    private final String fieldName;
    private final BytesRef[] values;

    public SVIndexedStringFieldData(String fieldName, BytesRef[] values) {
        this.fieldName = fieldName;
        this.values = values;
    }

    @Override
    public BytesRef value(int docId) {
        return values[docId];
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    @Override
    public boolean hasValue(int docId) {
        return values[docId] != null;
    }

    @Override
    public long computeSizeInBytes() {
        long size = RamUsage.NUM_BYTES_ARRAY_HEADER;
        for (BytesRef value : values) {
            if (value != null) {
                size += RamUsage.NUM_BYTES_OBJECT_HEADER + (value.bytes.length + (2 * RamUsage.NUM_BYTES_INT));
            }
        }
        return size;
    }
}
