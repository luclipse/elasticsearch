/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTermsEnum;

import java.io.IOException;

/**
 */
public final class TermsEnums {

    private TermsEnums() {
    }

    public static MultiTermsEnum getCompoundTermsEnum(IndexReader indexReader, String field) throws IOException {
        /*List<TermsEnum> fieldEnums = new ArrayList<TermsEnum>();
        for (AtomicReaderContext readerContext : indexReader.leaves()) {
            Terms terms = readerContext.reader().terms(field);
            if (terms != null) {
                fieldEnums.add(terms.iterator(null));
            }
        }

        ReaderSlice[] slices = new ReaderSlice[fieldEnums.size()];
        MultiTermsEnum.TermsEnumIndex[] indexes = new MultiTermsEnum.TermsEnumIndex[fieldEnums.size()];
        for (int j = 0; j < slices.length; j++) {
            slices[j] = new ReaderSlice(0, 0, j);
            indexes[j] = new MultiTermsEnum.TermsEnumIndex(fieldEnums.get(j), j);
        }
        MultiTermsEnum enums = new MultiTermsEnum(slices);
        enums.reset(indexes);
        return enums;*/
        return null;
    }

}
