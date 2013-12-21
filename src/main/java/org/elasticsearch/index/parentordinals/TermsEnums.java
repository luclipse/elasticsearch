/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.index.parentordinals;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

/**
 */
public final class TermsEnums {

    private TermsEnums() {
    }

    public static MultiTermsEnum getCompoundTermsEnum(IndexReader indexReader, NavigableSet<BytesRef> parentTypes, String... fields) throws IOException {
        final MultiTermsEnum[] enums = new MultiTermsEnum[fields.length];
        for (int i = 0; i < fields.length; i++) {
            List<TermsEnum> fieldEnums = new ArrayList<TermsEnum>();
            for (AtomicReaderContext readerContext : indexReader.leaves()) {
                Terms terms = readerContext.reader().terms(fields[i]);
                if (terms != null) {
                    fieldEnums.add(new ParentUidTermsEnum(terms.iterator(null), parentTypes));
                }
            }

            ReaderSlice[] slices = new ReaderSlice[fieldEnums.size()];
            MultiTermsEnum.TermsEnumIndex[] indexes = new MultiTermsEnum.TermsEnumIndex[fieldEnums.size()];
            for (int j = 0; j < slices.length; j++) {
                slices[j] = new ReaderSlice(0, 0, j);
                indexes[j] = new MultiTermsEnum.TermsEnumIndex(fieldEnums.get(j), j);
            }
            enums[i] = new MultiTermsEnum(slices);
            enums[i].reset(indexes);
        }

        // final wrap...
        ReaderSlice[] slices = new ReaderSlice[enums.length];
        MultiTermsEnum.TermsEnumIndex[] indexes = new MultiTermsEnum.TermsEnumIndex[enums.length];
        for (int j = 0; j < slices.length; j++) {
            slices[j] = new ReaderSlice(0, 0, j);
            indexes[j] = new MultiTermsEnum.TermsEnumIndex(enums[j], j);
        }

        MultiTermsEnum termsEnum = new MultiTermsEnum(slices);
        termsEnum.reset(indexes);
        return termsEnum;
    }

    public static MultiTermsEnum getCompoundTermsEnum(AtomicReader atomicReader, NavigableSet<BytesRef> parentTypes, String... fields) throws IOException {
        List<TermsEnum> fieldEnums = new ArrayList<TermsEnum>();
        for (int i = 0; i < fields.length; i++) {
            Terms terms = atomicReader.terms(fields[i]);
            if (terms != null) {
                fieldEnums.add(new ParentUidTermsEnum(terms.iterator(null), parentTypes));
            }
        }

        ReaderSlice[] slices = new ReaderSlice[fieldEnums.size()];
        MultiTermsEnum.TermsEnumIndex[] indexes = new MultiTermsEnum.TermsEnumIndex[fieldEnums.size()];
        for (int j = 0; j < slices.length; j++) {
            slices[j] = new ReaderSlice(0, 0, j);
            indexes[j] = new MultiTermsEnum.TermsEnumIndex(fieldEnums.get(j), j);
        }

        MultiTermsEnum termsEnum = new MultiTermsEnum(slices);
        termsEnum.reset(indexes);
        return termsEnum;
    }

    public static final class ParentUidTermsEnum extends FilteredTermsEnum {

        private final NavigableSet<BytesRef> parentTypes;
        private BytesRef seekTerm;
        private String type;
        private BytesRef id;

        ParentUidTermsEnum(TermsEnum tenum, NavigableSet<BytesRef> parentTypes) {
            super(tenum, true);
            this.parentTypes = parentTypes;
            this.seekTerm = parentTypes.isEmpty() ? null : parentTypes.first();
        }

        @Override
        protected BytesRef nextSeekTerm(BytesRef currentTerm) throws IOException {
            BytesRef temp = seekTerm;
            seekTerm = null;
            return temp;
        }

        @Override
        protected AcceptStatus accept(BytesRef term) throws IOException {
            BytesRef[] typeAndId = Uid.splitUidIntoTypeAndId(term);
            if (parentTypes.contains(typeAndId[0])) {
                type = typeAndId[0].utf8ToString();
                id = typeAndId[1];
                return AcceptStatus.YES;
            } else {
                BytesRef nextType = parentTypes.ceiling(typeAndId[0]);
                if (nextType == null) {
                    return AcceptStatus.END;
                }
                seekTerm = nextType;
                return AcceptStatus.NO_AND_SEEK;
            }
        }

        public String type() {
            return type;
        }

        public BytesRef id() {
            return id;
        }

    }

}
