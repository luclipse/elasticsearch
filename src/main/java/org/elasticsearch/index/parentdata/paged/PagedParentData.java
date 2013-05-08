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

package org.elasticsearch.index.parentdata.paged;

import com.google.common.collect.Maps;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.index.parentdata.ParentData;
import org.elasticsearch.index.shard.ShardUtils;

import java.io.IOException;
import java.util.Map;

/**
 */
public class PagedParentData extends ParentData<PagedAtomicParentData, ParentData.ParentBuilder<PagedAtomicParentData>> {

    @Override
    protected ParentBuilder newParentBuilder(AtomicReader reader) {
        return new ParentBuilder(reader);
    }

    public static class ParentBuilder extends ParentData.ParentBuilder<PagedAtomicParentData> {

        private final AtomicReader reader;
        private final Map<String, TypeBuilder> typeBuilders = Maps.newHashMap();

        public ParentBuilder(AtomicReader reader) {
            this.reader = reader;
        }

        @Override
        public void onUid(String type, BytesRef uid, DocsEnum parentDocs, DocsEnum childDocs) throws IOException {
            TypeBuilder typeBuilder = typeBuilders.get(type);
            if (typeBuilder == null) {
                typeBuilder = new TypeBuilder(reader);
                typeBuilders.put(type, typeBuilder);
            }
            long offset = typeBuilder.parentIds.copyUsingLengthPrefix(uid);
            int hash = uid.hashCode();
            if (parentDocs != null) {
                for (int docId = parentDocs.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = parentDocs.nextDoc()) {
                    typeBuilder.docIdToUidOffsetWriter.set(docId, offset);
                    typeBuilder.hashes[docId] = hash;
                }
            }
            if (childDocs != null) {
                for (int docId = childDocs.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = childDocs.nextDoc()) {
                    typeBuilder.docIdToParentUidOffsetWriter.set(docId, offset);
                    typeBuilder.hashes[docId] = hash;
                }
            }
        }

        @Override
        public PagedAtomicParentData build(AtomicReader reader) {
            MapBuilder<String, PagedAtomicParentData.Type> types = MapBuilder.newMapBuilder();
            for (Map.Entry<String, TypeBuilder> entry : typeBuilders.entrySet()) {
                String type = entry.getKey();
                TypeBuilder typeBuilder = entry.getValue();

                types.put(type, new PagedAtomicParentData.Type(type,
                        typeBuilder.parentIds.freeze(true),
                        typeBuilder.parentIds.getPointer(),
                        typeBuilder.docIdToParentUidOffsetWriter.getMutable(),
                        typeBuilder.docIdToUidOffsetWriter.getMutable(),
                        typeBuilder.hashes));
            }

            return new PagedAtomicParentData(types.immutableMap(), ShardUtils.extractShardId(reader));
        }

        static class TypeBuilder {

            final PagedBytes parentIds = new PagedBytes(15);
            final GrowableWriter docIdToUidOffsetWriter;
            final GrowableWriter docIdToParentUidOffsetWriter;
            final int[] hashes;

            TypeBuilder(IndexReader reader) {
                parentIds.copyUsingLengthPrefix(new BytesRef()); // pointer 0 is for not set
                docIdToUidOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
                docIdToParentUidOffsetWriter = new GrowableWriter(1, reader.maxDoc(), PackedInts.FAST);
                hashes = new int[reader.maxDoc()];
            }
        }
    }
}
