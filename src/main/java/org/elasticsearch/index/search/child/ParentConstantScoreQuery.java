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
package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public class ParentConstantScoreQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentConstantScoreQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenParentQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        Recycler.V<ObjectOpenHashSet<HashedBytesRef>> parentIdsHolder = searchContext.cacheRecycler().hashSet(-1);
        ObjectOpenHashSet<HashedBytesRef> parentIds = parentIdsHolder.v();
        ParentUidsCollector collector = new ParentUidsCollector(parentType, parentChildIndexFieldData, parentIds);

        final Query parentQuery;
        if (rewrittenParentQuery != null) {
            parentQuery = rewrittenParentQuery;
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery = originalParentQuery.rewrite(searcher.getIndexReader());
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(parentQuery, collector);

        if (parentIds.size() == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        ChildrenWeight childrenWeight = new ChildrenWeight(childrenFilter, parentIdsHolder);
        searchContext.addReleasable(childrenWeight);
        return childrenWeight;
    }

    private final class ChildrenWeight extends Weight implements Releasable {

        private final Filter childrenFilter;
        private final Recycler.V<ObjectOpenHashSet<HashedBytesRef>> parentIdsHolder;

        private float queryNorm;
        private float queryWeight;

        private ChildrenWeight(Filter childrenFilter, Recycler.V<ObjectOpenHashSet<HashedBytesRef>> parentIdsHolder) {
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.parentIdsHolder = parentIdsHolder;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentConstantScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            queryWeight = getBoost();
            return queryWeight * queryWeight;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocIdSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocIdSet)) {
                return null;
            }

            BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues != null) {
                DocIdSetIterator innerIterator = childrenDocIdSet.iterator();
                if (innerIterator != null) {
                    ChildrenDocIdIterator childrenDocIdIterator = new ChildrenDocIdIterator(innerIterator, parentIdsHolder.v(), bytesValues);
                    return ConstantScorer.create(childrenDocIdIterator, this, queryWeight);
                }
            }
            return null;
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(parentIdsHolder);
            return true;
        }

        private final class ChildrenDocIdIterator extends FilteredDocIdSetIterator {

            private final ObjectOpenHashSet<HashedBytesRef> parentIds;
            private final HashedBytesRef spare = new HashedBytesRef();
            private final BytesValues bytesValues;

            ChildrenDocIdIterator(DocIdSetIterator innerIterator, ObjectOpenHashSet<HashedBytesRef> parentIds, BytesValues bytesValues) {
                super(innerIterator);
                this.parentIds = parentIds;
                this.bytesValues = bytesValues;
            }

            @Override
            protected boolean match(int doc) {
                int numValues = bytesValues.setDocument(doc);
                assert numValues == 1;
                spare.bytes = bytesValues.nextValue();
                spare.hash = bytesValues.currentValueHash();
                return parentIds.contains(spare);
            }

        }
    }

    private final static class ParentUidsCollector extends NoopCollector {

        private final ObjectOpenHashSet<HashedBytesRef> parentIds;
        private final ParentChildIndexFieldData indexFieldData;
        private final String parentType;

        private BytesValues values;

        ParentUidsCollector(String parentType, ParentChildIndexFieldData indexFieldData, ObjectOpenHashSet<HashedBytesRef> parentIds) {
            this.parentIds = parentIds;
            this.indexFieldData = indexFieldData;
            this.parentType = parentType;
        }

        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (values != null) {
                int numValues = values.setDocument(doc);
                assert numValues == 1;
                values.nextValue();
                parentIds.add(new HashedBytesRef(values.copyShared(), values.currentValueHash()));
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            values = indexFieldData.load(readerContext).getBytesValues(parentType);
        }
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentConstantScoreQuery that = (ParentConstantScoreQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (this.getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("parent_filter[").append(parentType).append("](").append(originalParentQuery).append(')');
        return sb.toString();
    }

}

