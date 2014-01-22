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

import com.carrotsearch.hppc.ObjectFloatOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link IdReaderTypeCache}.
 */
public class ParentQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentQuery(ParentChildIndexFieldData parentChildIndexFieldData, Query parentQuery, String parentType, Filter childrenFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.originalParentQuery = parentQuery;
        this.parentType = parentType;
        this.childrenFilter = childrenFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ParentQuery that = (ParentQuery) obj;
        if (!originalParentQuery.equals(that.originalParentQuery)) {
            return false;
        }
        if (!parentType.equals(that.parentType)) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalParentQuery.hashCode();
        result = 31 * result + parentType.hashCode();
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ParentQuery[").append(parentType).append("](")
                .append(originalParentQuery.toString(field)).append(')')
                .append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenParentQuery == null) {
            rewriteIndexReader = reader;
            rewrittenParentQuery = originalParentQuery.rewrite(reader);
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
        Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore = searchContext.cacheRecycler().objectFloatMap(-1);
        ParentUidCollector collector = new ParentUidCollector(uidToScore.v(), parentChildIndexFieldData, parentType);

        final Query parentQuery;
        if (rewrittenParentQuery == null) {
            parentQuery = rewrittenParentQuery = searcher.rewrite(originalParentQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(parentQuery, collector);

        if (uidToScore.v().isEmpty()) {
            uidToScore.release();
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        ChildWeight childWeight = new ChildWeight(parentQuery.createWeight(searcher), childrenFilter, uidToScore);
        searchContext.addReleasable(childWeight);
        return childWeight;
    }

    private static class ParentUidCollector extends NoopCollector {

        private final ObjectFloatOpenHashMap<HashedBytesRef> uidToScore;
        private final ParentChildIndexFieldData indexFieldData;
        private final String parentType;
        private final HashedBytesRef spare = new HashedBytesRef();

        private Scorer scorer;
        private BytesValues values;

        ParentUidCollector(ObjectFloatOpenHashMap<HashedBytesRef> uidToScore, ParentChildIndexFieldData indexFieldData, String parentType) {
            this.uidToScore = uidToScore;
            this.indexFieldData = indexFieldData;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (values != null) {
                int numValues = values.setDocument(doc);
                assert numValues == 1;
                values.nextValue();
                spare.bytes = values.copyShared();
                spare.hash = values.currentValueHash();
                if (!uidToScore.containsKey(spare)) {
                    HashedBytesRef hashedParentId = new HashedBytesRef(spare.bytes, spare.hash);
                    uidToScore.put(hashedParentId, scorer.score());
                }
            }
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getBytesValues(parentType);
        }
    }

    private class ChildWeight extends Weight implements Releasable {

        private final Weight parentWeight;
        private final Filter childrenFilter;
        private final Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore;

        private ChildWeight(Weight parentWeight, Filter childrenFilter, Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore) {
            this.parentWeight = parentWeight;
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.uidToScore = uidToScore;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ParentQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = parentWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet childrenDocSet = childrenFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(childrenDocSet)) {
                return null;
            }
            BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            return new ChildScorer(this, uidToScore.v(), childrenDocSet.iterator(), bytesValues);
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(uidToScore);
            return true;
        }
    }

    private static class ChildScorer extends Scorer {

        private final ObjectFloatOpenHashMap<HashedBytesRef> uidToScore;
        private final DocIdSetIterator childrenIterator;
        private final BytesValues bytesValues;
        private final HashedBytesRef spare = new HashedBytesRef();

        private int currentChildDoc = -1;
        private float currentScore;

        ChildScorer(Weight weight, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore, DocIdSetIterator childrenIterator, BytesValues bytesValues) {
            super(weight);
            this.uidToScore = uidToScore;
            this.childrenIterator = childrenIterator;
            this.bytesValues = bytesValues;
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        @Override
        public int freq() throws IOException {
            // We don't have the original child query hit info here...
            // But the freq of the children could be collector and returned here, but makes this Scorer more expensive.
            return 1;
        }

        @Override
        public int docID() {
            return currentChildDoc;
        }

        @Override
        public int nextDoc() throws IOException {
            while (true) {
                currentChildDoc = childrenIterator.nextDoc();
                if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentChildDoc;
                }

                int numValues = bytesValues.setDocument(currentChildDoc);
                assert numValues == 1;
                spare.bytes = bytesValues.nextValue();
                spare.hash = bytesValues.currentValueHash();
                if (uidToScore.containsKey(spare)) {
                    // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                    currentScore = uidToScore.lget();
                    return currentChildDoc;
                }
            }
        }

        @Override
        public int advance(int target) throws IOException {
            currentChildDoc = childrenIterator.advance(target);
            if (currentChildDoc == DocIdSetIterator.NO_MORE_DOCS) {
                return currentChildDoc;
            }

            int numValues = bytesValues.setDocument(currentChildDoc);
            assert numValues == 1;
            spare.bytes = bytesValues.nextValue();
            spare.hash = bytesValues.currentValueHash();
            if (uidToScore.containsKey(spare)) {
                // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                currentScore = uidToScore.lget();
                return currentChildDoc;
            } else {
                return nextDoc();
            }
        }

        @Override
        public long cost() {
            return childrenIterator.cost();
        }
    }
}
