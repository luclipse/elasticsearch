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

package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.IntFloatOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.RecyclerUtils;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query implementation that executes the wrapped parent query and
 * connects the matching parent docs to the related child documents
 * using the {@link org.elasticsearch.index.parentordinals.ParentOrdinalsService}.
 */
public class ParentQuery extends Query {

    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentQuery(Query parentQuery, String parentType, Filter childrenFilter) {
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
        return "ParentQuery[" + parentType + "](" + originalParentQuery.toString(field) + ')' + ToStringUtils.boost(getBoost());
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
        Recycler.V<IntFloatOpenHashMap> uidToScore = searchContext.cacheRecycler().intFloatMap(-1);
        ParentOrdsCollector collector = new ParentOrdsCollector(uidToScore.v(), searchContext, parentType);

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

        ChildWeight childWeight = new ChildWeight(parentQuery.createWeight(searcher), childrenFilter, searchContext, uidToScore);
        searchContext.addReleasable(childWeight);
        return childWeight;
    }

    private static class ParentOrdsCollector extends NoopCollector {

        private final IntFloatOpenHashMap ordToScore;
        private final SearchContext searchContext;
        private final String parentType;

        private Scorer scorer;
        private ParentOrdinals.Segment parentOrdinals;

        ParentOrdsCollector(IntFloatOpenHashMap ordToScore, SearchContext searchContext, String parentType) {
            this.ordToScore = ordToScore;
            this.searchContext = searchContext;
            this.parentType = parentType;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (parentOrdinals.isEmpty()) {
                return;
            }

            int parentOrd = parentOrdinals.ordinal(doc);
            ordToScore.put(parentOrd, scorer.score());
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            parentOrdinals = searchContext.parentOrdinalService().current().ordinals(parentType, context);
        }
    }

    private class ChildWeight extends Weight implements Releasable {

        private final Weight parentWeight;
        private final Filter childrenFilter;
        private final SearchContext searchContext;
        private final Recycler.V<IntFloatOpenHashMap> ordToScore;

        private ChildWeight(Weight parentWeight, Filter childrenFilter, SearchContext searchContext, Recycler.V<IntFloatOpenHashMap> ordToScore) {
            this.parentWeight = parentWeight;
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.searchContext = searchContext;
            this.ordToScore = ordToScore;
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
            ParentOrdinals.Segment parentOrdinals = searchContext.parentOrdinalService().current().ordinals(parentType, context);
            if (parentOrdinals.isEmpty()) {
                return null;
            }

            return new ChildScorer(this, ordToScore.v(), childrenDocSet.iterator(), parentOrdinals);
        }

        @Override
        public boolean release() throws ElasticSearchException {
            RecyclerUtils.release(ordToScore);
            return true;
        }
    }

    private static class ChildScorer extends Scorer {

        private final IntFloatOpenHashMap ordToScore;
        private final DocIdSetIterator childrenIterator;
        private final ParentOrdinals.Segment parentOrdinals;

        private int currentChildDoc = -1;
        private float currentScore;

        ChildScorer(Weight weight, IntFloatOpenHashMap ordToScore, DocIdSetIterator childrenIterator, ParentOrdinals.Segment parentOrdinals) {
            super(weight);
            this.ordToScore = ordToScore;
            this.childrenIterator = childrenIterator;
            this.parentOrdinals = parentOrdinals;
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

                int ord = parentOrdinals.ordinal(currentChildDoc);
                if (ord == 0) {
                    continue;
                }
                if (ordToScore.containsKey(ord)) {
                    // Can use lget b/c ordToScore is only used by one thread at the time (via CacheRecycler)
                    currentScore = ordToScore.lget();
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
            int ord = parentOrdinals.ordinal(currentChildDoc);
            if (ord == 0) {
                return nextDoc();
            }

            if (ordToScore.containsKey(ord)) {
                // Can use lget b/c ordToScore is only used by one thread at the time (via CacheRecycler)
                currentScore = ordToScore.lget();
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
