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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.NoopCollector;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 * A query that only return child documents that are linked to the parent documents that matched with the inner query.
 */
public class ParentConstantScoreQuery extends Query {

    private final Query originalParentQuery;
    private final String parentType;
    private final Filter childrenFilter;

    private Query rewrittenParentQuery;
    private IndexReader rewriteIndexReader;

    public ParentConstantScoreQuery(Query parentQuery, String parentType, Filter childrenFilter) {
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
        final Query parentQuery;
        if (rewrittenParentQuery != null) {
            parentQuery = rewrittenParentQuery;
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            parentQuery = rewrittenParentQuery = originalParentQuery.rewrite(searcher.getIndexReader());
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        ParentOrdsCollector collector = new ParentOrdsCollector(searchContext, parentType);
        indexSearcher.search(parentQuery, collector);
        OpenBitSet collectedOrds = collector.collectedOrds;

        if (collectedOrds.cardinality() == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        return new ChildrenWeight(childrenFilter, searchContext, collectedOrds);
    }

    private final class ChildrenWeight extends Weight {

        private final Filter childrenFilter;
        private final SearchContext searchContext;
        private final OpenBitSet collectedOrds;

        private float queryNorm;
        private float queryWeight;

        private ChildrenWeight(Filter childrenFilter, SearchContext searchContext, OpenBitSet collectedOrds) {
            this.childrenFilter = new ApplyAcceptedDocsFilter(childrenFilter);
            this.searchContext = searchContext;
            this.collectedOrds = collectedOrds;
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

            ParentOrdinals.Segment parentOrdinals = searchContext.parentOrdinalService().current().ordinals(parentType, context);
            if (parentOrdinals != null) {
                DocIdSetIterator innerIterator = childrenDocIdSet.iterator();
                if (innerIterator != null) {
                    ChildrenDocIdIterator childrenDocIdIterator = new ChildrenDocIdIterator(innerIterator, collectedOrds, parentOrdinals);
                    return ConstantScorer.create(childrenDocIdIterator, this, queryWeight);
                }
            }
            return null;
        }

        private final class ChildrenDocIdIterator extends FilteredDocIdSetIterator {

            private final OpenBitSet collectedUids;
            private final ParentOrdinals.Segment parentOrdinals;

            ChildrenDocIdIterator(DocIdSetIterator innerIterator, OpenBitSet collectedUids, ParentOrdinals.Segment parentOrdinals) {
                super(innerIterator);
                this.collectedUids = collectedUids;
                this.parentOrdinals = parentOrdinals;
            }

            @Override
            protected boolean match(int doc) {
                int ordinal = parentOrdinals.ordinal(doc);
                return collectedUids.get(ordinal);
            }

        }
    }

    private final static class ParentOrdsCollector extends NoopCollector {

        private final OpenBitSet collectedOrds;
        private final SearchContext context;
        private final String parentType;

        private ParentOrdinals.Segment parentOrdinals;

        ParentOrdsCollector(SearchContext context, String parentType) {
            this.collectedOrds = new OpenBitSet();
            this.context = context;
            this.parentType = parentType;
        }

        public void collect(int doc) throws IOException {
            // It can happen that for particular segment no document exist for an specific type. This prevents NPE
            if (parentOrdinals != null) {
                int ord = parentOrdinals.ordinal(doc);
                collectedOrds.set(ord);
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext readerContext) throws IOException {
            parentOrdinals = context.parentOrdinalService().current().ordinals(parentType, readerContext);
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
        return "parent_filter[" + parentType + "](" + originalParentQuery + ')';
    }

}

