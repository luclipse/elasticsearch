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

package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.parentordinals.ParentOrdinals;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Set;

/**
 *
 */
public class ChildrenConstantScoreQuery extends Query {

    private final Query originalChildQuery;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private final BytesRef spare = new BytesRef();

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenConstantScoreQuery(Query childQuery, String parentType, String childType, Filter parentFilter, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.parentFilter = parentFilter;
        this.parentType = parentType;
        this.childType = childType;
        this.originalChildQuery = childQuery;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
            rewriteIndexReader = reader;
        }
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        rewrittenChildQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        SearchContext searchContext = SearchContext.current();
        OrdCollector collector = new OrdCollector(parentType, searchContext);
        final Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(childQuery, collector);
        OpenBitSet collectedOrds = collector.collectedOrds;

        long remaining = collectedOrds.cardinality();
        if (remaining == 0) {
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        Filter shortCircuitFilter = null;
        if (searchContext.parentOrdinals().supportsValueLookup()) {
            if (remaining == 1) {
                BytesRef id = searchContext.parentOrdinals().parentValue(collectedOrds.iterator().nextDoc(), spare);
                shortCircuitFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            } else if (remaining <= shortCircuitParentDocSet) {
                ObjectOpenHashSet<HashedBytesArray> parentIds = new ObjectOpenHashSet<HashedBytesArray>();
                DocIdSetIterator iterator = collectedOrds.iterator();
                for (int ordinal = iterator.nextDoc(); ordinal != DocIdSetIterator.NO_MORE_DOCS; ordinal = iterator.nextDoc()) {
                    BytesRef parentId = searchContext.parentOrdinals().parentValue(ordinal, spare);
                    byte[] bytes = new byte[parentId.length];
                    System.arraycopy(parentId.bytes, parentId.offset, bytes, 0, parentId.length);
                    parentIds.add(new HashedBytesArray(bytes));
                }
                shortCircuitFilter = new ParentIdsFilter(parentType, parentIds.keys, parentIds.allocated, nonNestedDocsFilter);
            }
        }

        return new ParentWeight(parentFilter, shortCircuitFilter, searchContext, collectedOrds);
    }

    private final class ParentWeight extends Weight {

        private final Filter parentFilter;
        private final Filter shortCircuitFilter;
        private final SearchContext searchContext;
        private final OpenBitSet collectedOrds;

        private long remaining;
        private float queryNorm;
        private float queryWeight;

        public ParentWeight(Filter parentFilter, Filter shortCircuitFilter, SearchContext searchContext, OpenBitSet collectedOrds) {
            this.parentFilter = new ApplyAcceptedDocsFilter(parentFilter);
            this.shortCircuitFilter = shortCircuitFilter;
            this.searchContext = searchContext;
            this.collectedOrds = collectedOrds;
            this.remaining = collectedOrds.cardinality();
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenConstantScoreQuery.this;
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
            if (remaining == 0) {
                return null;
            }

            if (shortCircuitFilter != null) {
                DocIdSet docIdSet = shortCircuitFilter.getDocIdSet(context, acceptDocs);
                if (!DocIdSets.isEmpty(docIdSet)) {
                    DocIdSetIterator iterator = docIdSet.iterator();
                    if (iterator != null) {
                        return ConstantScorer.create(iterator, this, queryWeight);
                    }
                }
                return null;
            }

            DocIdSet parentDocIdSet = this.parentFilter.getDocIdSet(context, acceptDocs);
            if (!DocIdSets.isEmpty(parentDocIdSet)) {
                ParentOrdinals parentOrdinals = searchContext.parentOrdinals().ordinals(parentType, context);
                // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
                // count down (short circuit) logic will then work as expected.
                parentDocIdSet = BitsFilteredDocIdSet.wrap(parentDocIdSet, context.reader().getLiveDocs());
                if (!parentOrdinals.isEmpty()) {
                    DocIdSetIterator innerIterator = parentDocIdSet.iterator();
                    if (innerIterator != null) {
                        ParentDocIdIterator parentDocIdIterator = new ParentDocIdIterator(innerIterator, collectedOrds, parentOrdinals);
                        return ConstantScorer.create(parentDocIdIterator, this, queryWeight);
                    }
                }
            }
            return null;
        }

        private final class ParentDocIdIterator extends FilteredDocIdSetIterator {

            private final OpenBitSet collectedOrds;
            private final ParentOrdinals parentOrdinals;

            private ParentDocIdIterator(DocIdSetIterator innerIterator, OpenBitSet collectedOrds, ParentOrdinals parentOrdinals) {
                super(innerIterator);
                this.collectedOrds = collectedOrds;
                this.parentOrdinals = parentOrdinals;
            }

            @Override
            protected boolean match(int doc) {
                if (remaining == 0) {
                    try {
                        advance(DocIdSetIterator.NO_MORE_DOCS);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return false;
                }

                int ord = parentOrdinals.ordinal(doc);
                boolean match = collectedOrds.get(ord);
                if (match) {
                    remaining--;
                }
                return match;
            }
        }
    }

    private final static class OrdCollector extends ParentOrdCollector {

        private final OpenBitSet collectedOrds;

        OrdCollector(String parentType, SearchContext searchContext) {
            super(parentType, searchContext);
            this.collectedOrds = new OpenBitSet();
        }

        @Override
        protected void collect(int doc, int parentOrd) throws IOException {
            collectedOrds.set(parentOrd);
        }

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenConstantScoreQuery that = (ChildrenConstantScoreQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
            return false;
        }
        if (shortCircuitParentDocSet != that.shortCircuitParentDocSet) {
            return false;
        }
        if (getBoost() != that.getBoost()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = originalChildQuery.hashCode();
        result = 31 * result + childType.hashCode();
        result = 31 * result + shortCircuitParentDocSet;
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        return "child_filter[" + childType + "/" + parentType + "](" + originalChildQuery + ')';
    }

}
