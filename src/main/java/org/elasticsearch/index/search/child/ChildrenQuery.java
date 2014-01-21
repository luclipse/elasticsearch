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
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ToStringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.ApplyAcceptedDocsFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.plain.ParentChildIndexFieldData;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * A query implementation that executes the wrapped child query and connects all the matching child docs to the related
 * parent documents using the {@link IdReaderTypeCache}.
 * <p/>
 * This query is executed in two rounds. The first round resolves all the matching child documents and groups these
 * documents by parent uid value. Also the child scores are aggregated per parent uid value. During the second round
 * all parent documents having the same uid value that is collected in the first phase are emitted as hit including
 * a score based on the aggregated child scores and score type.
 */
public class ChildrenQuery extends Query {

    private final ParentChildIndexFieldData parentChildIndexFieldData;
    private final String parentType;
    private final String childType;
    private final Filter parentFilter;
    private final ScoreType scoreType;
    private final Query originalChildQuery;
    private final int shortCircuitParentDocSet;
    private final Filter nonNestedDocsFilter;

    private Query rewrittenChildQuery;
    private IndexReader rewriteIndexReader;

    public ChildrenQuery(ParentChildIndexFieldData parentChildIndexFieldData, String parentType, String childType, Filter parentFilter, Query childQuery, ScoreType scoreType, int shortCircuitParentDocSet, Filter nonNestedDocsFilter) {
        this.parentChildIndexFieldData = parentChildIndexFieldData;
        this.parentType = parentType;
        this.childType = childType;
        this.parentFilter = parentFilter;
        this.originalChildQuery = childQuery;
        this.scoreType = scoreType;
        this.shortCircuitParentDocSet = shortCircuitParentDocSet;
        this.nonNestedDocsFilter = nonNestedDocsFilter;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        ChildrenQuery that = (ChildrenQuery) obj;
        if (!originalChildQuery.equals(that.originalChildQuery)) {
            return false;
        }
        if (!childType.equals(that.childType)) {
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
        result = 31 * result + Float.floatToIntBits(getBoost());
        return result;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("ChildrenQuery[").append(childType).append("/").append(parentType).append("](").append(originalChildQuery
                .toString(field)).append(')').append(ToStringUtils.boost(getBoost()));
        return sb.toString();
    }

    @Override
    // See TopChildrenQuery#rewrite
    public Query rewrite(IndexReader reader) throws IOException {
        if (rewrittenChildQuery == null) {
            rewriteIndexReader = reader;
            rewrittenChildQuery = originalChildQuery.rewrite(reader);
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
        Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore = searchContext.cacheRecycler().objectFloatMap(-1);
        Recycler.V<ObjectIntOpenHashMap<HashedBytesRef>> uidToCount = null;

        final Collector collector;
        switch (scoreType) {
            case MAX:
                collector = new MaxChildIdCollector(parentChildIndexFieldData, parentType, uidToScore.v());
                break;
            case SUM:
                collector = new SumChildIdCollector(parentChildIndexFieldData, parentType, uidToScore.v());
                break;
            case AVG:
                uidToCount = searchContext.cacheRecycler().objectIntMap(-1);
                collector = new AvgChildUidCollector(parentChildIndexFieldData, parentType, uidToScore.v(), uidToCount.v());
                break;
            default:
                throw new RuntimeException("Are we missing a score type here? -- " + scoreType);
        }
        final Query childQuery;
        if (rewrittenChildQuery == null) {
            childQuery = rewrittenChildQuery = searcher.rewrite(originalChildQuery);
        } else {
            assert rewriteIndexReader == searcher.getIndexReader();
            childQuery = rewrittenChildQuery;
        }
        IndexSearcher indexSearcher = new IndexSearcher(searcher.getIndexReader());
        indexSearcher.search(childQuery, collector);

        int size = uidToScore.v().size();
        if (size == 0) {
            uidToScore.release();
            if (uidToCount != null) {
                uidToCount.release();
            }
            return Queries.newMatchNoDocsQuery().createWeight(searcher);
        }

        final Filter parentFilter;
        if (size == 1) {
            BytesRef id = uidToScore.v().keys().iterator().next().value.bytes;
            if (nonNestedDocsFilter != null) {
                List<Filter> filters = Arrays.asList(
                        new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id))),
                        nonNestedDocsFilter
                );
                parentFilter = new AndFilter(filters);
            } else {
                parentFilter = new TermFilter(new Term(UidFieldMapper.NAME, Uid.createUidAsBytes(parentType, id)));
            }
        } else if (size <= shortCircuitParentDocSet) {
            parentFilter = ParentIdsFilter.b(parentType, nonNestedDocsFilter, uidToScore.v().keys, uidToScore.v().allocated);
        } else {
            parentFilter = new ApplyAcceptedDocsFilter(this.parentFilter);
        }
        ParentWeight parentWeight = new ParentWeight(rewrittenChildQuery.createWeight(searcher), parentFilter, searchContext, size, uidToScore, uidToCount);
        searchContext.addReleasable(parentWeight);
        return parentWeight;
    }

    private final class ParentWeight extends Weight implements Releasable {

        private final Weight childWeight;
        private final Filter parentFilter;
        private final SearchContext searchContext;
        private final Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore;
        private final Recycler.V<ObjectIntOpenHashMap<HashedBytesRef>> uidToCount;

        private int remaining;

        private ParentWeight(Weight childWeight, Filter parentFilter, SearchContext searchContext, int remaining, Recycler.V<ObjectFloatOpenHashMap<HashedBytesRef>> uidToScore, Recycler.V<ObjectIntOpenHashMap<HashedBytesRef>> uidToCount) {
            this.childWeight = childWeight;
            this.parentFilter = parentFilter;
            this.searchContext = searchContext;
            this.remaining = remaining;
            this.uidToScore = uidToScore;
            this.uidToCount = uidToCount;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            return new Explanation(getBoost(), "not implemented yet...");
        }

        @Override
        public Query getQuery() {
            return ChildrenQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            float sum = childWeight.getValueForNormalization();
            sum *= getBoost() * getBoost();
            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
            DocIdSet parentsSet = parentFilter.getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(parentsSet) || remaining == 0) {
                return null;
            }

            BytesValues bytesValues = parentChildIndexFieldData.load(context).getBytesValues(parentType);
            if (bytesValues == null) {
                return null;
            }

            // We can't be sure of the fact that liveDocs have been applied, so we apply it here. The "remaining"
            // count down (short circuit) logic will then work as expected.
            DocIdSetIterator parentsIterator = BitsFilteredDocIdSet.wrap(parentsSet, context.reader().getLiveDocs()).iterator();
            switch (scoreType) {
                case AVG:
                    return new AvgParentScorer(this, bytesValues, uidToScore.v(), uidToCount.v(), parentsIterator);
                default:
                    return new ParentScorer(this, bytesValues, uidToScore.v(), parentsIterator);
            }
        }

        @Override
        public boolean release() throws ElasticsearchException {
            Releasables.release(uidToScore, uidToCount);
            return true;
        }

        private class ParentScorer extends Scorer {

            final ObjectFloatOpenHashMap<HashedBytesRef> uidToScore;
            final BytesValues bytesValues;
            final DocIdSetIterator parentsIterator;
            final HashedBytesRef spare = new HashedBytesRef();

            int currentDocId = -1;
            float currentScore;

            ParentScorer(Weight weight, BytesValues bytesValues, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore, DocIdSetIterator parentsIterator) {
                super(weight);
                this.bytesValues = bytesValues;
                this.parentsIterator = parentsIterator;
                this.uidToScore = uidToScore;
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
                return currentDocId;
            }

            @Override
            public int nextDoc() throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    int numValues = bytesValues.setDocument(currentDocId);
                    if (numValues == 1) {
                        spare.bytes = bytesValues.nextValue();
                        spare.hash = bytesValues.currentValueHash();
                        if (uidToScore.containsKey(spare)) {
                            // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                            currentScore = uidToScore.lget();
                            remaining--;
                            return currentDocId;
                        }
                    } else {
                        assert numValues == 0;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                int numValues = bytesValues.setDocument(currentDocId);
                if (numValues == 1) {
                    spare.bytes = bytesValues.nextValue();
                    spare.hash = bytesValues.currentValueHash();
                    if (uidToScore.containsKey(spare)) {
                        // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                        currentScore = uidToScore.lget();
                        remaining--;
                        return currentDocId;
                    } else {
                        return nextDoc();
                    }
                } else {
                    assert numValues == 0;
                    return nextDoc();
                }
            }

            @Override
            public long cost() {
                return parentsIterator.cost();
            }
        }

        private final class AvgParentScorer extends ParentScorer {

            final ObjectIntOpenHashMap<HashedBytesRef> uidToCount;

            AvgParentScorer(Weight weight, BytesValues idTypeCache, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore, ObjectIntOpenHashMap<HashedBytesRef> uidToCount, DocIdSetIterator parentsIterator) {
                super(weight, idTypeCache, uidToScore, parentsIterator);
                this.uidToCount = uidToCount;
            }

            @Override
            public int nextDoc() throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                while (true) {
                    currentDocId = parentsIterator.nextDoc();
                    if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                        return currentDocId;
                    }

                    int numValues = bytesValues.setDocument(currentDocId);
                    if (numValues == 1) {
                        spare.bytes = bytesValues.nextValue();
                        spare.hash = bytesValues.currentValueHash();
                        if (uidToScore.containsKey(spare)) {
                            // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                            currentScore = uidToScore.lget();
                            currentScore /= uidToCount.get(spare);
                            remaining--;
                            return currentDocId;
                        }
                    } else {
                        assert numValues == 0;
                    }
                }
            }

            @Override
            public int advance(int target) throws IOException {
                if (remaining == 0) {
                    return currentDocId = NO_MORE_DOCS;
                }

                currentDocId = parentsIterator.advance(target);
                if (currentDocId == DocIdSetIterator.NO_MORE_DOCS) {
                    return currentDocId;
                }

                int numValues = bytesValues.setDocument(currentDocId);
                if (numValues == 1) {
                    spare.bytes = bytesValues.nextValue();
                    spare.hash = bytesValues.currentValueHash();
                    if (uidToScore.containsKey(spare)) {
                        // Can use lget b/c uidToScore is only used by one thread at the time (via CacheRecycler)
                        currentScore = uidToScore.lget();
                        currentScore /= uidToCount.get(spare);
                        remaining--;
                        return currentDocId;
                    } else {
                        return nextDoc();
                    }
                } else {
                    return nextDoc();
                }
            }
        }

    }

    private abstract static class ChildUidCollector extends ParentIdCollector {

        protected final ObjectFloatOpenHashMap<HashedBytesRef> uidToScore;
        protected final HashedBytesRef spare = new HashedBytesRef();
        protected Scorer scorer;

        ChildUidCollector(ParentChildIndexFieldData indexFieldData, String childType, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore) {
            super(childType, indexFieldData);
            this.uidToScore = uidToScore;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

    }

    private final static class SumChildIdCollector extends ChildUidCollector {

        private SumChildIdCollector(ParentChildIndexFieldData indexFieldData, String childType, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore) {
            super(indexFieldData, childType, uidToScore);
        }

        @Override
        protected void collect(int doc, BytesRef parentId, int hash) throws IOException {
            float currentScore = scorer.score();
            spare.bytes = parentId;
            spare.hash = hash;

            if (uidToScore.containsKey(spare)) {
                uidToScore.lset(uidToScore.lget() + currentScore);
            } else {
                uidToScore.addTo(spare.deepCopy(), currentScore);
            }
        }
    }

    private final static class MaxChildIdCollector extends ChildUidCollector {

        private MaxChildIdCollector(ParentChildIndexFieldData indexFieldData, String childType, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore) {
            super(indexFieldData, childType, uidToScore);
        }

        @Override
        protected void collect(int doc, BytesRef parentId, int hash) throws IOException {
            float currentScore = scorer.score();
            spare.bytes = parentId;
            spare.hash = hash;

            if (uidToScore.containsKey(spare)) {
                float previousScore = uidToScore.lget();
                if (currentScore > previousScore) {
                    uidToScore.lset(currentScore);
                }
            } else {
                uidToScore.put(spare.deepCopy(), currentScore);
            }
        }
    }

    private final static class AvgChildUidCollector extends ChildUidCollector {

        private final ObjectIntOpenHashMap<HashedBytesRef> uidToCount;

        AvgChildUidCollector(ParentChildIndexFieldData indexFieldData, String childType, ObjectFloatOpenHashMap<HashedBytesRef> uidToScore, ObjectIntOpenHashMap<HashedBytesRef> uidToCount) {
            super(indexFieldData, childType, uidToScore);
            this.uidToCount = uidToCount;
        }

        @Override
        protected void collect(int doc, BytesRef parentId, int hash) throws IOException {
            spare.bytes = parentId;
            spare.hash = hash;
            float currentScore = scorer.score();
            if (uidToScore.containsKey(spare)) {
                uidToScore.lset(uidToScore.lget() + currentScore);
                uidToCount.addTo(uidToScore.lkey(), 1);
            } else {
                HashedBytesRef hashedParentId = spare.deepCopy();
                uidToScore.put(hashedParentId, currentScore);
                uidToCount.put(hashedParentId, 1);
            }
        }

    }

}
