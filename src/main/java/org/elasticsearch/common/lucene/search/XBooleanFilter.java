package org.elasticsearch.common.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Similar to {@link org.apache.lucene.queries.BooleanFilter}.
 * <p/>
 * Our own variance mainly differs by the fact that we pass the acceptDocs down to the filters
 * and don't filter based on them at the end. Our logic is a bit different, and we filter based on that
 * at the top level filter chain.
 */
// TODO: Rename to SmartBooleanFilter?
public class XBooleanFilter extends Filter implements Iterable<FilterClause> {

    final List<FilterClause> shouldClauses = new ArrayList<FilterClause>();
    final List<FilterClause> mustNotClauses = new ArrayList<FilterClause>();
    final List<FilterClause> mustClauses = new ArrayList<FilterClause>();

    final List<FilterClause> clauses = new ArrayList<FilterClause>();

    /**
     * Returns the a DocIdSetIterator representing the Boolean composition
     * of the filters that have been added.
     */
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        boolean onlyShould = mustClauses.isEmpty() && mustNotClauses.isEmpty() && !shouldClauses.isEmpty();
        boolean onlyMust = shouldClauses.isEmpty() && mustNotClauses.isEmpty() && !mustClauses.isEmpty();
        boolean onlyMustNot = shouldClauses.isEmpty() && mustClauses.isEmpty() && !mustNotClauses.isEmpty();

        List<Bits> bits = new ArrayList<Bits>();
        List<DocIdSetIterator> iterators = new ArrayList<DocIdSetIterator>();
        final AtomicReader reader = context.reader();

        if (onlyShould) {
            split(shouldClauses, context, acceptDocs, bits, iterators);
            if (iterators.isEmpty()) {
                List<Filter> shouldFilters = new ArrayList<Filter>(shouldClauses.size());
                for (FilterClause shouldClause : shouldClauses) {
                    shouldFilters.add(shouldClause.getFilter());
                }
                return new OrFilter(shouldFilters).getDocIdSet(context, acceptDocs);
            } else {
                FixedBitSet result = new FixedBitSet(context.reader().maxDoc());
                result.or(iterators.remove(0));
                for (DocIdSetIterator disi : iterators) {
                    result.or(disi);
                }
                if (!bits.isEmpty()) {
                    for (int doc = result.nextSetBit(0); doc != -1; doc = result.nextSetBit(doc)) {
                        for (Bits slowFilter : bits) {
                            if (!slowFilter.get(doc)) {
                                result.clear(doc);
                                break;
                            }
                        }
                    }
                }
                return result;
            }
        } else if (onlyMustNot) {
            split(mustNotClauses, context, acceptDocs, bits, iterators);
            if (iterators.isEmpty()) {
                List<Filter> mustNotFilters = new ArrayList<Filter>(mustNotClauses.size());
                for (FilterClause mustNotClause : mustNotClauses) {
                    mustNotFilters.add(mustNotClause.getFilter());
                }
                return new NotFilter(new AndFilter(mustNotFilters)).getDocIdSet(context, acceptDocs);
            } else {
                FixedBitSet result = new FixedBitSet(reader.maxDoc());
                result.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
                for (DocIdSetIterator iterator : iterators) {
                    result.andNot(iterator);
                }
                if (!bits.isEmpty()) {
                    for (int doc = result.nextSetBit(0); doc != -1; doc = result.nextSetBit(doc)) {
                        for (Bits slowFilter : bits) {
                            if (slowFilter.get(doc)) {
                                result.clear(doc);
                                break;
                            }
                        }
                    }
                }
                return result;
            }
        } else if (onlyMust) {
            if (splitForMust(mustClauses, context, acceptDocs, bits, iterators)) {
                return null; // One must clause has not matches, shortcut to exit
            }
            if (iterators.isEmpty()) {
                List<Filter> mustFilters = new ArrayList<Filter>(mustClauses.size());
                for (FilterClause mustClause : mustClauses) {
                    mustFilters.add(mustClause.getFilter());
                }
                return new AndFilter(mustFilters).getDocIdSet(context, acceptDocs);
            } else {
                FixedBitSet result = new FixedBitSet(context.reader().maxDoc());
                result.or(iterators.remove(0));
                for (DocIdSetIterator disi : iterators) {
                    result.and(disi);
                }
                if (!bits.isEmpty()) {
                    for (int doc = result.nextSetBit(0); doc != -1; doc = result.nextSetBit(doc)) {
                        for (Bits slowFilter : bits) {
                            if (!slowFilter.get(doc)) {
                                result.clear(doc);
                                break;
                            }
                        }
                    }
                }
                return result;
            }
        }

        FixedBitSet res = null;
        if (!shouldClauses.isEmpty()) {
            res = handleShouldClauses(res, context, acceptDocs, bits, iterators);
            if (res == null) {
                return null;
            }
        }
        if (!mustNotClauses.isEmpty()) {
            res = handleNotClauses(res, context, acceptDocs, bits, iterators);
        }
        assert res != null;
        if (!mustClauses.isEmpty()) {
            res = handleMustClauses(res, context, acceptDocs, bits, iterators);
        }

        // don't wrap, based on our own strategy of doing the wrapping on the filtered query level
        //return res != null ? BitsFilteredDocIdSet.wrap(res, acceptDocs) : DocIdSet.EMPTY_DOCIDSET;
        return res;
    }

    private FixedBitSet handleMustClauses(FixedBitSet res, AtomicReaderContext context, Bits acceptDocs, List<Bits> bits, List<DocIdSetIterator> iterators) throws IOException {
        if (splitForMust(mustClauses, context, acceptDocs, bits, iterators)) {
            return null;
        }
        if (iterators.isEmpty()) {
            if (res == null) {
                res = new FixedBitSet(context.reader().maxDoc());
                res.or(new BitsDocIdSetIterator(new AndDocIdSet.AndBits(bits.toArray(new Bits[bits.size()]))));
            } else {
                res.and(new BitsDocIdSetIterator(new AndDocIdSet.AndBits(bits.toArray(new Bits[bits.size()]))));
            }
            return res;
        } else {
            if (res == null) {
                res = new FixedBitSet(context.reader().maxDoc());
                res.or(iterators.remove(0));
            }
            for (DocIdSetIterator disi : iterators) {
                res.and(disi);
            }
            if (!bits.isEmpty()) {
                for (int doc = res.nextSetBit(0); doc != -1; doc = res.nextSetBit(doc)) {
                    for (Bits slowFilter : bits) {
                        if (!slowFilter.get(doc)) {
                            res.clear(doc);
                            break;
                        }
                    }
                }
            }
            return res;
        }
    }

    private FixedBitSet handleShouldClauses(FixedBitSet res, AtomicReaderContext context, Bits acceptDocs, List<Bits> bits, List<DocIdSetIterator> iterators) throws IOException {
        split(mustClauses, context, acceptDocs, bits, iterators);

        if (iterators.isEmpty()) {
            if (res == null) {
                res = new FixedBitSet(context.reader().maxDoc());
                res.or(new BitsDocIdSetIterator(new OrDocIdSet.OrBits(bits.toArray(new Bits[bits.size()]))));
            } else {
                res.and(new BitsDocIdSetIterator(new OrDocIdSet.OrBits(bits.toArray(new Bits[bits.size()]))));
            }
            return res;
        } else {
            if (res == null) {
                res = new FixedBitSet(context.reader().maxDoc());
                res.or(iterators.remove(0));
            }
            for (DocIdSetIterator disi : iterators) {
                res.or(disi);
            }
            if (!bits.isEmpty()) {
                int previousSetBit = res.nextSetBit(0);
                for (int currentSetBit = res.nextSetBit(previousSetBit); currentSetBit != -1; currentSetBit = res.nextSetBit(currentSetBit)) {
                    int diff = currentSetBit - previousSetBit;
                    if (diff > 1) {
                        for (int unsetDocBit = previousSetBit + 1; unsetDocBit < currentSetBit; unsetDocBit++) {
                            for (Bits slowFilter : bits) {
                                if (slowFilter.get(unsetDocBit)) {
                                    res.set(unsetDocBit);
                                    break;
                                }
                            }
                        }
                    }
                    previousSetBit = currentSetBit;
                }
            }
            return res;
        }
    }

    private FixedBitSet handleNotClauses(FixedBitSet res, AtomicReaderContext context, Bits acceptDocs, List<Bits> bits, List<DocIdSetIterator> iterators) throws IOException {
        split(mustNotClauses, context, acceptDocs, bits, iterators);

        if (iterators.isEmpty()) {
            if (res == null) {
                assert !shouldClauses.isEmpty();
                res = new FixedBitSet(context.reader().maxDoc());
                res.set(0, context.reader().maxDoc()); // NOTE: may set bits on deleted docs
                res.andNot(new BitsDocIdSetIterator(new NotDocIdSet.NotBits(new AndDocIdSet.AndBits(bits.toArray(new Bits[bits.size()])))));
            } else {
                for (int doc = res.nextSetBit(0); doc != -1; doc = res.nextSetBit(doc)) {
                    for (Bits slowFilter : bits) {
                        if (slowFilter.get(doc)) {
                            res.clear(doc);
                            break;
                        }
                    }
                }
            }
            return res;
        } else {
            if (res == null) {
                assert !shouldClauses.isEmpty();
                res = new FixedBitSet(context.reader().maxDoc());
                res.set(0, context.reader().maxDoc()); // NOTE: may set bits on deleted docs
            }
            for (DocIdSetIterator iter : iterators) {
                res.andNot(iter);
            }
            if (!bits.isEmpty()) {
                for (int doc = res.nextSetBit(0); doc != -1; doc = res.nextSetBit(doc)) {
                    for (Bits slowFilter : bits) {
                        if (slowFilter.get(doc)) {
                            res.clear(doc);
                            break;
                        }
                    }
                }
            }
            return res;
        }
    }

    private static void split(List<FilterClause> clauses, AtomicReaderContext context, Bits acceptDocs, List<Bits> bits, List<DocIdSetIterator> iterators) throws IOException {
        bits.clear();
        iterators.clear();
        for (final FilterClause fc : clauses) {
            final DocIdSetIterator disi = getDISI(fc.getFilter(), context, acceptDocs);
            if (disi == null) {
                continue;
            }
            if (slowBits(disi)) {
                bits.add((Bits) disi);
            } else {
                iterators.add(disi);
            }
        }
    }

    private static boolean splitForMust(List<FilterClause> clauses, AtomicReaderContext context, Bits acceptDocs, List<Bits> bits, List<DocIdSetIterator> iterators) throws IOException {
        bits.clear();
        iterators.clear();
        for (final FilterClause fc : clauses) {
            final DocIdSetIterator disi = getDISI(fc.getFilter(), context, acceptDocs);
            if (disi == null) {
                return true;
            }
            if (slowBits(disi)) {
                bits.add((Bits) disi);
            } else {
                iterators.add(disi);
            }
        }
        return false;
    }

    private static DocIdSetIterator getDISI(Filter filter, AtomicReaderContext context, Bits acceptDocs)
            throws IOException {
        final DocIdSet set = filter.getDocIdSet(context, acceptDocs);
        return (set == null || set == DocIdSet.EMPTY_DOCIDSET) ? null : set.iterator();
    }

    private static boolean slowBits(final DocIdSetIterator disi) {
        return !DocIdSets.isFastIterator(disi) && disi instanceof Bits;
    }

    /**
     * Adds a new FilterClause to the Boolean Filter container
     *
     * @param filterClause A FilterClause object containing a Filter and an Occur parameter
     */
    public void add(FilterClause filterClause) {
        if (filterClause.getOccur() == Occur.SHOULD) {
            shouldClauses.add(filterClause);
        } else if (filterClause.getOccur() == Occur.MUST_NOT) {
            mustNotClauses.add(filterClause);
        } else if (filterClause.getOccur() == Occur.MUST) {
            mustClauses.add(filterClause);
        }

        clauses.add(filterClause);
    }

    public final void add(Filter filter, Occur occur) {
        add(new FilterClause(filter, occur));
    }

    /**
     * Returns the list of clauses
     */
    public List<FilterClause> clauses() {
        return clauses;
    }

    /**
     * Returns an iterator on the clauses in this query. It implements the {@link Iterable} interface to
     * make it possible to do:
     * <pre class="prettyprint">for (FilterClause clause : booleanFilter) {}</pre>
     */
    public final Iterator<FilterClause> iterator() {
        return clauses().iterator();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (obj.getClass() != this.getClass())) {
            return false;
        }

        final XBooleanFilter other = (XBooleanFilter) obj;
        return clauses.equals(other.clauses);
    }

    @Override
    public int hashCode() {
        return 657153718 ^ clauses.hashCode();
    }

    /**
     * Prints a user-readable version of this Filter.
     */
    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder("BooleanFilter(");
        final int minLen = buffer.length();
        for (final FilterClause c : clauses) {
            if (buffer.length() > minLen) {
                buffer.append(' ');
            }
            buffer.append(c);
        }
        return buffer.append(')').toString();
    }
}
