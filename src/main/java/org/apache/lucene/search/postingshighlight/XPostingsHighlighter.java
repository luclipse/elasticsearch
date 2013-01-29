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
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.*;

/**
 * Simple highlighter that does not analyze fields nor use
 * term vectors. Instead it requires
 * {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}.
 * <p/>
 * PostingsHighlighter treats the single original document as the whole corpus, and then scores individual
 * passages as if they were documents in this corpus. It uses a {@link BreakIterator} to find
 * passages in the text; by default it breaks using {@link BreakIterator#getSentenceInstance(Locale)
 * getSentenceInstance(Locale.ROOT)}. It then iterates in parallel (merge sorting by offset) through
 * the positions of all terms from the query, coalescing those hits that occur in a single passage
 * into a {@link Passage}, and then scores each Passage using a separate {@link PassageScorer}.
 * Passages are finally formatted into highlighted snippets with a {@link PassageFormatter}.
 * <p/>
 * <b>WARNING</b>: The code is very new and may still have some exciting bugs!
 * <p/>
 * Example usage:
 * <pre class="prettyprint">
 * // configure field with offsets at index time
 * FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
 * offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
 * Field body = new Field("body", "foobar", offsetsType);
 * <p/>
 * // retrieve highlights at query time
 * PostingsHighlighter highlighter = new PostingsHighlighter();
 * Query query = new TermQuery(new Term("body", "highlighting"));
 * TopDocs topDocs = searcher.search(query, n);
 * String highlights[] = highlighter.highlight("body", query, searcher, topDocs);
 * </pre>
 * <p/>
 * This is thread-safe, and can be used across different readers.
 *
 * @lucene.experimental
 */
// FORKED from Lucene to support highlighting on the _source field and threat multi valued values as separate passages.
public class XPostingsHighlighter {

    // TODO: maybe allow re-analysis for tiny fields? currently we require offsets,
    // but if the analyzer is really fast and the field is tiny, this might really be
    // unnecessary.

    /**
     * for rewriting: we don't want slow processing from MTQs
     */
    private static final IndexReader EMPTY_INDEXREADER = new MultiReader();

    /**
     * Default maximum content size to process. Typically snippets
     * closer to the beginning of the document better summarize its content
     */
    public static final int DEFAULT_MAX_LENGTH = 10000;

    private final int maxLength;
    private final BreakIterator breakIterator;
    private final PassageScorer scorer;
    private final PassageFormatter formatter;
    private final boolean fieldMatch;

    /**
     * Creates a new highlighter with default parameters.
     */
    public XPostingsHighlighter() {
        this(DEFAULT_MAX_LENGTH);
    }

    /**
     * Creates a new highlighter, specifying maximum content length.
     *
     * @param maxLength maximum content size to process.
     * @throws IllegalArgumentException if <code>maxLength</code> is negative or <code>Integer.MAX_VALUE</code>
     */
    public XPostingsHighlighter(int maxLength) {
        this(maxLength, BreakIterator.getSentenceInstance(Locale.ROOT), new PassageScorer(), new PassageFormatter(), false);
    }

    /**
     * Creates a new highlighter with custom parameters.
     *
     * @param maxLength     maximum content size to process.
     * @param breakIterator used for finding passage boundaries.
     * @param scorer        used for ranking passages.
     * @param formatter     used for formatting passages into highlighted snippets.
     * @throws IllegalArgumentException if <code>maxLength</code> is negative or <code>Integer.MAX_VALUE</code>
     */
    public XPostingsHighlighter(int maxLength, BreakIterator breakIterator, PassageScorer scorer, PassageFormatter formatter, boolean fieldMatch) {
        if (maxLength < 0 || maxLength == Integer.MAX_VALUE) {
            // two reasons: no overflow problems in BreakIterator.preceding(offset+1),
            // our sentinel in the offsets queue uses this value to terminate.
            throw new IllegalArgumentException("maxLength must be < Integer.MAX_VALUE");
        }
        if (breakIterator == null || scorer == null || formatter == null) {
            throw new NullPointerException();
        }
        this.maxLength = maxLength;
        this.breakIterator = breakIterator;
        this.scorer = scorer;
        this.formatter = formatter;
        this.fieldMatch = fieldMatch;
    }

    /**
     * Highlights the top passages from a single field.
     *
     * @param field    field name to highlight.
     *                 Must have a stored string value and also be indexed with offsets.
     * @param query    query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs  TopDocs containing the summary result documents to highlight.
     * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, its value is <code>null</code>.
     * @throws IOException              if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public String[] highlight(String field, Query query, IndexSearcher searcher, TopDocs topDocs) throws IOException {
        return highlight(field, query, searcher, topDocs, 1);
    }

    /**
     * Highlights the top-N passages from a single field.
     *
     * @param field       field name to highlight.
     *                    Must have a stored string value and also be indexed with offsets.
     * @param query       query to highlight.
     * @param searcher    searcher that was previously used to execute the query.
     * @param topDocs     TopDocs containing the summary result documents to highlight.
     * @param maxPassages The maximum number of top-N ranked passages used to
     *                    form the highlighted snippets.
     * @return Array of formatted snippets corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, its value is <code>null</code>.
     * @throws IOException              if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public String[] highlight(String field, Query query, IndexSearcher searcher, TopDocs topDocs, int maxPassages) throws IOException {
        Map<String, String[]> res = highlightFields(new String[]{field}, query, searcher, topDocs, maxPassages);
        return res.get(field);
    }

    /**
     * Highlights the top passages from multiple fields.
     * <p/>
     * Conceptually, this behaves as a more efficient form of:
     * <pre class="prettyprint">
     * Map m = new HashMap();
     * for (String field : fields) {
     * m.put(field, highlight(field, query, searcher, topDocs));
     * }
     * return m;
     * </pre>
     *
     * @param fields   field names to highlight.
     *                 Must have a stored string value and also be indexed with offsets.
     * @param query    query to highlight.
     * @param searcher searcher that was previously used to execute the query.
     * @param topDocs  TopDocs containing the summary result documents to highlight.
     * @return Map keyed on field name, containing the array of formatted snippets
     *         corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, its value is <code>null</code>.
     * @throws IOException              if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public Map<String, String[]> highlightFields(String fields[], Query query, IndexSearcher searcher, TopDocs topDocs) throws IOException {
        return highlightFields(fields, query, searcher, topDocs, 1);
    }

    /**
     * Highlights the top-N passages from multiple fields.
     * <p/>
     * Conceptually, this behaves as a more efficient form of:
     * <pre class="prettyprint">
     * Map m = new HashMap();
     * for (String field : fields) {
     * m.put(field, highlight(field, query, searcher, topDocs, maxPassages));
     * }
     * return m;
     * </pre>
     *
     * @param fields      field names to highlight.
     *                    Must have a stored string value and also be indexed with offsets.
     * @param query       query to highlight.
     * @param searcher    searcher that was previously used to execute the query.
     * @param topDocs     TopDocs containing the summary result documents to highlight.
     * @param maxPassages The maximum number of top-N ranked passages per-field used to
     *                    form the highlighted snippets.
     * @return Map keyed on field name, containing the array of formatted snippets
     *         corresponding to the documents in <code>topDocs</code>.
     *         If no highlights were found for a document, its value is <code>null</code>.
     * @throws IOException              if an I/O error occurred during processing
     * @throws IllegalArgumentException if <code>field</code> was indexed without
     *                                  {@link IndexOptions#DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS}
     */
    public Map<String, String[]> highlightFields(String fields[], Query query, IndexSearcher searcher, TopDocs topDocs, int maxPassages) throws IOException {
        final IndexReader reader = searcher.getIndexReader();
        final ScoreDoc scoreDocs[] = topDocs.scoreDocs;
        query = rewrite(query);
        SortedSet<Term> queryTerms = new TreeSet<Term>();
        query.extractTerms(queryTerms);

        int docids[] = new int[scoreDocs.length];
        for (int i = 0; i < docids.length; i++) {
            docids[i] = scoreDocs[i].doc;
        }
        IndexReaderContext readerContext = reader.getContext();
        List<AtomicReaderContext> leaves = readerContext.leaves();

        BreakIterator bi = (BreakIterator) breakIterator.clone();

        // sort for sequential io
        Arrays.sort(docids);
        Arrays.sort(fields);

        // pull stored data:
        String[][][] contents = fetchStoredData(fields, docids, searcher);

        Map<String, String[]> highlights = new HashMap<String, String[]>();
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            Term terms[];
            if (fieldMatch) {
                Term floor = new Term(field, "");
                Term ceiling = new Term(field, UnicodeUtil.BIG_TERM);
                SortedSet<Term> fieldTerms = queryTerms.subSet(floor, ceiling);
                // TODO: should we have some reasonable defaults for term pruning? (e.g. stopwords)
                terms = fieldTerms.toArray(new Term[fieldTerms.size()]);
            } else {
                terms = queryTerms.toArray(new Term[queryTerms.size()]);
            }
            Map<Integer, String> fieldHighlights = highlightField(field, contents[i], bi, terms, docids, leaves, maxPassages);

            String[] result = new String[scoreDocs.length];
            for (int j = 0; j < scoreDocs.length; j++) {
                result[j] = fieldHighlights.get(scoreDocs[j].doc);
            }
            highlights.put(field, result);
        }
        return highlights;
    }

    /**
     * we rewrite against an empty indexreader: as we don't want things like
     * rangeQueries that don't summarize the document
     */
    protected Query rewrite(Query original) throws IOException {
        Query query = original;
        for (Query rewrittenQuery = query.rewrite(EMPTY_INDEXREADER); rewrittenQuery != query;
             rewrittenQuery = query.rewrite(EMPTY_INDEXREADER)) {
            query = rewrittenQuery;
        }
        return query;
    }

    protected String[][][] fetchStoredData(String[] fields, int[] docids, IndexSearcher searcher) throws IOException {
        LimitedStoredFieldVisitor visitor = new LimitedStoredFieldVisitor(fields, maxLength);
        String contents[][][] = new String[fields.length][docids.length][1];
        for (int i = 0; i < docids.length; i++) {
            searcher.doc(docids[i], visitor);
            for (int j = 0; j < fields.length; j++) {
                contents[j][i][0] = visitor.getValue(j);
            }
            visitor.reset();
        }
        return contents;
    }

    // START CHANGE
    /*public Map<String,String[]> highlightFields(String fields[], ScoreDoc scoreDocs[], String contents[][], Query query, IndexSearcher searcher, int maxPassages, boolean fieldMatch) throws IOException {
        final IndexReader reader = searcher.getIndexReader();
        query = rewrite(query);
        SortedSet<Term> queryTerms = new TreeSet<Term>();
        query.extractTerms(queryTerms);

        int docids[] = new int[scoreDocs.length];
        for (int i = 0; i < docids.length; i++) {
            docids[i] = scoreDocs[i].doc;
        }
        IndexReaderContext readerContext = reader.getContext();
        List<AtomicReaderContext> leaves = readerContext.leaves();

        BreakIterator bi = (BreakIterator) breakIterator.clone();

        // sort for sequential io
        Arrays.sort(docids);
        Arrays.sort(fields);

        Map<String,String[]> highlights = new HashMap<String,String[]>();
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            Term[] terms;
            if (fieldMatch) {
                Term floor = new Term(field, "");
                Term ceiling = new Term(field, UnicodeUtil.BIG_TERM);
                SortedSet<Term> fieldTerms = queryTerms.subSet(floor, ceiling);
                // TODO: should we have some reasonable defaults for term pruning? (e.g. stopwords)
                terms = fieldTerms.toArray(new Term[fieldTerms.size()]);
            } else {
                terms = queryTerms.toArray(new Term[queryTerms.size()]);
            }
            Map<Integer, String> fieldHighlights = highlightField(field, contents[i], bi, terms, docids, leaves, maxPassages);

            String[] result = new String[scoreDocs.length];
            for (int j = 0; j < scoreDocs.length; j++) {
                result[j] = fieldHighlights.get(scoreDocs[j].doc);
            }
            String[] existingSnippets = highlights.get(field);
            if (existingSnippets != null) {
                String[] extendedResult = new String[existingSnippets.length + result.length];
                System.arraycopy(existingSnippets, 0, extendedResult, 0, existingSnippets.length);
                System.arraycopy(result, 0, extendedResult, existingSnippets.length, result.length);
                highlights.put(field, extendedResult);
            } else {
                highlights.put(field, result);
            }
        }
        return highlights;
    }*/
    // END CHANGE

    private Map<Integer, String> highlightField(String field, String contents[][], BreakIterator bi, Term terms[], int[] docids, List<AtomicReaderContext> leaves, int maxPassages) throws IOException {
        Map<Integer, String> highlights = new HashMap<Integer, String>();

        // reuse in the real sense... for docs in same segment we just advance our old enum
        DocsAndPositionsEnum postings[] = null;
        TermsEnum termsEnum = null;
        int lastLeaf = -1;

        for (int i = 0; i < docids.length; i++) {
            int doc = docids[i];
            int leaf = ReaderUtil.subIndex(doc, leaves);
            AtomicReaderContext subContext = leaves.get(leaf);
            AtomicReader r = subContext.reader();

            String[] docContents = contents[i];
            for (String content : docContents) {
                if (content.length() == 0) {
                    continue; // nothing to do
                }

                bi.setText(content);
                Terms t = r.terms(field);
                if (t == null) {
                    continue; // nothing to do
                }
                if (leaf != lastLeaf) {
                    termsEnum = t.iterator(termsEnum);
                    postings = new DocsAndPositionsEnum[terms.length];
                }
                Passage passages[] = highlightDoc(field, terms, content.length(), bi, doc - subContext.docBase, termsEnum, postings, maxPassages);
                if (passages.length > 0) {
                    // otherwise a null snippet
                    highlights.put(doc, formatter.format(passages, content));
                }
                lastLeaf = leaf;
            }
        }

        return highlights;
    }

    // algorithm: treat sentence snippets as miniature documents
    // we can intersect these with the postings lists via BreakIterator.preceding(offset),s
    // score each sentence as norm(sentenceStartOffset) * sum(weight * tf(freq))
    private Passage[] highlightDoc(String field, Term terms[], int contentLength, BreakIterator bi, int doc,
                                   TermsEnum termsEnum, DocsAndPositionsEnum[] postings, int n) throws IOException {
        PriorityQueue<OffsetsEnum> pq = new PriorityQueue<OffsetsEnum>();
        float weights[] = new float[terms.length];
        // initialize postings
        for (int i = 0; i < terms.length; i++) {
            DocsAndPositionsEnum de = postings[i];
            int pDoc;
            if (de == EMPTY) {
                continue;
            } else if (de == null) {
                postings[i] = EMPTY; // initially
                if (!termsEnum.seekExact(terms[i].bytes(), true)) {
                    continue; // term not found
                }
                de = postings[i] = termsEnum.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_OFFSETS);
                if (de == null) {
                    // no positions available
                    throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
                }
                pDoc = de.advance(doc);
            } else {
                pDoc = de.docID();
                if (pDoc < doc) {
                    pDoc = de.advance(doc);
                } else if (pDoc == doc) {
                    // Can only invoke nextPosition() upto freq times..., so lets recreate it...
                    de = postings[i] = termsEnum.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_OFFSETS);
                    if (de == null) {
                        // no positions available
                        throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
                    }
                    pDoc = de.advance(doc);
                }
            }

            if (doc == pDoc) {
                weights[i] = scorer.weight(contentLength, de.freq());
                de.nextPosition();
                pq.add(new OffsetsEnum(de, i));
            }
        }

        pq.add(new OffsetsEnum(EMPTY, Integer.MAX_VALUE)); // a sentinel for termination

        int initialSize = Math.min(pq.size(), n);
        PriorityQueue<Passage> passageQueue = new PriorityQueue<Passage>(initialSize, new Comparator<Passage>() {
            @Override
            public int compare(Passage left, Passage right) {
                if (right.score == left.score) {
                    return right.startOffset - left.endOffset;
                } else {
                    return right.score > left.score ? 1 : -1;
                }
            }
        });
        Passage current = new Passage();

        OffsetsEnum off;
        while ((off = pq.poll()) != null) {
            final DocsAndPositionsEnum dp = off.dp;
            int start = dp.startOffset();
            if (start == -1) {
                throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
            }
            int end = dp.endOffset();
            if (start > current.endOffset) {
                if (current.startOffset >= 0) {
                    // finalize current
                    current.score *= scorer.norm(current.startOffset);
                    // new sentence: first add 'current' to queue
                    if (passageQueue.size() == n && current.score < passageQueue.peek().score) {
                        current.reset(); // can't compete, just reset it
                    } else {
                        passageQueue.offer(current);
                        if (passageQueue.size() > n) {
                            current = passageQueue.poll();
                            current.reset();
                        } else {
                            current = new Passage();
                        }
                    }
                }
                // if we exceed limit, we are done
                if (start >= contentLength) {
                    Passage passages[] = new Passage[passageQueue.size()];
                    passageQueue.toArray(passages);
                    // sort in ascending order
                    Arrays.sort(passages, new Comparator<Passage>() {
                        @Override
                        public int compare(Passage left, Passage right) {
                            return left.startOffset - right.startOffset;
                        }
                    });
                    return passages;
                }
                // advance breakiterator
                assert BreakIterator.DONE < 0;
                current.startOffset = Math.max(bi.preceding(start + 1), 0);
                current.endOffset = Math.min(bi.next(), contentLength);
            }
            int tf = 0;
            while (true) {
                tf++;
                current.addMatch(start, end, terms[off.id]);
                if (off.pos == dp.freq()) {
                    break; // removed from pq
                } else {
                    off.pos++;
                    dp.nextPosition();
                    start = dp.startOffset();
                    end = dp.endOffset();
                }
                if (start >= current.endOffset) {
                    pq.offer(off);
                    break;
                }
            }
            current.score += weights[off.id] * scorer.tf(tf, current.endOffset - current.startOffset);
        }
        return new Passage[0];
    }

    private static class OffsetsEnum implements Comparable<OffsetsEnum> {
        DocsAndPositionsEnum dp;
        int pos;
        int id;

        OffsetsEnum(DocsAndPositionsEnum dp, int id) throws IOException {
            this.dp = dp;
            this.id = id;
            this.pos = 1;
        }

        @Override
        public int compareTo(OffsetsEnum other) {
            try {
                int off = dp.startOffset();
                int otherOff = other.dp.startOffset();
                if (off == otherOff) {
                    return id - other.id;
                } else {
                    return Long.signum(((long) off) - otherOff);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final DocsAndPositionsEnum EMPTY = new DocsAndPositionsEnum() {

        @Override
        public int nextPosition() throws IOException {
            return 0;
        }

        @Override
        public int startOffset() throws IOException {
            return Integer.MAX_VALUE;
        }

        @Override
        public int endOffset() throws IOException {
            return Integer.MAX_VALUE;
        }

        @Override
        public BytesRef getPayload() throws IOException {
            return null;
        }

        @Override
        public int freq() throws IOException {
            return 0;
        }

        @Override
        public int docID() {
            return NO_MORE_DOCS;
        }

        @Override
        public int nextDoc() throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            return NO_MORE_DOCS;
        }
    };

    private static class LimitedStoredFieldVisitor extends StoredFieldVisitor {
        private final String fields[];
        private final int maxLength;
        private final StringBuilder builders[];
        private int currentField = -1;

        public LimitedStoredFieldVisitor(String fields[], int maxLength) {
            this.fields = fields;
            this.maxLength = maxLength;
            builders = new StringBuilder[fields.length];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = new StringBuilder();
            }
        }

        @Override
        public void stringField(FieldInfo fieldInfo, String value) throws IOException {
            assert currentField >= 0;
            StringBuilder builder = builders[currentField];
            if (builder.length() > 0) {
                builder.append(' '); // for the offset gap, TODO: make this configurable
            }
            if (builder.length() + value.length() > maxLength) {
                builder.append(value, 0, maxLength - builder.length());
            } else {
                builder.append(value);
            }
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            currentField = Arrays.binarySearch(fields, fieldInfo.name);
            if (currentField < 0) {
                return Status.NO;
            } else if (builders[currentField].length() > maxLength) {
                return fields.length == 1 ? Status.STOP : Status.NO;
            }
            return Status.YES;
        }

        String getValue(int i) {
            return builders[i].toString();
        }

        void reset() {
            currentField = -1;
            for (int i = 0; i < fields.length; i++) {
                builders[i].setLength(0);
            }
        }
    }
}
