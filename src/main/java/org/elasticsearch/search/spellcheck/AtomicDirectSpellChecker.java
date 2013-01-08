package org.elasticsearch.search.spellcheck;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spell.*;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.text.BytesText;

import java.io.IOException;
import java.util.*;

/**
 * Forked this from Lucene's direct spellchecker.
 * This spellchecker works on an atomic reader instead of a top level reader, in order to support spellcheck
 * filter efficiently. Performance slowdown of this impl compared to direct spellchecker seems around 12%
 */
class AtomicDirectSpellChecker {

    /**
     * The default StringDistance, Damerau-Levenshtein distance implemented internally
     * via {@link LevenshteinAutomata}.
     * <p/>
     * Note: this is the fastest distance metric, because Damerau-Levenshtein is used
     * to draw candidates from the term dictionary: this just re-uses the scoring.
     */
    public static final StringDistance INTERNAL_LEVENSHTEIN = new LuceneLevenshteinDistance();

    /**
     * maximum edit distance for candidate terms
     */
    private int maxEdits = LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE;
    /**
     * minimum prefix for candidate terms
     */
    private int minPrefix = 1;
    /**
     * maximum number of top-N inspections per suggestion
     */
    private int maxInspections = 5;
    /**
     * minimum accuracy for a term to match
     */
    private float accuracy = SpellChecker.DEFAULT_ACCURACY;
    /**
     * value in [0..1] (or absolute number >=1) representing the minimum
     * number of documents (of the total) where a term should appear.
     */
    private float thresholdFrequency = 0f;
    /**
     * minimum length of a query word to return suggestions
     */
    private int minQueryLength = 4;
    /**
     * value in [0..1] (or absolute number >=1) representing the maximum
     * number of documents (of the total) a query term can appear in to
     * be corrected.
     */
    private float maxQueryFrequency = 0.01f;
    /**
     * true if the spellchecker should lowercase terms
     */
    private boolean lowerCaseTerms = true;
    /**
     * the comparator to use
     */
    private Comparator<SuggestedWord> comparator = scoreFirst;
    /**
     * the string distance to use
     */
    private StringDistance distance = INTERNAL_LEVENSHTEIN;

    private Filter filter;

    /**
     * Creates a DirectSpellChecker with default configuration values
     */
    AtomicDirectSpellChecker() {
    }

    /**
     * Sets the maximum number of Levenshtein edit-distances to draw
     * candidate terms from. This value can be 1 or 2. The default is 2.
     * <p/>
     * Note: a large number of spelling errors occur with an edit distance
     * of 1, by setting this value to 1 you can increase both performance
     * and precision at the cost of recall.
     */
    public void setMaxEdits(int maxEdits) {
        if (maxEdits < 1 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE)
            throw new UnsupportedOperationException("Invalid maxEdits");
        this.maxEdits = maxEdits;
    }

    /**
     * Sets the minimal number of initial characters (default: 1)
     * that must match exactly.
     * <p/>
     * This can improve both performance and accuracy of results,
     * as misspellings are commonly not the first character.
     */
    public void setMinPrefix(int minPrefix) {
        this.minPrefix = minPrefix;
    }

    /**
     * Set the maximum number of top-N inspections (default: 5) per suggestion.
     * <p/>
     * Increasing this number can improve the accuracy of results, at the cost
     * of performance.
     */
    public void setMaxInspections(int maxInspections) {
        this.maxInspections = maxInspections;
    }

    /**
     * Set the minimal accuracy required (default: 0.5f) from a StringDistance
     * for a suggestion match.
     */
    public void setAccuracy(float accuracy) {
        this.accuracy = accuracy;
    }

    /**
     * Set the minimal threshold of documents a term must appear for a match.
     * <p/>
     * This can improve quality by only suggesting high-frequency terms. Note that
     * very high values might decrease performance slightly, by forcing the spellchecker
     * to draw more candidates from the term dictionary, but a practical value such
     * as <code>1</code> can be very useful towards improving quality.
     * <p/>
     * This can be specified as a relative percentage of documents such as 0.5f,
     * or it can be specified as an absolute whole document frequency, such as 4f.
     * Absolute document frequencies may not be fractional.
     */
    public void setThresholdFrequency(float thresholdFrequency) {
        if (thresholdFrequency >= 1f && thresholdFrequency != (int) thresholdFrequency)
            throw new IllegalArgumentException("Fractional absolute document frequencies are not allowed");
        this.thresholdFrequency = thresholdFrequency;
    }

    /**
     * Set the minimum length of a query term (default: 4) needed to return suggestions.
     * <p/>
     * Very short query terms will often cause only bad suggestions with any distance
     * metric.
     */
    public void setMinQueryLength(int minQueryLength) {
        this.minQueryLength = minQueryLength;
    }

    /**
     * Set the maximum threshold (default: 0.01f) of documents a query term can
     * appear in order to provide suggestions.
     * <p/>
     * Very high-frequency terms are typically spelled correctly. Additionally,
     * this can increase performance as it will do no work for the common case
     * of correctly-spelled input terms.
     * <p/>
     * This can be specified as a relative percentage of documents such as 0.5f,
     * or it can be specified as an absolute whole document frequency, such as 4f.
     * Absolute document frequencies may not be fractional.
     */
    public void setMaxQueryFrequency(float maxQueryFrequency) {
        if (maxQueryFrequency >= 1f && maxQueryFrequency != (int) maxQueryFrequency)
            throw new IllegalArgumentException("Fractional absolute document frequencies are not allowed");
        this.maxQueryFrequency = maxQueryFrequency;
    }

    /**
     * True if the spellchecker should lowercase terms (default: true)
     * <p/>
     * This is a convenience method, if your index field has more complicated
     * analysis (such as StandardTokenizer removing punctuation), its probably
     * better to turn this off, and instead run your query terms through your
     * Analyzer first.
     * <p/>
     * If this option is not on, case differences count as an edit!
     */
    public void setLowerCaseTerms(boolean lowerCaseTerms) {
        this.lowerCaseTerms = lowerCaseTerms;
    }

    /**
     * Set the comparator for sorting suggestions.
     * The default is {@link SuggestWordQueue#DEFAULT_COMPARATOR}
     */
    public void setComparator(Comparator<SuggestedWord> comparator) {
        this.comparator = comparator;
    }

    /**
     * Set the string distance metric.
     * The default is {@link #INTERNAL_LEVENSHTEIN}
     * <p/>
     * Note: because this spellchecker draws its candidates from the term
     * dictionary using Damerau-Levenshtein, it works best with an edit-distance-like
     * string metric. If you use a different metric than the default,
     * you might want to consider increasing {@link #setMaxInspections(int)}
     * to draw more candidates for your metric to rank.
     */
    public void setDistance(StringDistance distance) {
        this.distance = distance;
    }

    /**
     * Sets The filter the suggested words need to match with.
     *
     * @param filter The filter the suggested words need to match with.
     */
    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    /**
     * Suggest similar words.
     * <p/>
     * <p>Unlike {@link SpellChecker}, the similarity used to fetch the most
     * relevant terms is an edit distance, therefore typically a low value
     * for numSug will work very well.
     *
     * @param term           Term you want to spell check on
     * @param numSug         the maximum number of suggested words
     * @param readerContexts
     * @param suggestMode    specifies when to return suggested words
     * @return sorted list of the suggested words according to the comparator
     * @throws IOException If there is a low-level I/O error.
     */
    public List<SuggestedWord> suggestSimilar(Term term, int numSug, List<AtomicReaderContext> readerContexts, SuggestMode suggestMode) throws IOException {
        final CharsRef spare = new CharsRef();
        String text = term.text();
        if (minQueryLength > 0 && text.codePointCount(0, text.length()) < minQueryLength)
            return Collections.emptyList();

        if (lowerCaseTerms) {
            term = new Term(term.field(), text.toLowerCase(Locale.ROOT));
        }

        int docfreq = 0;
        for (AtomicReaderContext readerContext : readerContexts) {
            docfreq += readerContext.reader().docFreq(term);
        }

        if (suggestMode == SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX && docfreq > 0) {
            return Collections.emptyList();
        }

        int maxDoc = 0;
        for (AtomicReaderContext readerContext : readerContexts) {
            maxDoc += readerContext.reader().maxDoc();
        }

        if (maxQueryFrequency >= 1f && docfreq > maxQueryFrequency) {
            return Collections.emptyList();
        } else if (docfreq > (int) Math.ceil(maxQueryFrequency * (float) maxDoc)) {
            return Collections.emptyList();
        }

        if (suggestMode != SuggestMode.SUGGEST_MORE_POPULAR) {
            docfreq = 0;
        }

        if (thresholdFrequency >= 1f) {
            docfreq = Math.max(docfreq, (int) thresholdFrequency);
        } else if (thresholdFrequency > 0f) {
            docfreq = Math.max(docfreq, (int) (thresholdFrequency * (float) maxDoc) - 1);
        }

        int inspections = numSug * maxInspections;
        // try ed=1 first, in case we get lucky
        Collection<ScoreTerm> terms = suggestSimilar(term, inspections, readerContexts, docfreq, 1, accuracy, spare);
        if (maxEdits > 1 && terms.size() < inspections) {
            HashSet<ScoreTerm> moreTerms = new HashSet<ScoreTerm>();
            moreTerms.addAll(terms);
            moreTerms.addAll(suggestSimilar(term, inspections, readerContexts, docfreq, maxEdits, accuracy, spare));
            terms = moreTerms;
        }

        // create the suggestword response, sort it, and trim it to size.

        List<SuggestedWord> suggestions = new ArrayList<SuggestedWord>();
        for (ScoreTerm s : terms) {
            suggestions.add(new SuggestedWord(new BytesText(new BytesArray(s.term)), s.docfreq, s.score));
        }
        Collections.sort(suggestions, comparator);
        return suggestions;
    }

    private Collection<ScoreTerm> suggestSimilar(Term term, int numSug, List<AtomicReaderContext> readerContexts,
                                                 int docfreq, int editDistance, float accuracy, final CharsRef spare) throws IOException {

        Map<Object, PriorityQueue<ScoreTerm>> segmentStQueue = new HashMap<Object, PriorityQueue<ScoreTerm>>();
        for (AtomicReaderContext readerContext : readerContexts) {
            DocIdSet docIdSet = null;
            if (filter != null) {
                docIdSet = filter.getDocIdSet(readerContext, null);
                if (DocIdSets.isEmpty(docIdSet)) {
                    continue;
                }
            }

            PriorityQueue<ScoreTerm> segmentResult;
            segmentStQueue.put(readerContext.reader().getCoreCacheKey(), segmentResult = new PriorityQueue<ScoreTerm>());
            AttributeSource atts = new AttributeSource();
            MaxNonCompetitiveBoostAttribute maxBoostAtt =
                    atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);

            Terms terms = readerContext.reader().terms(term.field());
            if (terms == null) {
                return Collections.emptyList();
            }
            FuzzyTermsEnum e = new FuzzyTermsEnum(terms, atts, term, editDistance, Math.max(minPrefix, editDistance - 1), true);

            BytesRef queryTerm = new BytesRef(term.text());
            BytesRef candidateTerm;
            ScoreTerm st = new ScoreTerm();
            BoostAttribute boostAtt = e.attributes().addAttribute(BoostAttribute.class);
            DocsEnum docsEnum = null;
            outer:
            while ((candidateTerm = e.next()) != null) {
                if (filter != null) {
                    DocIdSetIterator iterator = docIdSet.iterator();
                    docsEnum = e.docs(null, docsEnum);
                    int filterDocId = iterator.advance(0);
                    int termsDocId = docsEnum.advance(filterDocId);
                    while (filterDocId != termsDocId) {
                        if (filterDocId == DocIdSetIterator.NO_MORE_DOCS || termsDocId == DocIdSetIterator.NO_MORE_DOCS) {
                            continue outer; // Ignore hits that don't match with the specified filter.
                        }

                        if (filterDocId < termsDocId) {
                            filterDocId = iterator.advance(termsDocId);
                        } else {
                            termsDocId = docsEnum.advance(filterDocId);
                        }
                    }
                }

                final float boost = boostAtt.getBoost();
                // ignore uncompetitive hits
                if (segmentResult.size() >= numSug && boost <= segmentResult.peek().boost) {
                    continue;
                }

                // ignore exact match of the same term
                if (queryTerm.bytesEquals(candidateTerm)) {
                    continue;
                }

                final float score;
                final String termAsString;
                if (distance == INTERNAL_LEVENSHTEIN) {
                    // delay creating strings until the end
                    termAsString = null;
                    // undo FuzzyTermsEnum's scale factor for a real scaled lev score
                    score = boost / e.getScaleFactor() + e.getMinSimilarity();
                } else {
                    UnicodeUtil.UTF8toUTF16(candidateTerm, spare);
                    termAsString = spare.toString();
                    score = distance.getDistance(term.text(), termAsString);
                }

                if (score < accuracy) {
                    continue;
                }

                // add new entry in PQ
                st.term = BytesRef.deepCopyOf(candidateTerm);
                st.boost = boost;
                st.docfreq = e.docFreq();
                st.termAsString = termAsString;
                st.score = score;
                segmentResult.offer(st);
                // possibly drop entries from queue
                st = (segmentResult.size() > numSug) ? segmentResult.poll() : new ScoreTerm();
                maxBoostAtt.setMaxNonCompetitiveBoost((segmentResult.size() >= numSug) ? segmentResult.peek().boost : Float.NEGATIVE_INFINITY);
            }
        }

        if (segmentStQueue.isEmpty()) {
            return Collections.emptyList();
        }

        NavigableMap<BytesRef, ScoreTerm> stQueue = null;
        for (Map.Entry<Object, PriorityQueue<ScoreTerm>> entry : segmentStQueue.entrySet()) {
            if (stQueue == null) {
                stQueue = new TreeMap<BytesRef, ScoreTerm>();
                for (ScoreTerm scoreTerm : entry.getValue()) {
                    stQueue.put(scoreTerm.term, scoreTerm);
                }
            } else {
                for (ScoreTerm scoreTerm : entry.getValue()) {
                    ScoreTerm existing = stQueue.get(scoreTerm.term);
                    if (existing != null) {
                        existing.docfreq += scoreTerm.docfreq;
                    } else {
                        stQueue.put(scoreTerm.term, scoreTerm);
                    }
                }
            }
        }

        List<ScoreTerm> endResult = new ArrayList<ScoreTerm>(numSug);
        int addedSuggestions = 0;
        for (ScoreTerm scoreTerm : stQueue.values()) {
            if (addedSuggestions == numSug) {
                return endResult;
            }

            if (scoreTerm.docfreq < docfreq) {
                // TODO: Hits that have docFreq lower than required docFreq are just removed now. If because of removal the
                // number of hits is lower than numSug we should re-execute...
                continue;
            }
            endResult.add(scoreTerm);
            addedSuggestions++;
        }

        return endResult;
    }

    private static class ScoreTerm implements Comparable<ScoreTerm> {
        public BytesRef term;
        public float boost;
        public int docfreq;

        public String termAsString;
        public float score;

        public int compareTo(ScoreTerm other) {
            if (term.bytesEquals(other.term)) {
                return 0; // consistent with equals
            } else if (this.boost == other.boost) {
                return other.term.compareTo(this.term);
            } else {
                return Float.compare(this.boost, other.boost);
            }
        }

        @Override
        public int hashCode() {
            return term.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            ScoreTerm other = (ScoreTerm) obj;
            if (term == null) {
                if (other.term != null) {
                    return false;
                }
            } else if (!term.bytesEquals(other.term)) {
                return false;
            }
            return true;
        }
    }

    public static Comparator<SuggestedWord> scoreFirst = new ScoreFirst();
    public static Comparator<SuggestedWord> frequenctFirst = new FrequencyFirst();

    public static class ScoreFirst implements Comparator<SuggestedWord> {

        @Override
        public int compare(SuggestedWord first, SuggestedWord second) {
            // first criteria: the distance
            int cmp = Float.compare(first.score(), second.score());
            if (cmp != 0) {
                return cmp;
            }

            // second criteria (if first criteria is equal): the popularity
            cmp = first.frequency() - second.frequency();
            if (cmp != 0) {
                return cmp;
            }
            // third criteria: term text
            return second.suggestion().compareTo(first.suggestion());
        }

    }

    public static class FrequencyFirst implements Comparator<SuggestedWord> {
        @Override
        public int compare(SuggestedWord first, SuggestedWord second) {
            // first criteria: the popularity
            int cmp = first.frequency() - second.frequency();
            if (cmp != 0) {
                return cmp;
            }

            // second criteria (if first criteria is equal): the distance
            cmp = Float.compare(first.score(), second.score());
            if (cmp != 0) {
                return cmp;
            }

            // third criteria: term text
            return second.suggestion().compareTo(first.suggestion());
        }
    }

}
