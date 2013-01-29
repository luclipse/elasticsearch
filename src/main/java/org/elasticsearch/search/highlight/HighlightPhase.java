/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.highlight;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.postingshighlight.CustomPassageFormatter;
import org.apache.lucene.search.postingshighlight.CustomPostingsHighlighter;
import org.apache.lucene.search.postingshighlight.NoneBreakIterator;
import org.apache.lucene.search.postingshighlight.PassageScorer;
import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.search.vectorhighlight.SimpleBoundaryScanner2;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.fieldvisitor.CustomFieldsVisitor;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

/**
 *
 */
public class HighlightPhase extends AbstractComponent implements FetchSubPhase {

    public static class Encoders {
        public static Encoder DEFAULT = new DefaultEncoder();
        public static Encoder HTML = new SimpleHTMLEncoder();
    }

    private final boolean termVectorMultiValue;

    @Inject
    public HighlightPhase(Settings settings) {
        super(settings);
        this.termVectorMultiValue = componentSettings.getAsBoolean("term_vector_multi_value", true);
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        // we use a cache to cache heavy things, mainly the rewrite in FieldQuery for FVH or PH
        HighlighterEntry cache = (HighlighterEntry) hitContext.cache().get("highlight");
        if (cache == null) {
            cache = new HighlighterEntry();
            hitContext.cache().put("highlight", cache);
        }

        DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());

        Map<String, HighlightField> highlightFields = newHashMap();
        for (SearchContextHighlight.Field field : context.highlight().fields()) {
            Encoder encoder;
            if (field.encoder().equals("html")) {
                encoder = Encoders.HTML;
            } else {
                encoder = Encoders.DEFAULT;
            }

            Set<String> fieldNamesToHighlight;
            if (Regex.isSimpleMatchPattern(field.field())) {
                fieldNamesToHighlight = documentMapper.mappers().simpleMatchToFullName(field.field());
            } else {
                fieldNamesToHighlight = ImmutableSet.of(field.field());
            }

            for (String fieldName : fieldNamesToHighlight) {

                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(fieldName);
                if (mapper == null) {
                    MapperService.SmartNameFieldMappers fullMapper = context.mapperService().smartName(fieldName);
                    if (fullMapper == null || !fullMapper.hasDocMapper()) {
                        //Save skipping missing fields
                        continue;
                    }
                    if (!fullMapper.docMapper().type().equals(hitContext.hit().type())) {
                        continue;
                    }
                    mapper = fullMapper.mapper();
                    if (mapper == null) {
                        continue;
                    }
                }
                boolean useFastVectorHighlighter;
                boolean usePostingsHighlighter;
                if (field.highlighterType() == null) {
                    // if we can do highlighting using Term Vectors, use FastVectorHighlighter,
                    // if we can use highlighting using index_options=offsets, use PostingsHighlighter
                    // otherwise, use the slower plain highlighter
                    useFastVectorHighlighter = mapper.fieldType().storeTermVectors() && mapper.fieldType().storeTermVectorOffsets() && mapper.fieldType().storeTermVectorPositions();
                    if (!useFastVectorHighlighter) {
                        usePostingsHighlighter = mapper.fieldType().indexOptions() == FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
                    } else {
                        usePostingsHighlighter = false;
                    }
                } else if (field.highlighterType().equals("fast-vector-highlighter") || field.highlighterType().equals("fvh")) {
                    if (!(mapper.fieldType().storeTermVectors() && mapper.fieldType().storeTermVectorOffsets() && mapper.fieldType().storeTermVectorPositions())) {
                        throw new ElasticSearchIllegalArgumentException("the field [" + fieldName + "] should be indexed with term vector with position offsets to be used with fast vector highlighter");
                    }
                    useFastVectorHighlighter = true;
                    usePostingsHighlighter = false;
                } else if (field.highlighterType().equals("highlighter") || field.highlighterType().equals("plain")) {
                    useFastVectorHighlighter = false;
                    usePostingsHighlighter = false;
                } else if (field.highlighterType().equals("postings-highlighter") || field.highlighterType().equals("ph")) {
                    useFastVectorHighlighter = false;
                    usePostingsHighlighter = true;
                } else {
                    throw new ElasticSearchIllegalArgumentException("unknown highlighter type [" + field.highlighterType() + "] for the field [" + fieldName + "]");
                }

                if (usePostingsHighlighter) {
                    MapperHighlightEntry entry = cache.mappers.get(mapper);
                    if (entry == null) {
                        entry = new MapperHighlightEntry();
                        cache.mappers.put(mapper, entry);
                    }

                    if (field.numberOfFragments() == 0) {
                        field.numberOfFragments(Integer.MAX_VALUE);
                        field.breakIterator(new NoneBreakIterator());
                    }

                    String hlField = mapper.names().indexName();
                    Query unwrittenQuery = context.parsedQuery().query();
                    IndexSearcher searcher = context.searcher();
                    int numberOfFragments = field.numberOfFragments();

                    CustomPassageFormatter formatter;
                    if (entry.ph == null) {
                        int maxLength = field.maxLength();
                        String preTag = field.preTags()[0];
                        String postTag = field.postTags()[0];
                        BreakIterator breakIterator = field.breakIterator();

                        PassageScorer scorer = new PassageScorer();
                        formatter = new CustomPassageFormatter(preTag, postTag);
                        entry.ph = new CustomPostingsHighlighter(maxLength, breakIterator, scorer, formatter, mapper, context, field.requireFieldMatch());
                    } else {
                        formatter = entry.ph.getFormatter();
                        formatter.clear();
                    }

                    try {
                        entry.ph.setHitContext(hitContext);
                        entry.ph.highlight(hlField, unwrittenQuery, searcher, CustomPostingsHighlighter.topDocs(hitContext.docId()), numberOfFragments);
                        String[] snippets = formatter.getPassages();
                        if (snippets != null) {
                            HighlightField highlightField = new HighlightField(fieldName, StringText.convertFromStringArray(snippets));
                            highlightFields.put(highlightField.name(), highlightField);
                        }
                    } catch (IOException e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + fieldName + "]", e);
                    }
                } else if (!useFastVectorHighlighter) {
                    MapperHighlightEntry entry = cache.mappers.get(mapper);
                    if (entry == null) {
                        Query query = context.parsedQuery().query();
                        QueryScorer queryScorer = new CustomQueryScorer(query, field.requireFieldMatch() ? mapper.names().indexName() : null);
                        queryScorer.setExpandMultiTermQuery(true);
                        Fragmenter fragmenter;
                        if (field.numberOfFragments() == 0) {
                            fragmenter = new NullFragmenter();
                        } else if (field.fragmenter() == null) {
                            fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
                        } else if ("simple".equals(field.fragmenter())) {
                            fragmenter = new SimpleFragmenter(field.fragmentCharSize());
                        } else if ("span".equals(field.fragmenter())) {
                            fragmenter = new SimpleSpanFragmenter(queryScorer, field.fragmentCharSize());
                        } else {
                            throw new ElasticSearchIllegalArgumentException("unknown fragmenter option [" + field.fragmenter() + "] for the field [" + fieldName + "]");
                        }
                        Formatter formatter = new SimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);


                        entry = new MapperHighlightEntry();
                        entry.highlighter = new Highlighter(formatter, encoder, queryScorer);
                        entry.highlighter.setTextFragmenter(fragmenter);
                        // always highlight across all data
                        entry.highlighter.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

                        cache.mappers.put(mapper, entry);
                    }

                    List<Object> textsToHighlight;
                    if (mapper.fieldType().stored()) {
                        try {
                            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(ImmutableSet.of(mapper.names().indexName()), false);
                            hitContext.reader().document(hitContext.docId(), fieldVisitor);
                            textsToHighlight = fieldVisitor.fields().get(mapper.names().indexName());
                        } catch (Exception e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + fieldName + "]", e);
                        }
                    } else {
                        SearchLookup lookup = context.lookup();
                        lookup.setNextReader(hitContext.readerContext());
                        lookup.setNextDocId(hitContext.docId());
                        textsToHighlight = lookup.source().extractRawValues(mapper.names().sourcePath());
                    }

                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
                    try {
                        for (Object textToHighlight : textsToHighlight) {
                            String text = textToHighlight.toString();
                            Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                            TokenStream tokenStream = analyzer.tokenStream(mapper.names().indexName(), new FastStringReader(text));
                            tokenStream.reset();
                            TextFragment[] bestTextFragments = entry.highlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                            for (TextFragment bestTextFragment : bestTextFragments) {
                                if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                                    fragsList.add(bestTextFragment);
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + fieldName + "]", e);
                    }
                    if (field.scoreOrdered()) {
                        Collections.sort(fragsList, new Comparator<TextFragment>() {
                            public int compare(TextFragment o1, TextFragment o2) {
                                return Math.round(o2.getScore() - o1.getScore());
                            }
                        });
                    }
                    String[] fragments = null;
                    // number_of_fragments is set to 0 but we have a multivalued field
                    if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
                        fragments = new String[fragsList.size()];
                        for (int i = 0; i < fragsList.size(); i++) {
                            fragments[i] = fragsList.get(i).toString();
                        }
                    } else {
                        // refine numberOfFragments if needed
                        numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size() : numberOfFragments;
                        fragments = new String[numberOfFragments];
                        for (int i = 0; i < fragments.length; i++) {
                            fragments[i] = fragsList.get(i).toString();
                        }
                    }

                    if (fragments != null && fragments.length > 0) {
                        HighlightField highlightField = new HighlightField(fieldName, StringText.convertFromStringArray(fragments));
                        highlightFields.put(highlightField.name(), highlightField);
                    }
                } else {
                    try {
                        MapperHighlightEntry entry = cache.mappers.get(mapper);
                        FieldQuery fieldQuery = null;
                        if (entry == null) {
                            FragListBuilder fragListBuilder;
                            BaseFragmentsBuilder fragmentsBuilder;

                            BoundaryScanner boundaryScanner = SimpleBoundaryScanner2.DEFAULT;
                            if (field.boundaryMaxScan() != SimpleBoundaryScanner2.DEFAULT_MAX_SCAN || field.boundaryChars() != SimpleBoundaryScanner2.DEFAULT_BOUNDARY_CHARS) {
                                boundaryScanner = new SimpleBoundaryScanner2(field.boundaryMaxScan(), field.boundaryChars());
                            }

                            if (field.numberOfFragments() == 0) {
                                fragListBuilder = new SingleFragListBuilder();

                                if (mapper.fieldType().stored()) {
                                    fragmentsBuilder = new XSimpleFragmentsBuilder(field.preTags(), field.postTags(), boundaryScanner);
                                } else {
                                    fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, context, field.preTags(), field.postTags(), boundaryScanner);
                                }
                            } else {
                                if (field.fragmentOffset() == -1)
                                    fragListBuilder = new SimpleFragListBuilder();
                                else
                                    fragListBuilder = new SimpleFragListBuilder(field.fragmentOffset());

                                if (field.scoreOrdered()) {
                                    if (mapper.fieldType().stored()) {
                                        fragmentsBuilder = new XScoreOrderFragmentsBuilder(field.preTags(), field.postTags(), boundaryScanner);
                                    } else {
                                        fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(mapper, context, field.preTags(), field.postTags(), boundaryScanner);
                                    }
                                } else {
                                    if (mapper.fieldType().stored()) {
                                        fragmentsBuilder = new XSimpleFragmentsBuilder(field.preTags(), field.postTags(), boundaryScanner);
                                    } else {
                                        fragmentsBuilder = new SourceSimpleFragmentsBuilder(mapper, context, field.preTags(), field.postTags(), boundaryScanner);
                                    }
                                }
                            }
                            fragmentsBuilder.setDiscreteMultiValueHighlighting(termVectorMultiValue);
                            entry = new MapperHighlightEntry();
                            entry.fragListBuilder = fragListBuilder;
                            entry.fragmentsBuilder = fragmentsBuilder;
                            if (cache.fvh == null) {
                                // parameters to FVH are not requires since:
                                // first two booleans are not relevant since they are set on the CustomFieldQuery (phrase and fieldMatch)
                                // fragment builders are used explicitly
                                cache.fvh = new FastVectorHighlighter();
                            }
                            CustomFieldQuery.highlightFilters.set(field.highlightFilter());
                            if (field.requireFieldMatch()) {
                                if (cache.fieldMatchFieldQuery == null) {
                                    // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                                    cache.fieldMatchFieldQuery = new CustomFieldQuery(context.parsedQuery().query(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                                }
                                fieldQuery = cache.fieldMatchFieldQuery;
                            } else {
                                if (cache.noFieldMatchFieldQuery == null) {
                                    // we use top level reader to rewrite the query against all readers, with use caching it across hits (and across readers...)
                                    cache.noFieldMatchFieldQuery = new CustomFieldQuery(context.parsedQuery().query(), hitContext.topLevelReader(), true, field.requireFieldMatch());
                                }
                                fieldQuery = cache.noFieldMatchFieldQuery;
                            }
                            cache.mappers.put(mapper, entry);
                        }

                        String[] fragments;

                        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                        int numberOfFragments = field.numberOfFragments() == 0 ? Integer.MAX_VALUE : field.numberOfFragments();
                        int fragmentCharSize = field.numberOfFragments() == 0 ? Integer.MAX_VALUE : field.fragmentCharSize();
                        // we highlight against the low level reader and docId, because if we load source, we want to reuse it if possible
                        fragments = cache.fvh.getBestFragments(fieldQuery, hitContext.reader(), hitContext.docId(), mapper.names().indexName(), fragmentCharSize, numberOfFragments,
                                entry.fragListBuilder, entry.fragmentsBuilder, field.preTags(), field.postTags(), encoder);

                        if (fragments != null && fragments.length > 0) {
                            HighlightField highlightField = new HighlightField(fieldName, StringText.convertFromStringArray(fragments));
                            highlightFields.put(highlightField.name(), highlightField);
                        }
                    } catch (Exception e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + fieldName + "]", e);
                    }
                }
            }
        }

        hitContext.hit().highlightFields(highlightFields);
    }

    static class MapperHighlightEntry {
        public FragListBuilder fragListBuilder;
        public FragmentsBuilder fragmentsBuilder;

        public Highlighter highlighter;
        public CustomPostingsHighlighter ph;
    }

    static class HighlighterEntry {
        public FastVectorHighlighter fvh;
        public FieldQuery noFieldMatchFieldQuery;
        public FieldQuery fieldMatchFieldQuery;
        public Map<FieldMapper, MapperHighlightEntry> mappers = Maps.newHashMap();
    }
}
