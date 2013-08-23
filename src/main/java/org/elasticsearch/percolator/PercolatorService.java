/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.percolator;

import com.google.common.collect.ImmutableMap;
import gnu.trove.map.hash.TByteObjectHashMap;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.*;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.common.xcontent.XContentFactory.smileBuilder;
import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.QueryCollector.*;

/**
 */
public class PercolatorService extends AbstractComponent {

    public final static float NO_SCORE = Float.NEGATIVE_INFINITY;

    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndicesService indicesService;
    private final TByteObjectHashMap<PercolatorType> percolatorTypes;

    private final HighlightPhase highlightPhase;

    @Inject
    public PercolatorService(Settings settings, IndicesService indicesService, HighlightPhase highlightPhase) {
        super(settings);
        this.indicesService = indicesService;
        this.highlightPhase = highlightPhase;

        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(false, maxReuseBytes);
            }
        };
        
        percolatorTypes = new TByteObjectHashMap<PercolatorType>(6);
        percolatorTypes.put(countPercolator.id(), countPercolator);
        percolatorTypes.put(queryCountPercolator.id(), queryCountPercolator);
        percolatorTypes.put(matchPercolator.id(), matchPercolator);
        percolatorTypes.put(queryPercolator.id(), queryPercolator);
        percolatorTypes.put(scoringPercolator.id(), scoringPercolator);
        percolatorTypes.put(topMatchingPercolator.id(), topMatchingPercolator);
    }


    public ReduceResult reduce(byte percolatorTypeId, List<PercolateShardResponse> shardResults) {
        PercolatorType percolatorType = percolatorTypes.get(percolatorTypeId);
        return percolatorType.reduce(shardResults);
    }

    public PercolateShardResponse percolate(PercolateShardRequest request) {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = percolateIndexService.shardSafe(request.shardId());

        ShardPercolateService shardPercolateService = indexShard.shardPercolateService();
        shardPercolateService.prePercolate();
        long startTime = System.nanoTime();

        try {
            final PercolateContext context = new PercolateContext(request.documentType());
            context.percolateQueries = indexShard.percolateRegistry().percolateQueries();
            context.indexShard = indexShard;
            context.percolateIndexService = percolateIndexService;
            ParsedDocument parsedDocument = parsePercolate(percolateIndexService, request, context);
            if (context.percolateQueries.isEmpty()) {
                return new PercolateShardResponse(context, request.index(), request.shardId());
            }

            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(request.docSource(), percolateIndexService, request.documentType());
            } else if (parsedDocument == null) {
                throw new ElasticSearchIllegalArgumentException("Nothing to percolate");
            }

            if (context.query == null && (context.score || context.sort)) {
                throw new ElasticSearchIllegalArgumentException("Can't sort or score if query isn't specified");
            }

            if (context.sort && !context.limit) {
                throw new ElasticSearchIllegalArgumentException("Can't sort if size isn't specified");
            }

            if (context.size < 0) {
                context.size = 0;
            }

            // first, parse the source doc into a MemoryIndex
            final MemoryIndex memoryIndex = cache.get();
            try {
                // TODO: This means percolation does not support nested docs...
                // So look into: ByteBufferDirectory
                for (IndexableField field : parsedDocument.rootDoc().getFields()) {
                    if (!field.fieldType().indexed()) {
                        continue;
                    }
                    // no need to index the UID field
                    if (field.name().equals(UidFieldMapper.NAME)) {
                        continue;
                    }
                    TokenStream tokenStream;
                    try {
                        tokenStream = field.tokenStream(parsedDocument.analyzer());
                        if (tokenStream != null) {
                            memoryIndex.addField(field.name(), tokenStream, field.boost());
                        }
                    } catch (IOException e) {
                        throw new ElasticSearchException("Failed to create token stream", e);
                    }
                }

                PercolatorType action;
                if (request.onlyCount()) {
                    action = context.query != null ? queryCountPercolator : countPercolator;
                } else {
                    if (context.sort) {
                        action = topMatchingPercolator;
                    } else if (context.query != null) {
                        action = context.score ? scoringPercolator : queryPercolator;
                    } else {
                        action = matchPercolator;
                    }
                }
                context.percolatorTypeId = action.id();

                context.docSearcher = memoryIndex.createSearcher();
                context.fieldDataService = percolateIndexService.fieldData();

                if (context.highlight() != null) {
                    final IndexReader topLevelReader = context.docSearcher.getIndexReader();
                    AtomicReaderContext readerContext = topLevelReader.leaves().get(0);
                    Engine.Searcher searcher = new Engine.Searcher() {
                        @Override
                        public IndexReader reader() {
                            return topLevelReader;
                        }

                        @Override
                        public IndexSearcher searcher() {
                            return context.docSearcher;
                        }

                        @Override
                        public boolean release() throws ElasticSearchException {
                            return true;
                        }
                    };

                    context.hitContext = new FetchSubPhase.HitContext();
                    context.lookup().setNextReader(readerContext);
                    context.lookup().setNextDocId(0);
                    context.lookup().source().setNextSource(parsedDocument.source());

                    Map<String, SearchHitField> fields = new HashMap<String, SearchHitField>();
                    for (IndexableField field : parsedDocument.rootDoc().getFields()) {
                        List<Object> values = context.lookup().source().extractRawValues(field.name());
                        fields.put(field.name(), new InternalSearchHitField(field.name(), values));
                    }
                    context.hitContext.reset(new InternalSearchHit(0, "unknown", new StringText(request.documentType()), fields), readerContext, 0, topLevelReader, 0, new JustSourceFieldsVisitor());
                }

                IndexCache indexCache = percolateIndexService.cache();
                try {
                    return action.doPercolate(request, context);
                } finally {
                    // explicitly clear the reader, since we can only register on callback on SegmentReader
                    indexCache.clear(context.docSearcher.getIndexReader());
                    context.fieldDataService.clear(context.docSearcher.getIndexReader());
                }
            } finally {
                memoryIndex.reset();
            }
        } finally {
            shardPercolateService.postPercolate(System.nanoTime() - startTime);
        }
    }

    private ParsedDocument parsePercolate(IndexService documentIndexService, PercolateShardRequest request, PercolateContext context) throws ElasticSearchException {
        BytesReference source = request.source();
        if (source == null || source.length() == 0) {
            return null;
        }

        Map<String, ? extends SearchParseElement> hlElements = highlightPhase.parseElements();

        ParsedDocument doc = null;
        XContentParser parser = null;

        // Some queries (function_score query when for decay functions) rely on SearchContext being set:
        SearchContext searchContext = new DefaultSearchContext(0,
                new ShardSearchRequest().types(new String[0]),
                null, context.indexShard.searcher(), context.percolateIndexService, context.indexShard,
                null, null);
        SearchContext.setCurrent(searchContext);
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        if (doc != null) {
                            throw new ElasticSearchParseException("Either specify doc or get, not both");
                        }

                        MapperService mapperService = documentIndexService.mapperService();
                        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        XContentBuilder builder = smileBuilder();
                        token = parser.nextToken();
                        assert token == XContentParser.Token.START_OBJECT;
                        builder.copyCurrentStructure(parser);
                        doc = docMapper.parse(source(builder.bytes()).type(request.documentType()).flyweight(true));
                    }

                    SearchParseElement element = hlElements.get(currentFieldName);
                    if (element != null) {
                        element.parse(parser, context);
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        if (context.query != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        context.query = documentIndexService.queryParserService().parse(parser).query();
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.query != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        Filter filter = documentIndexService.queryParserService().parseInnerFilter(parser).filter();
                        context.query = new XConstantScoreQuery(filter);
                    }
                } else if (token == null) {
                    break;
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.limit = true;
                        context.size = parser.intValue();
                        if (context.size < 0) {
                            throw new ElasticSearchParseException("size is set to [" + context.size + "] and is expected to be higher or equal to 0");
                        }
                    } else if ("sort".equals(currentFieldName)) {
                        context.sort = parser.booleanValue();
                    } else if ("score".equals(currentFieldName)) {
                        context.score = parser.booleanValue();
                    }
                }
            }
        } catch (Exception e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            searchContext.release();
            SearchContext.removeCurrent();
            if (parser != null) {
                parser.close();
            }
        }

        return doc;
    }

    private ParsedDocument parseFetchedDoc(BytesReference fetchedDoc, IndexService documentIndexService, String type) {
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(fetchedDoc).createParser(fetchedDoc);
            MapperService mapperService = documentIndexService.mapperService();
            DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type);
            doc = docMapper.parse(source(parser).type(type).flyweight(true));
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new ElasticSearchParseException("No doc to percolate in the request");
        }

        return doc;
    }

    public void close() {
        cache.close();
    }

    interface PercolatorType {

        // 0x00 is reserved for empty type.
        byte id();

        ReduceResult reduce(List<PercolateShardResponse> shardResults);

        PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context);

    }

    private final PercolatorType countPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x01;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            long finalCount = 0;
            for (PercolateShardResponse shardResponse : shardResults) {
                finalCount += shardResponse.count();
            }
            return new ReduceResult(finalCount);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries.entrySet()) {
                collector.reset();
                try {
                    context.docSearcher.search(entry.getValue(), collector);
                } catch (IOException e) {
                    logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                }

                if (collector.exists()) {
                    count++;
                }
            }
            return new PercolateShardResponse(count, context, request.index(), request.shardId());
        }

    };

    private final PercolatorType queryCountPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x02;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return countPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                Count countCollector = count(logger, context);
                queryBasedPercolating(percolatorSearcher, context, countCollector);
                count = countCollector.counter();
            } catch (IOException e) {
                logger.warn("failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
            return new PercolateShardResponse(count, context, request.index(), request.shardId());
        }

    };

    private final PercolatorType matchPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x03;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            long foundMatches = 0;
            int numMatches = 0;
            for (PercolateShardResponse response : shardResults) {
                foundMatches += response.count();
                numMatches += response.matches().length;
            }
            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize == 0 ? numMatches : requestedSize);
            outer: for (PercolateShardResponse response : shardResults) {
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? NO_SCORE : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    Map<String, HighlightField> hl = response.hls().isEmpty() ? null : response.hls().get(i);
                    finalMatches.add(new PercolateResponse.Match(index, match, score, hl));
                    if (requestedSize != 0 && finalMatches.size() == requestedSize) {
                        break outer;
                    }
                }
            }
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            long count = 0;
            List<BytesRef> matches = new ArrayList<BytesRef>();
            List<Map<String, HighlightField>> hls = new ArrayList<Map<String, HighlightField>>();
            Lucene.ExistsCollector collector = new Lucene.ExistsCollector();

            for (Map.Entry<HashedBytesRef, Query> entry : context.percolateQueries.entrySet()) {
                collector.reset();
                if (context.highlight() != null) {
                    context.parsedQuery(new ParsedQuery(entry.getValue(), ImmutableMap.<String, Filter>of()));
                }
                try {
                    context.docSearcher.search(entry.getValue(), collector);
                } catch (IOException e) {
                    logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                }

                if (collector.exists()) {
                    if (!context.limit || count < context.size) {
                        matches.add(entry.getKey().bytes);
                        if (context.highlight() != null) {
                            highlightPhase.hitExecute(context, context.hitContext);
                            hls.add(context.hitContext.hit().getHighlightFields());
                        }
                    }
                    count++;
                }
            }

            BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
            return new PercolateShardResponse(finalMatches, hls, count, context, request.index(), request.shardId());
        }
    };

    private final PercolatorType queryPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x04;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return matchPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                Match match = match(logger, context);
                queryBasedPercolating(percolatorSearcher, context, match);
                List<BytesRef> matches = match.matches();
                long count = match.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, count, context, request.index(), request.shardId());
            } catch (IOException e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }
    };

    private final PercolatorType scoringPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x05;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            return matchPercolator.reduce(shardResults);
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                MatchAndScore matchAndScore = matchAndScore(logger, context);
                queryBasedPercolating(percolatorSearcher, context, matchAndScore);
                List<BytesRef> matches = matchAndScore.matches();
                float[] scores = matchAndScore.scores().toArray();
                long count = matchAndScore.counter();

                BytesRef[] finalMatches = matches.toArray(new BytesRef[matches.size()]);
                return new PercolateShardResponse(finalMatches, count, scores, context, request.index(), request.shardId());
            } catch (IOException e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }
    };

    private final PercolatorType topMatchingPercolator = new PercolatorType() {

        @Override
        public byte id() {
            return 0x06;
        }

        @Override
        public ReduceResult reduce(List<PercolateShardResponse> shardResults) {
            long foundMatches = 0;
            int nonEmptyResponses = 0;
            int firstNonEmptyIndex = 0;
            for (int i = 0; i < shardResults.size(); i++) {
                PercolateShardResponse response = shardResults.get(i);
                foundMatches += response.count();
                if (response.matches().length != 0) {
                    if (firstNonEmptyIndex == 0) {
                        firstNonEmptyIndex = i;
                    }
                    nonEmptyResponses++;
                }
            }

            int requestedSize = shardResults.get(0).requestedSize();

            // Use a custom impl of AbstractBigArray for Object[]?
            List<PercolateResponse.Match> finalMatches = new ArrayList<PercolateResponse.Match>(requestedSize);
            if (nonEmptyResponses == 1) {
                PercolateShardResponse response = shardResults.get(firstNonEmptyIndex);
                Text index = new StringText(response.getIndex());
                for (int i = 0; i < response.matches().length; i++) {
                    float score = response.scores().length == 0 ? Float.NaN : response.scores()[i];
                    Text match = new BytesText(new BytesArray(response.matches()[i]));
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                }
            } else {
                int[] slots = new int[shardResults.size()];
                while (true) {
                    float lowestScore = Float.NEGATIVE_INFINITY;
                    int requestIndex = -1;
                    int itemIndex = -1;
                    for (int i = 0; i < shardResults.size(); i++) {
                        int scoreIndex = slots[i];
                        float[] scores = shardResults.get(i).scores();
                        if (scoreIndex >= scores.length) {
                            continue;
                        }

                        float score = scores[scoreIndex];
                        int cmp = Float.compare(lowestScore, score);
                        // TODO: Maybe add a tie?
                        if (cmp < 0) {
                            requestIndex = i;
                            itemIndex = scoreIndex;
                            lowestScore = score;
                        }
                    }

                    // This means the shard matches have been exhausted and we should bail
                    if (requestIndex == -1) {
                        break;
                    }

                    slots[requestIndex]++;

                    PercolateShardResponse shardResponse = shardResults.get(requestIndex);
                    Text index = new StringText(shardResponse.getIndex());
                    Text match = new BytesText(new BytesArray(shardResponse.matches()[itemIndex]));
                    float score = shardResponse.scores()[itemIndex];
                    finalMatches.add(new PercolateResponse.Match(index, match, score));
                    if (finalMatches.size() == requestedSize) {
                        break;
                    }
                }
            }
            return new ReduceResult(foundMatches, finalMatches.toArray(new PercolateResponse.Match[finalMatches.size()]));
        }

        @Override
        public PercolateShardResponse doPercolate(PercolateShardRequest request, PercolateContext context) {
            Engine.Searcher percolatorSearcher = context.indexShard.searcher();
            try {
                MatchAndSort matchAndSort = QueryCollector.matchAndSort(logger, context);
                queryBasedPercolating(percolatorSearcher, context, matchAndSort);
                TopDocs topDocs = matchAndSort.topDocs();
                long count = topDocs.totalHits;
                List<BytesRef> matches = new ArrayList<BytesRef>(topDocs.scoreDocs.length);
                float[] scores = new float[topDocs.scoreDocs.length];

                IndexFieldData idFieldData = context.fieldDataService.getForField(
                        new FieldMapper.Names(IdFieldMapper.NAME),
                        new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes"))
                );
                int i = 0;
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    int segmentIdx = ReaderUtil.subIndex(scoreDoc.doc, percolatorSearcher.reader().leaves());
                    AtomicReaderContext atomicReaderContext = percolatorSearcher.reader().leaves().get(segmentIdx);
                    BytesValues values = idFieldData.load(atomicReaderContext).getBytesValues();
                    BytesRef id = values.getValue(scoreDoc.doc - atomicReaderContext.docBase);
                    matches.add(values.makeSafe(id));
                    scores[i++] = scoreDoc.score;
                }
                return new PercolateShardResponse(matches.toArray(new BytesRef[matches.size()]), count, scores, context, request.index(), request.shardId());
            } catch (Exception e) {
                logger.debug("failed to execute", e);
                throw new PercolateException(context.indexShard.shardId(), "failed to execute", e);
            } finally {
                percolatorSearcher.release();
            }
        }

    };

    private static void queryBasedPercolating(Engine.Searcher percolatorSearcher, PercolateContext context, Collector collector) throws IOException {
        Filter percolatorTypeFilter = context.percolateIndexService.mapperService().documentMapper(Constants.TYPE_NAME).typeFilter();
        percolatorTypeFilter = context.percolateIndexService.cache().filter().cache(percolatorTypeFilter);
        FilteredQuery query = new FilteredQuery(context.query, percolatorTypeFilter);
        percolatorSearcher.searcher().search(query, collector);
    }

    public final static class ReduceResult {

        private final long count;
        private final PercolateResponse.Match[] matches;

        ReduceResult(long count, PercolateResponse.Match[] matches) {
            this.count = count;
            this.matches = matches;
        }

        public ReduceResult(long count) {
            this.count = count;
            this.matches = new PercolateResponse.Match[0];
        }

        public long count() {
            return count;
        }

        public PercolateResponse.Match[] matches() {
            return matches;
        }
    }

    public static final class Constants {

        public static final String TYPE_NAME = "_percolator";

    }

    public class PercolateContext extends SearchContext {

        public boolean limit;
        public int size;
        public boolean score;
        public boolean sort;
        public byte percolatorTypeId;

        Query query;
        ConcurrentMap<HashedBytesRef, Query> percolateQueries;
        IndexSearcher docSearcher;
        IndexShard indexShard;
        IndexFieldDataService fieldDataService;
        IndexService percolateIndexService;

        FetchSubPhase.HitContext hitContext;

        private final String documentType;

        public PercolateContext(String documentType) {
            this.documentType = documentType;
        }

        @Override
        public void preProcess() {
        }

        @Override
        public Filter searchFilter(String[] types) {
            return null;
        }

        @Override
        public long id() {
            return 0;
        }

        @Override
        public ShardSearchRequest request() {
            return null;
        }

        @Override
        public SearchType searchType() {
            return null;
        }

        @Override
        public SearchContext searchType(SearchType searchType) {
            return null;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return null;
        }

        @Override
        public int numberOfShards() {
            return 0;
        }

        @Override
        public boolean hasTypes() {
            return false;
        }

        @Override
        public String[] types() {
            return new String[0];
        }

        @Override
        public float queryBoost() {
            return 0;
        }

        @Override
        public SearchContext queryBoost(float queryBoost) {
            return null;
        }

        @Override
        public long nowInMillis() {
            return 0;
        }

        @Override
        public Scroll scroll() {
            return null;
        }

        @Override
        public SearchContext scroll(Scroll scroll) {
            return null;
        }

        @Override
        public SearchContextFacets facets() {
            return null;
        }

        @Override
        public SearchContext facets(SearchContextFacets facets) {
            return null;
        }

        private SearchContextHighlight highlight;

        @Override
        public SearchContextHighlight highlight() {
            return highlight;
        }

        @Override
        public void highlight(SearchContextHighlight highlight) {
            this.highlight = highlight;
        }

        @Override
        public SuggestionSearchContext suggest() {
            return null;
        }

        @Override
        public void suggest(SuggestionSearchContext suggest) {
        }

        @Override
        public RescoreSearchContext rescore() {
            return null;
        }

        @Override
        public void rescore(RescoreSearchContext rescore) {
        }

        @Override
        public boolean hasScriptFields() {
            return false;
        }

        @Override
        public ScriptFieldsContext scriptFields() {
            return null;
        }

        @Override
        public boolean hasPartialFields() {
            return false;
        }

        @Override
        public PartialFieldsContext partialFields() {
            return null;
        }

        @Override
        public boolean sourceRequested() {
            return false;
        }

        @Override
        public boolean hasFetchSourceContext() {
            return false;
        }

        @Override
        public FetchSourceContext fetchSourceContext() {
            return null;
        }

        @Override
        public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
            return null;
        }

        @Override
        public ContextIndexSearcher searcher() {
            return null;
        }

        @Override
        public IndexShard indexShard() {
            return null;
        }

        @Override
        public MapperService mapperService() {
            return percolateIndexService.mapperService();
        }

        @Override
        public AnalysisService analysisService() {
            return null;
        }

        @Override
        public IndexQueryParserService queryParserService() {
            return null;
        }

        @Override
        public SimilarityService similarityService() {
            return null;
        }

        @Override
        public ScriptService scriptService() {
            return null;
        }

        @Override
        public CacheRecycler cacheRecycler() {
            return null;
        }

        @Override
        public FilterCache filterCache() {
            return null;
        }

        @Override
        public DocSetCache docSetCache() {
            return null;
        }

        @Override
        public IndexFieldDataService fieldData() {
            return fieldDataService;
        }

        @Override
        public IdCache idCache() {
            return null;
        }

        @Override
        public long timeoutInMillis() {
            return 0;
        }

        @Override
        public void timeoutInMillis(long timeoutInMillis) {
        }

        @Override
        public SearchContext minimumScore(float minimumScore) {
            return null;
        }

        @Override
        public Float minimumScore() {
            return null;
        }

        @Override
        public SearchContext sort(Sort sort) {
            return null;
        }

        @Override
        public Sort sort() {
            return null;
        }

        @Override
        public SearchContext trackScores(boolean trackScores) {
            return null;
        }

        @Override
        public boolean trackScores() {
            return false;
        }

        @Override
        public SearchContext parsedFilter(ParsedFilter filter) {
            return null;
        }

        @Override
        public ParsedFilter parsedFilter() {
            return null;
        }

        @Override
        public Filter aliasFilter() {
            return null;
        }

        private ParsedQuery parsedQuery;

        @Override
        public SearchContext parsedQuery(ParsedQuery query) {
            this.parsedQuery = query;
            return this;
        }

        @Override
        public ParsedQuery parsedQuery() {
            return parsedQuery;
        }

        @Override
        public Query query() {
            return null;
        }

        @Override
        public boolean queryRewritten() {
            return false;
        }

        @Override
        public SearchContext updateRewriteQuery(Query rewriteQuery) {
            return null;
        }

        @Override
        public int from() {
            return 0;
        }

        @Override
        public SearchContext from(int from) {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public SearchContext size(int size) {
            return null;
        }

        @Override
        public boolean hasFieldNames() {
            return false;
        }

        @Override
        public List<String> fieldNames() {
            return null;
        }

        @Override
        public void emptyFieldNames() {
        }

        @Override
        public boolean explain() {
            return false;
        }

        @Override
        public void explain(boolean explain) {
        }

        @Override
        public List<String> groupStats() {
            return null;
        }

        @Override
        public void groupStats(List<String> groupStats) {
        }

        @Override
        public boolean version() {
            return false;
        }

        @Override
        public void version(boolean version) {
        }

        @Override
        public int[] docIdsToLoad() {
            return new int[0];
        }

        @Override
        public int docIdsToLoadFrom() {
            return 0;
        }

        @Override
        public int docIdsToLoadSize() {
            return 0;
        }

        @Override
        public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
            return null;
        }

        @Override
        public void accessed(long accessTime) {
        }

        @Override
        public long lastAccessTime() {
            return 0;
        }

        @Override
        public long keepAlive() {
            return 0;
        }

        @Override
        public void keepAlive(long keepAlive) {
        }

        private SearchLookup searchLookup;

        @Override
        public SearchLookup lookup() {
            if (searchLookup == null) {
                searchLookup = new SearchLookup(mapperService(), fieldData(), new String[] {documentType});
            }
            return searchLookup;
        }

        @Override
        public DfsSearchResult dfsResult() {
            return null;
        }

        @Override
        public QuerySearchResult queryResult() {
            return null;
        }

        @Override
        public FetchSearchResult fetchResult() {
            return null;
        }

        @Override
        public void addRewrite(Rewrite rewrite) {
        }

        @Override
        public List<Rewrite> rewrites() {
            return null;
        }

        @Override
        public ScanContext scanContext() {
            return null;
        }

        @Override
        public MapperService.SmartNameFieldMappers smartFieldMappers(String name) {
            return null;
        }

        @Override
        public FieldMappers smartNameFieldMappers(String name) {
            return null;
        }

        @Override
        public FieldMapper smartNameFieldMapper(String name) {
            return null;
        }

        @Override
        public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
            return null;
        }

        @Override
        public boolean release() throws ElasticSearchException {
            return true;
        }
    }

}
