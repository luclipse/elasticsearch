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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.ExtendedMemoryIndex;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.action.percolate.PercolateShardResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 */
public class PercolatorService extends AbstractComponent {

    private final CloseableThreadLocal<MemoryIndex> cache;
    private final IndicesService indicesService;

    @Inject
    public PercolatorService(Settings settings, IndicesService indicesService) {
        super(settings);
        this.indicesService = indicesService;
        final long maxReuseBytes = settings.getAsBytesSize("indices.memory.memory_index.size_per_thread", new ByteSizeValue(1, ByteSizeUnit.MB)).bytes();
        cache = new CloseableThreadLocal<MemoryIndex>() {
            @Override
            protected MemoryIndex initialValue() {
                return new ExtendedMemoryIndex(false, maxReuseBytes);
            }
        };
    }

    public PercolateShardResponse matchPercolate(final PercolateShardRequest request) {
        return preparePercolate(request, new PercolateAction() {
            @Override
            public PercolateShardResponse doPercolateAction(PercolateContext context) {
                Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                QueryIterator queryIterator = context.queryIterator;
                List<Text> matches = new ArrayList<Text>();
                long count = 0;
                while (queryIterator.next()) {
                    collector.reset();
                    Text queryId = queryIterator.queryId();
                    try {
                        context.docSearcher.search(queryIterator.query(), collector);
                    } catch (IOException e) {
                        logger.warn("[" + queryId + "] failed to execute query", e);
                    }
                    if (collector.exists()) {
                        if (context.shortCircuit && (count == context.size)) {
                            break;
                        } if (context.limit && (count >= context.size)) {
                            count++;
                        } else {
                            matches.add(queryId);
                            count = matches.size();
                        }
                    }
                }
                return new PercolateShardResponse(matches.toArray(new Text[matches.size()]), count, context, request.index(), request.shardId());
            }
        });
    }

    public PercolateShardResponse countPercolate(final PercolateShardRequest request) {
        return preparePercolate(request, new PercolateAction() {
            @Override
            public PercolateShardResponse doPercolateAction(PercolateContext context) {
                Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                QueryIterator queryIterator = context.queryIterator;
                long count = 0;
                while (queryIterator.next()) {
                    collector.reset();
                    Text queryId = queryIterator.queryId();
                    try {
                        context.docSearcher.search(queryIterator.query(), collector);
                    } catch (IOException e) {
                        logger.warn("[" + queryId + "] failed to execute query", e);
                    }
                    if (collector.exists()) {
                        count++;
                        if (context.limit && count == context.size) {
                            break;
                        }
                    }
                }
                return new PercolateShardResponse(count, context, request.index(), request.shardId());
            }
        });
    }

    private PercolateShardResponse preparePercolate(PercolateShardRequest request, PercolateAction action) {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = percolateIndexService.shardSafe(request.shardId());

        ShardPercolateService shardPercolateService = indexShard.shardPercolateService();
        shardPercolateService.prePercolate();
        long startTime = System.nanoTime();
        try {
            ConcurrentMap<Text, Query> percolateQueries = indexShard.percolateRegistry().percolateQueries();
            if (percolateQueries.isEmpty()) {
                return new PercolateShardResponse(request.index(), request.shardId());
            }

            final PercolateContext context = new PercolateContext();
            ParsedDocument parsedDocument = parsePercolate(percolateIndexService, request, context);
            if (request.docSource() != null && request.docSource().length() != 0) {
                parsedDocument = parseFetchedDoc(request.docSource(), percolateIndexService, request.documentType());
            } else if (parsedDocument == null) {
                throw new ElasticSearchParseException("No doc to percolate in the request");
            }

            if (context.size < 0) {
                context.size = 0;
            }

            if (request.onlyCount() && context.limit && context.size == 0) {
                return new PercolateShardResponse(request.index(), request.shardId());
            } else if (context.limit && context.shortCircuit && context.size == 0) {
                return new PercolateShardResponse(request.index(), request.shardId());
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

                context.docSearcher = memoryIndex.createSearcher();
                IndexFieldDataService fieldDataService = percolateIndexService.fieldData();
                if (context.filter != null) {
                    context.queryIterator = new IndexShardQueryIterator(percolateQueries, indexShard, fieldDataService, context.filter);
                } else {
                    context.queryIterator = new MapBasedQueryIterator(percolateQueries);
                }

                IndexCache indexCache = percolateIndexService.cache();
                try {
                    return action.doPercolateAction(context);
                } finally {
                    // explicitly clear the reader, since we can only register on callback on SegmentReader
                    indexCache.clear(context.docSearcher.getIndexReader());
                    fieldDataService.clear(context.docSearcher.getIndexReader());
                    context.queryIterator.abort();
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

        ParsedDocument doc = null;
        XContentParser parser = null;
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
                        doc = docMapper.parse(source(parser).type(request.documentType()).flyweight(true));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        if (context.filter != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        context.filter = new QueryWrapperFilter(documentIndexService.queryParserService().parse(parser).query());
                    } else if ("filter".equals(currentFieldName)) {
                        if (context.filter != null) {
                            throw new ElasticSearchParseException("Either specify query or filter, not both");
                        }
                        context.filter = documentIndexService.queryParserService().parseInnerFilter(parser).filter();
                    }
                } else if (token.isValue()) {
                    if ("size".equals(currentFieldName)) {
                        context.limit = true;
                        context.size = parser.intValue();
                        if (context.size < 0) {
                            throw new ElasticSearchParseException("size is set to [" + context.size + "] and is expected to be higher or equal to 0");
                        }
                    } else if ("short_circuit".equals(currentFieldName)) {
                        context.shortCircuit = parser.booleanValue();
                    }
                } else if (token == null) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new ElasticSearchParseException("failed to parse request", e);
        } finally {
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

    interface PercolateAction {

        PercolateShardResponse doPercolateAction(PercolateContext context);

    }

    public class PercolateContext {

        public boolean limit;
        public int size;
        public boolean shortCircuit;


        Filter filter;
        QueryIterator queryIterator;
        IndexSearcher docSearcher;
    }

    public static final class Constants {

        public static final String TYPE_NAME = "_percolator";

    }

    abstract class QueryIterator {

        abstract boolean next();

        abstract Query query();

        abstract Text queryId();

        void abort() {
        }

    }

    class MapBasedQueryIterator extends QueryIterator {

        private final Iterator<Map.Entry<Text, Query>> iterator;
        private Map.Entry<Text, Query> current;

        MapBasedQueryIterator(ConcurrentMap<Text, Query> queries) {
            this.iterator = queries.entrySet().iterator();
        }

        @Override
        boolean next() {
            if (iterator.hasNext()) {
                current = iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        Query query() {
            return current.getValue();
        }

        @Override
        Text queryId() {
            return current.getKey();
        }
    }

    class IndexShardQueryIterator extends QueryIterator {

        private final ConcurrentMap<Text, Query> queries;
        private final Engine.Searcher percolatorSearcher;
        private final List<AtomicReaderContext> leaves;
        private final IndexFieldData uidFieldData;
        private final Filter percolatorFilter;

        private int currentLeaveIndex;
        private BytesValues values;
        private DocIdSetIterator docIdSetIterator;

        private Query currentQuery;
        private Text currentQueryString;

        IndexShardQueryIterator(ConcurrentMap<Text, Query> queries, IndexShard indexShard, IndexFieldDataService fieldData, Filter percolatorFilter) {
            this.queries = queries;
            percolatorSearcher = indexShard.searcher();
            leaves = percolatorSearcher.searcher().getTopReaderContext().leaves();
            // TODO: when we move to a UID level mapping def on the index level, we can use that one, now, its per type, and we can't easily choose one
            this.uidFieldData = fieldData.getForField(new FieldMapper.Names(UidFieldMapper.NAME), new FieldDataType("string", ImmutableSettings.builder().put("format", "paged_bytes")));
            this.percolatorFilter = percolatorFilter;
        }

        @Override
        boolean next() {
            while (true) {
                if (docIdSetIterator == null) {
                    while (true) {
                        if (currentLeaveIndex >= leaves.size()) {
                            return false;
                        }

                        AtomicReaderContext currentContext = leaves.get(currentLeaveIndex++);
                        values = uidFieldData.load(currentContext).getBytesValues();
                        try {
                            DocIdSet docIdSet = percolatorFilter.getDocIdSet(currentContext, currentContext.reader().getLiveDocs());
                            if (DocIdSets.isEmpty(docIdSet)) {
                                continue;
                            }

                            docIdSetIterator = docIdSet.iterator();
                            if (docIdSetIterator == null) {
                                continue;
                            }
                        } catch (IOException e) {
                            throw new ElasticSearchException("Can't run " + percolatorFilter, e);
                        }
                        break;
                    }
                }

                // unset
                currentQueryString = null;
                currentQuery = null;

                try {
                    for (int doc = docIdSetIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docIdSetIterator.nextDoc()) {
                        BytesRef uid = values.getValue(doc);
                        if (uid == null) {
                            continue;
                        }
                        // if there is an id field, then we can optimize this
                        currentQueryString = new BytesText(Uid.idFromUid(uid));
                        currentQuery = queries.get(currentQueryString);
                        if (currentQuery != null) {
                            return true;
                        }
                    }
                } catch (IOException e) {
                    throw new ElasticSearchException("Can't run " + percolatorFilter, e);
                }

                docIdSetIterator = null;
                if (currentLeaveIndex >= leaves.size()) {
                    return false;
                }
            }
        }

        @Override
        Query query() {
            return currentQuery;
        }

        @Override
        Text queryId() {
            return currentQueryString;
        }

        @Override
        void abort() {
            percolatorSearcher.release();
        }
    }

}
