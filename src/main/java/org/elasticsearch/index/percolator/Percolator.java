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

package org.elasticsearch.index.percolator;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.ReusableMemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.memory.MemoryIndexPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 */
public class Percolator extends AbstractComponent {

    private final MemoryIndexPool memIndexPool;
    private final IndicesService indicesService;

    @Inject
    public Percolator(Settings settings, MemoryIndexPool memIndexPool, IndicesService indicesService) {
        super(settings);
        this.memIndexPool = memIndexPool;
        this.indicesService = indicesService;
    }

    public Response percolate(Request request) {
        IndexService percolateIndexService = indicesService.indexServiceSafe(request.percolateIndex());
        IndexShard indexShard = percolateIndexService.shardSafe(request.shardId());

        Map<String, Query> percolateQueries = indexShard.percolateRegistry().percolateQueries();
        if (percolateQueries.isEmpty()) {
            return new Response(Strings.EMPTY_ARRAY);
        }

        Tuple<ParsedDocument, Query> parseResult = parsePercolate(request);
        ParsedDocument parsedDocument = parseResult.v1();
        Query query = parseResult.v2();

        // first, parse the source doc into a MemoryIndex
        final ReusableMemoryIndex memoryIndex = memIndexPool.acquire();
        try {
            // TODO: This means percolation does not support nested docs...
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

            final IndexSearcher searcher = memoryIndex.createSearcher();
            List<String> matches = new ArrayList<String>();

            IndexFieldDataService fieldDataService = percolateIndexService.fieldData();
            IndexCache indexCache = percolateIndexService.cache();
            try {
                if (query == null) {
                    Lucene.ExistsCollector collector = new Lucene.ExistsCollector();
                    for (Map.Entry<String, Query> entry : percolateQueries.entrySet()) {
                        collector.reset();
                        try {
                            searcher.search(entry.getValue(), collector);
                        } catch (IOException e) {
                            logger.warn("[" + entry.getKey() + "] failed to execute query", e);
                        }

                        if (collector.exists()) {
                            matches.add(entry.getKey());
                        }
                    }
                } else {
                    Engine.Searcher percolatorSearcher = indexShard.searcher();
                    try {
                        percolatorSearcher.searcher().search(
                                query, new QueryCollector(logger, percolateQueries, searcher, fieldDataService, matches)
                        );
                    } catch (IOException e) {
                        logger.warn("failed to execute", e);
                    } finally {
                        percolatorSearcher.release();
                    }
                }
            } finally {
                // explicitly clear the reader, since we can only register on callback on SegmentReader
                indexCache.clear(searcher.getIndexReader());
                fieldDataService.clear(searcher.getIndexReader());
            }
            return new Response(matches.toArray(new String[matches.size()]));
        } finally {
            memIndexPool.release(memoryIndex);
        }
    }

    Tuple<ParsedDocument, Query> parsePercolate(Request request) throws ElasticSearchException {
        IndexService documentIndexService = indicesService.indexServiceSafe(request.documentIndex());
        Index index = new Index(request.documentIndex());

        Query query = null;
        ParsedDocument doc = null;
        XContentParser parser = null;
        try {

            parser = XContentFactory.xContent(request.source()).createParser(request.source());
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                    // we need to check the "doc" here, so the next token will be START_OBJECT which is
                    // the actual document starting
                    if ("doc".equals(currentFieldName)) {
                        MapperService mapperService = documentIndexService.mapperService();
                        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(request.documentType());
                        doc = docMapper.parse(source(parser).type(request.documentType()).flyweight(true));
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("query".equals(currentFieldName)) {
                        query = documentIndexService.queryParserService().parse(parser).query();
                    }
                } else if (token == null) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new PercolatorException(index, "failed to parse request", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        if (doc == null) {
            throw new PercolatorException(index, "No doc to percolate in the request");
        }

        return new Tuple<ParsedDocument, Query>(doc, query);
    }

    public static class Constants {

        public static final String TYPE_NAME = "_percolator";

    }

    public static class Request {

        private final String percolateIndex;
        private final String documentIndex;
        private final String documentType;
        private final int shardId;
        private final BytesReference source;

        public Request(String percolateIndex, String documentIndex, String documentType, int shardId, BytesReference source) {
            this.percolateIndex = percolateIndex;
            if (documentIndex != null) {
                this.documentIndex = documentIndex;
            } else {
                this.documentIndex = percolateIndex;
            }
            this.documentType = documentType;
            this.shardId = shardId;
            this.source = source;
        }

        public String percolateIndex() {
            return percolateIndex;
        }

        public String documentIndex() {
            return documentIndex;
        }

        public String documentType() {
            return documentType;
        }

        public int shardId() {
            return shardId;
        }

        public BytesReference source() {
            return source;
        }
    }

    public static class Response {

        private final String[] matches;

        public Response(String[] matches) {
            this.matches = matches;
        }

        public String[] matches() {
            return matches;
        }
    }

}
