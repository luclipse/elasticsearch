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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fieldvisitor.JustSourceFieldsVisitor;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.internal.FetchContext;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchParseContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.rescore.RescoreSearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class PercolateContext implements FetchContext {

    public boolean limit;
    public int size;
    public boolean score;
    public boolean sort;
    public byte percolatorTypeId;

    private final PercolateShardRequest request;
    private final SearchShardTarget searchShardTarget;
    private final IndexService indexService;
    private final IndexShard indexShard;
    private final ConcurrentMap<HashedBytesRef, Query> percolateQueries;

    private final SearchParseContext searchParseContext;

    private Engine.Searcher engineSearcher;
    Query query;
    IndexSearcher docSearcher;

    IndexFieldDataService fieldDataService;
    FetchSubPhase.HitContext hitContext;

    public PercolateContext(PercolateShardRequest request, SearchShardTarget searchShardTarget, IndexShard indexShard, IndexService indexService, ScriptService scriptService, CacheRecycler cacheRecycler) {
        this.request = request;
        this.indexShard = indexShard;
        this.indexService = indexService;
        this.searchParseContext = new SearchParseContext(
                searchShardTarget, indexService, new String[]{request.documentType()}, scriptService, cacheRecycler
        );
        this.searchShardTarget = searchShardTarget;
        this.percolateQueries = indexShard.percolateRegistry().percolateQueries();
    }

    public void initializeHitContext(final MemoryIndex memoryIndex, ParsedDocument parsedDocument) {
        final IndexSearcher docSearcher = memoryIndex.createSearcher();
        final IndexReader topLevelReader = docSearcher.getIndexReader();
        AtomicReaderContext readerContext = topLevelReader.leaves().get(0);
        engineSearcher = new Engine.Searcher() {
            @Override
            public IndexReader reader() {
                return topLevelReader;
            }

            @Override
            public IndexSearcher searcher() {
                return docSearcher;
            }

            @Override
            public boolean release() throws ElasticSearchException {
                try {
                    docSearcher.getIndexReader().close();
                    memoryIndex.reset();
                } catch (IOException e) {
                    throw new ElasticSearchException("failed to close percolator in-memory index", e);
                }
                return true;
            }
        };
        lookup().setNextReader(readerContext);
        lookup().setNextDocId(0);
        lookup().source().setNextSource(parsedDocument.source());

        Map<String, SearchHitField> fields = new HashMap<String, SearchHitField>();
        for (IndexableField field : parsedDocument.rootDoc().getFields()) {
            List<Object> values = lookup().source().extractRawValues(field.name());
            fields.put(field.name(), new InternalSearchHitField(field.name(), values));
        }
        hitContext = new FetchSubPhase.HitContext();
        hitContext.reset(new InternalSearchHit(0, "unknown", new StringText(request.documentType()), fields), readerContext, 0, topLevelReader, 0, new JustSourceFieldsVisitor());
    }

    @Override
    public IndexSearcher searcher() {
        return engineSearcher.searcher();
    }

    public IndexShard indexShard() {
        return indexShard;
    }

    public IndexService indexService() {
        return indexService;
    }

    public ConcurrentMap<HashedBytesRef, Query> percolateQueries() {
        return percolateQueries;
    }

    @Override
    public SearchContextHighlight highlight() {
        return searchParseContext.highlight();
    }

    @Override
    public SearchLookup lookup() {
        return searchParseContext.lookup();
    }

    @Override
    public RescoreSearchContext rescore() {
        return searchParseContext.rescore();
    }

    @Override
    public Query query() {
        return searchParseContext.query();
    }

    @Override
    public boolean sourceRequested() {
        return searchParseContext.sourceRequested();
    }

    @Override
    public FetchSourceContext fetchSourceContext() {
        return searchParseContext.fetchSourceContext();
    }

    @Override
    public MapperService mapperService() {
        return searchParseContext.mapperService();
    }

    @Override
    public ParsedQuery parsedQuery() {
        return searchParseContext.parsedQuery();
    }

    @Override
    public ParsedFilter parsedFilter() {
        return searchParseContext.parsedFilter();
    }

    @Override
    public boolean hasPartialFields() {
        return searchParseContext.hasPartialFields();
    }

    @Override
    public PartialFieldsContext partialFields() {
        return searchParseContext.partialFields();
    }

    @Override
    public boolean hasScriptFields() {
        return searchParseContext.hasScriptFields();
    }

    @Override
    public ScriptFieldsContext scriptFields() {
        return searchParseContext.scriptFields();
    }

    @Override
    public boolean version() {
        return searchParseContext.version();
    }

    @Override
    public boolean explain() {
        return searchParseContext.explain();
    }

    @Override
    public SearchShardTarget shardTarget() {
        return searchShardTarget;
    }

    @Override
    public SearchParseContext searchParseContext() {
        return searchParseContext;
    }

    @Override
    public boolean release() throws ElasticSearchException {
        if (engineSearcher != null) {
            return engineSearcher.release();
        } else {
            return false;
        }
    }
}
