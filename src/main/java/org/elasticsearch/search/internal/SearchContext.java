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

package org.elasticsearch.search.internal;

import com.google.common.collect.ImmutableList;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.search.AndFilter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.cache.id.IdCache;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.scan.ScanContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class SearchContext implements FetchContext {

    private static ThreadLocal<SearchContext> current = new ThreadLocal<SearchContext>();

    public static void setCurrent(SearchContext value) {
        current.set(value);
        QueryParseContext.setTypes(value.types());
    }

    public static void removeCurrent() {
        current.remove();
        QueryParseContext.removeTypes();
    }

    public static SearchContext current() {
        return current.get();
    }

    public static interface Rewrite {

        void contextRewrite(SearchContext searchContext) throws Exception;

        void executionDone();

        void contextClear();
    }

    private final long id;

    private final ShardSearchRequest request;

    private final SearchShardTarget shardTarget;

    private final SearchParseContext searchParseContext;

    private SearchType searchType;

    private final Engine.Searcher engineSearcher;

    private final ScriptService scriptService;

    private final CacheRecycler cacheRecycler;

    private final IndexShard indexShard;

    private final IndexService indexService;

    private final ContextIndexSearcher searcher;

    private final DfsSearchResult dfsResult;

    private final QuerySearchResult queryResult;

    private final FetchSearchResult fetchResult;

    // lazy initialized only if needed
    private ScanContext scanContext;

    private Scroll scroll;

    private Filter aliasFilter;

    private int[] docIdsToLoad;

    private int docsIdsToLoadFrom;

    private int docsIdsToLoadSize;

    private SearchLookup searchLookup;

    private boolean queryRewritten;

    private volatile long keepAlive;

    private volatile long lastAccessTime;

    private List<Rewrite> rewrites = null;


    public SearchContext(long id, ShardSearchRequest request, SearchShardTarget shardTarget,
                         Engine.Searcher engineSearcher, IndexService indexService, IndexShard indexShard,
                         ScriptService scriptService, CacheRecycler cacheRecycler) {
        this.id = id;
        this.request = request;
        this.searchType = request.searchType();
        this.shardTarget = shardTarget;
        this.searchParseContext = new SearchParseContext(shardTarget, indexService, request.types(), scriptService, cacheRecycler);
        this.engineSearcher = engineSearcher;
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.dfsResult = new DfsSearchResult(id, shardTarget);
        this.queryResult = new QuerySearchResult(id, shardTarget);
        this.fetchResult = new FetchSearchResult(id, shardTarget);
        this.indexShard = indexShard;
        this.indexService = indexService;

        this.searcher = new ContextIndexSearcher(this, engineSearcher);

        // initialize the filtering alias based on the provided filters
        aliasFilter = indexService.aliasesService().aliasFilter(request.filteringAliases());
    }

    @Override
    public boolean release() throws ElasticSearchException {
        if (scanContext != null) {
            scanContext.clear();
        }
        // clear and scope phase we  have
        if (rewrites != null) {
            for (Rewrite rewrite : rewrites) {
                rewrite.contextClear();
            }
        }
        searcher.release();
        engineSearcher.release();
        return true;
    }

    /**
     * Should be called before executing the main query and after all other parameters have been set.
     */
    public void preProcess() {
        if (query() == null) {
            parsedQuery(ParsedQuery.parsedMatchAllQuery());
        }
        if (queryBoost() != 1.0f) {
            parsedQuery(new ParsedQuery(new FunctionScoreQuery(query(), new BoostScoreFunction(searchParseContext.queryBoost())), parsedQuery()));
        }
        Filter searchFilter = searchFilter(types());
        if (searchFilter != null) {
            if (Queries.isConstantMatchAllQuery(query())) {
                Query q = new XConstantScoreQuery(searchFilter);
                q.setBoost(query().getBoost());
                parsedQuery(new ParsedQuery(q, parsedQuery()));
            } else {
                parsedQuery(new ParsedQuery(new XFilteredQuery(query(), searchFilter), parsedQuery()));
            }
        }
    }

    public Filter searchFilter(String[] types) {
        Filter filter = mapperService().searchFilter(types);
        if (filter == null) {
            return aliasFilter;
        } else {
            filter = filterCache().cache(filter);
            if (aliasFilter != null) {
                return new AndFilter(ImmutableList.of(filter, aliasFilter));
            }
            return filter;
        }
    }

    public long id() {
        return this.id;
    }

    public ShardSearchRequest request() {
        return this.request;
    }

    public SearchType searchType() {
        return this.searchType;
    }

    public SearchContext searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    public SearchParseContext searchParseContext() {
        return searchParseContext;
    }

    public int numberOfShards() {
        return request.numberOfShards();
    }

    public boolean hasTypes() {
        return request.types() != null && request.types().length > 0;
    }

    public String[] types() {
        return request.types();
    }

    public float queryBoost() {
        return searchParseContext.queryBoost();
    }

    public long nowInMillis() {
        return request.nowInMillis();
    }

    public Scroll scroll() {
        return this.scroll;
    }

    public SearchContext scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    public SearchContextFacets facets() {
        return searchParseContext.facets();
    }

    public SearchContextHighlight highlight() {
        return searchParseContext.highlight();
    }

    public SuggestionSearchContext suggest() {
        return searchParseContext.suggest();
    }

    public RescoreSearchContext rescore() {
        return searchParseContext.rescore();
    }

    public boolean hasScriptFields() {
        return searchParseContext.hasScriptFields();
    }

    public ScriptFieldsContext scriptFields() {
        return searchParseContext.scriptFields();
    }

    public boolean hasPartialFields() {
        return searchParseContext.hasPartialFields();
    }

    public PartialFieldsContext partialFields() {
        return searchParseContext.partialFields();
    }

    /**
     * A shortcut function to see whether there is a fetchSourceContext and it says the source is requested.
     */
    public boolean sourceRequested() {
        return searchParseContext.sourceRequested();
    }

    public boolean hasFetchSourceContext() {
        return searchParseContext.fetchSourceContext() != null;
    }

    public FetchSourceContext fetchSourceContext() {
        return searchParseContext.fetchSourceContext();
    }

    // TODO: check
    public SearchContext fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.searchParseContext.fetchSourceContext(fetchSourceContext);
        return this;
    }

    public ContextIndexSearcher searcher() {
        return this.searcher;
    }

    public IndexShard indexShard() {
        return this.indexShard;
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public AnalysisService analysisService() {
        return indexService.analysisService();
    }

    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    public SimilarityService similarityService() {
        return indexService.similarityService();
    }

    public ScriptService scriptService() {
        return scriptService;
    }

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    public FilterCache filterCache() {
        return indexService.cache().filter();
    }

    public DocSetCache docSetCache() {
        return indexService.cache().docSet();
    }

    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    public IdCache idCache() {
        return indexService.cache().idCache();
    }

    public long timeoutInMillis() {
        return searchParseContext.timeoutInMillis();
    }

    public SearchContext minimumScore(float minimumScore) {
        searchParseContext.minimumScore(minimumScore);
        return this;
    }

    public Float minimumScore() {
        return searchParseContext.minimumScore();
    }

    public Sort sort() {
        return searchParseContext.sort();
    }

    public boolean trackScores() {
        return searchParseContext.trackScores();
    }

    public ParsedFilter parsedFilter() {
        return searchParseContext.parsedFilter();
    }

    public Filter aliasFilter() {
        return aliasFilter;
    }

    public SearchContext parsedQuery(ParsedQuery query) {
        queryRewritten = false;
        searchParseContext.parsedQuery(query);
        return this;
    }

    public ParsedQuery parsedQuery() {
        return searchParseContext.parsedQuery();
    }

    /**
     * The query to execute, might be rewritten.
     */
    public Query query() {
        return searchParseContext.query();
    }

    /**
     * Has the query been rewritten already?
     */
    public boolean queryRewritten() {
        return queryRewritten;
    }

    /**
     * Rewrites the query and updates it. Only happens once.
     */
    public SearchContext updateRewriteQuery(Query rewriteQuery) {
        searchParseContext.query(rewriteQuery);
        queryRewritten = true;
        return this;
    }

    public int from() {
        return searchParseContext.from();
    }

    public SearchContext from(int from) {
        this.searchParseContext.from(from);
        return this;
    }

    public int size() {
        return searchParseContext.size();
    }

    public SearchContext size(int size) {
        this.searchParseContext.size(size);
        return this;
    }

    public boolean hasFieldNames() {
        return searchParseContext.hasFieldNames();
    }

    public List<String> fieldNames() {
        return searchParseContext.fieldNames();
    }

    public boolean explain() {
        return searchParseContext.explain();
    }

    @Nullable
    public List<String> groupStats() {
        return searchParseContext.groupStats();
    }

    public boolean version() {
        return searchParseContext.version();
    }

    public int[] docIdsToLoad() {
        return docIdsToLoad;
    }

    public int docIdsToLoadFrom() {
        return docsIdsToLoadFrom;
    }

    public int docIdsToLoadSize() {
        return docsIdsToLoadSize;
    }

    public SearchContext docIdsToLoad(int[] docIdsToLoad, int docsIdsToLoadFrom, int docsIdsToLoadSize) {
        this.docIdsToLoad = docIdsToLoad;
        this.docsIdsToLoadFrom = docsIdsToLoadFrom;
        this.docsIdsToLoadSize = docsIdsToLoadSize;
        return this;
    }

    public void accessed(long accessTime) {
        this.lastAccessTime = accessTime;
    }

    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    public long keepAlive() {
        return this.keepAlive;
    }

    public void keepAlive(long keepAlive) {
        this.keepAlive = keepAlive;
    }

    public SearchLookup lookup() {
        // TODO: The types should take into account the parsing context in QueryParserContext...
        if (searchLookup == null) {
            searchLookup = new SearchLookup(mapperService(), fieldData(), request.types());
        }
        return searchLookup;
    }

    public DfsSearchResult dfsResult() {
        return dfsResult;
    }

    public QuerySearchResult queryResult() {
        return queryResult;
    }

    public FetchSearchResult fetchResult() {
        return fetchResult;
    }

    public void addRewrite(Rewrite rewrite) {
        if (this.rewrites == null) {
            this.rewrites = new ArrayList<Rewrite>();
        }
        this.rewrites.add(rewrite);
    }

    public List<Rewrite> rewrites() {
        return this.rewrites;
    }

    public ScanContext scanContext() {
        if (scanContext == null) {
            scanContext = new ScanContext();
        }
        return scanContext;
    }

    public FieldMappers smartNameFieldMappers(String name) {
        return mapperService().smartNameFieldMappers(name, request.types());
    }

}
