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

package org.elasticsearch.search.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.cache.filter.FilterCache;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.SearchContextFacets;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.rescore.RescoreSearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

import java.util.List;

/**
 */
public class SearchParseContext {

    private final SearchShardTarget searchShardTarget;
    private final IndexService indexService;
    private final String[] types;
    private final ScriptService scriptService;
    private final CacheRecycler cacheRecycler;

    // Fetch phase
    private SearchContextHighlight searchContextHighlight;
    private FetchSourceContext fetchSourceContext;
    private ScriptFieldsContext scriptFields;
    private PartialFieldsContext partialFields;
    private List<String> fieldNames;
    private boolean explain;
    private boolean version = false; // by default, we don't return versions

    // Query phase
    private SearchContextFacets facets;
    private SuggestionSearchContext suggest;
    private RescoreSearchContext rescore;

    private Float minimumScore;
    private SearchLookup searchLookup;
    private ParsedFilter filter;
    private int from = -1;
    private int size = -1;
    private float queryBoost = 1.0f;
    private ParsedQuery originalQuery;
    private Query query;
    private Sort sort;
    private List<String> groupStats;
    // timeout in millis
    private long timeoutInMillis = -1;
    private boolean trackScores = false; // when sorting, track scores as well...

    public SearchParseContext(SearchShardTarget searchShardTarget, IndexService indexService, String[] types, ScriptService scriptService, CacheRecycler cacheRecycler) {
        this.scriptService = scriptService;
        this.cacheRecycler = cacheRecycler;
        this.searchShardTarget = searchShardTarget;
        this.indexService = indexService;
        this.types = types;
    }

    public void highlight(SearchContextHighlight searchContextHighlight) {
        this.searchContextHighlight = searchContextHighlight;
    }

    public SearchContextHighlight highlight() {
        return searchContextHighlight;
    }

    public void explain(boolean explain) {
        this.explain = explain;
    }

    public boolean explain() {
        return explain;
    }

    public void minimumScore(float minimumScore) {
        this.minimumScore = minimumScore;
    }

    public Float minimumScore() {
        return minimumScore;
    }


    public FilterCache filterCache() {
        return indexService.cache().filter();
    }

    public void facets(SearchContextFacets facets) {
        this.facets = facets;
    }

    public SearchContextFacets facets() {
        return facets;
    }

    public boolean sourceRequested() {
        return fetchSourceContext() != null && fetchSourceContext().fetchSource();
    }

    public void fetchSourceContext(FetchSourceContext fetchSourceContext) {
        this.fetchSourceContext = fetchSourceContext;
    }

    public FetchSourceContext fetchSourceContext() {
        return fetchSourceContext;
    }

    public ScriptService scriptService() {
        return scriptService;
    }

    public boolean hasScriptFields() {
        return scriptFields != null;
    }

    public ScriptFieldsContext scriptFields() {
        if (scriptFields == null) {
            scriptFields = new ScriptFieldsContext();
        }
        return this.scriptFields;
    }

    public boolean hasPartialFields() {
        return partialFields != null;
    }

    public PartialFieldsContext partialFields() {
        if (partialFields == null) {
            partialFields = new PartialFieldsContext();
        }
        return this.partialFields;
    }

    public boolean hasFieldNames() {
        return fieldNames != null;
    }

    public List<String> fieldNames() {
        if (fieldNames == null) {
            fieldNames = Lists.newArrayList();
        }
        return fieldNames;
    }

    public void emptyFieldNames() {
        this.fieldNames = ImmutableList.of();
    }

    public SearchLookup lookup() {
        // TODO: The types should take into account the parsing context in QueryParserContext...
        if (searchLookup == null) {
            searchLookup = new SearchLookup(indexService.mapperService(), indexService.fieldData(), types);
        }
        return searchLookup;
    }

    public SearchParseContext parsedFilter(ParsedFilter filter) {
        this.filter = filter;
        return this;
    }

    public ParsedFilter parsedFilter() {
        return this.filter;
    }

    public void from(int from) {
        this.from = from;
    }

    public int from() {
        return from;
    }

    public float queryBoost() {
        return queryBoost;
    }

    public SearchParseContext queryBoost(float queryBoost) {
        this.queryBoost = queryBoost;
        return this;
    }

    public SearchParseContext parsedQuery(ParsedQuery query) {
        this.originalQuery = query;
        this.query = query.query();
        return this;
    }

    public ParsedQuery parsedQuery() {
        return this.originalQuery;
    }

    public void query(Query query) {
        this.query = query;
    }

    public Query query() {
        return query;
    }

    public RescoreSearchContext rescore() {
        return this.rescore;
    }

    public void rescore(RescoreSearchContext rescore) {
        this.rescore = rescore;
    }

    public int size() {
        return size;
    }

    public SearchParseContext size(int size) {
        this.size = size;
        return this;
    }

    public SearchParseContext sort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public Sort sort() {
        return this.sort;
    }

    @Nullable
    public List<String> groupStats() {
        return this.groupStats;
    }

    public void groupStats(List<String> groupStats) {
        this.groupStats = groupStats;
    }

    public SuggestionSearchContext suggest() {
        return suggest;
    }

    public void suggest(SuggestionSearchContext suggest) {
        this.suggest = suggest;
    }

    public long timeoutInMillis() {
        return timeoutInMillis;
    }

    public void timeoutInMillis(long timeoutInMillis) {
        this.timeoutInMillis = timeoutInMillis;
    }

    public SearchParseContext trackScores(boolean trackScores) {
        this.trackScores = trackScores;
        return this;
    }

    public boolean trackScores() {
        return this.trackScores;
    }

    public boolean version() {
        return version;
    }

    public void version(boolean version) {
        this.version = version;
    }

    public SearchShardTarget shardTarget() {
        return searchShardTarget;
    }

    public MapperService mapperService() {
        return indexService.mapperService();
    }

    public IndexFieldDataService fieldData() {
        return indexService.fieldData();
    }

    public FieldMapper smartNameFieldMapper(String name) {
        return mapperService().smartNameFieldMapper(name, types);
    }

    public MapperService.SmartNameObjectMapper smartNameObjectMapper(String name) {
        return mapperService().smartNameObjectMapper(name, types);
    }

    public IndexQueryParserService queryParserService() {
        return indexService.queryParserService();
    }

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

}
