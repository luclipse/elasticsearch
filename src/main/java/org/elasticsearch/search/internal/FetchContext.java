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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ParsedFilter;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.partial.PartialFieldsContext;
import org.elasticsearch.search.fetch.script.ScriptFieldsContext;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.search.highlight.SearchContextHighlight;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.rescore.RescoreSearchContext;

/**
 */
public interface FetchContext extends Releasable {

    SearchShardTarget shardTarget();

    SearchParseContext searchParseContext();

    MapperService mapperService();

    SearchLookup lookup();

    Query query();

    ParsedQuery parsedQuery();

    IndexSearcher searcher();

    FetchSourceContext fetchSourceContext();

    ParsedFilter parsedFilter();

    boolean sourceRequested();

    boolean hasPartialFields();

    PartialFieldsContext partialFields();

    boolean hasScriptFields();

    ScriptFieldsContext scriptFields();

    SearchContextHighlight highlight();

    RescoreSearchContext rescore();

    boolean version();

    boolean explain();

}
