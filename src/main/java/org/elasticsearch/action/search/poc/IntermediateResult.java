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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search.poc;

import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Martijn van Groningen
 */
public class IntermediateResult {

    private Map<SearchShardTarget, QuerySearchResult> shardResults = newHashMap();
    private QuerySearchResult mergedQuerySearchResult;

    public Map<SearchShardTarget, QuerySearchResult> getShardResults() {
        return shardResults;
    }

    public void setShardResults(Map<SearchShardTarget, QuerySearchResult> shardResults) {
        this.shardResults = shardResults;
    }

    public QuerySearchResult getMergedQuerySearchResult() {
        return mergedQuerySearchResult;
    }

    public void setMergedQuerySearchResult(QuerySearchResult mergedQuerySearchResult) {
        this.mergedQuerySearchResult = mergedQuerySearchResult;
    }
}
