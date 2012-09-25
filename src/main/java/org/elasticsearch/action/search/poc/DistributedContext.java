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

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;

/**
 * @author Martijn van Groningen
 */
public class DistributedContext {

    private final ClusterState clusterState;
    private final String[] concreteIndices;
    private final Map<String, String[]> concreteIndexToFilteringAliases;
    private final Map<String, Set<String>> routing;

    private final SearchRequest searchRequest;
    private final long startTime;

    private GroupShardsIterator searchShards;
    private Map<SearchShardTarget, Long> searchShardTargetToSearchContext;
    private AggregatedDfs aggregateDfs;

    private DistributedContext(ClusterState clusterState, String[] concreteIndices, Map<String, String[]> concreteIndexToFilteringAliases,
                              Map<String, Set<String>> routing, SearchRequest request) {
        this.clusterState = clusterState;
        this.concreteIndices = concreteIndices;
        this.concreteIndexToFilteringAliases = concreteIndexToFilteringAliases;
        this.routing = routing;
        this.searchRequest = request;
        this.startTime = System.currentTimeMillis();
    }

    public String[] getConcreteIndices() {
        return concreteIndices;
    }

    public String[] getFilteringAliases(String concreteIndex) {
        return concreteIndexToFilteringAliases.get(concreteIndex);
    }

    public String preference() {
        return searchRequest.preference();
    }

    public Map<String, Set<String>> routing() {
        return routing;
    }

    public String queryHint() {
        return searchRequest.queryHint();
    }

    public long startTime() {
        return startTime;
    }

    public GroupShardsIterator searchShards() {
        return searchShards;
    }

    public void setSearchShardTargetToSearchContext(Map<SearchShardTarget,Long> searchShardTargetToSearchContext) {
        this.searchShardTargetToSearchContext = searchShardTargetToSearchContext;
    }

    public Map<SearchShardTarget, Long> searchShardTargetToSearchContext() {
        return searchShardTargetToSearchContext;
    }

    public void setAggregateDfs(AggregatedDfs aggregateDfs) {
        this.aggregateDfs = aggregateDfs;
    }

    public AggregatedDfs aggregateDfs() {
        return aggregateDfs;
    }

    // TODO: Move to some other place
    public static DistributedContext create(ClusterService clusterService, SearchRequest request) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = clusterState.metaData().concreteIndices(request.indices(), request.ignoreIndices(), true);
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        Map<String, String[]> concreteIndexToFilteringAliases = newHashMap();
        for (String concreteIndex : concreteIndices) {
            String[] filteringAliases = clusterState.metaData().filteringAliases(concreteIndex, request.indices());
            if (filteringAliases != null && filteringAliases.length != 0) {
                concreteIndexToFilteringAliases.put(concreteIndex, filteringAliases);
            }
        }
        clusterService.operationRouting().searchShards()

        return new DistributedContext(clusterState, concreteIndices, concreteIndexToFilteringAliases, routingMap, request);
    }

}
