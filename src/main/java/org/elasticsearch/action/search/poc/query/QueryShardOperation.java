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

package org.elasticsearch.action.search.poc.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.poc.DistributedContext;
import org.elasticsearch.action.search.poc.ShardOperation;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;

/**
 * @author Martijn van Groningen
 */
public class QueryShardOperation implements ShardOperation<ShardSearchRequest, QuerySearchResult> {

    private final SearchServiceTransportAction searchServiceTransportAction;
    private final ClusterService clusterService;

    @Inject
    public QueryShardOperation(SearchServiceTransportAction searchServiceTransportAction, ClusterService clusterService) {
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.clusterService = clusterService;
    }

    @Override
    public void execute(ShardSearchRequest shardRequest, DistributedContext context, final ActionListener<QuerySearchResult> listener) {
        ClusterState clusterState = clusterService.state();
        DiscoveryNode node = clusterState.nodes().get(shardRequest.getShardRouting().currentNodeId());
        // TODO: threading
        if (shardRequest.getRequest1() != null) {
            QuerySearchRequest request = shardRequest.getRequest1();
            searchServiceTransportAction.sendExecuteQuery(node, request, new SearchServiceListener<QuerySearchResult>() {

                @Override
                public void onResult(QuerySearchResult result) {
                    listener.onResponse(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(t);
                }

            });
        } else if (shardRequest.getRequest2() != null) {
            InternalSearchRequest request = shardRequest.getRequest2();
            searchServiceTransportAction.sendExecuteQuery(node, request, new SearchServiceListener<QuerySearchResult>() {

                @Override
                public void onResult(QuerySearchResult result) {
                    listener.onResponse(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    listener.onFailure(t);
                }

            });
        }
    }
}
