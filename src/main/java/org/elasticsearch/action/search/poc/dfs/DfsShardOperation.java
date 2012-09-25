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

package org.elasticsearch.action.search.poc.dfs;

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

/**
 * @author Martijn van Groningen
 */
public class DfsShardOperation implements ShardOperation<InternalSearchRequest, DfsSearchResult> {

    private final SearchServiceTransportAction searchServiceTransportAction;
    private final ClusterService clusterService;

    @Inject
    public DfsShardOperation(SearchServiceTransportAction searchServiceTransportAction, ClusterService clusterService) {
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.clusterService = clusterService;
    }

    @Override
    public void execute(InternalSearchRequest dfsShardOperationRequest,
                        DistributedContext context,
                        final ActionListener<DfsSearchResult> listener) {
        ClusterState clusterState = clusterService.state();
        for (ShardIterator shardIt : context.searchShards()) {
            final ShardRouting shard = shardIt.firstOrNull();
            if (shard != null) {
                // TODO: threading
                if (dfsShardOperationRequest.shardId() == shard.id()) {
                    DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                    searchServiceTransportAction.sendExecuteDfs(node, dfsShardOperationRequest, new SearchServiceListener<DfsSearchResult>() {

                        @Override
                        public void onResult(DfsSearchResult result) {
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
    }
}
