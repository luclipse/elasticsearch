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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.poc.*;
import org.elasticsearch.action.search.type.TransportSearchCache;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.action.search.type.TransportSearchHelper.internalSearchRequest;

/**
 * @author Martijn van Groningen
 */
public class DfsAction implements DistributedAction<InternalSearchRequest, DfsSearchResult> {

    private final DfsShardOperation shardOperation;
    private final TransportSearchCache searchCache;
    private final SearchPhaseController searchPhaseController;

    private DistributedContext context;
    private DistributedActionListener distributedActionListener;
    private Collection<DfsSearchResult> dfsShardResults;
    private AtomicInteger shardResultCounter;

    private /*volatile*/ AggregatedDfs endResult;

    public DfsAction(DfsShardOperation shardOperation, TransportSearchCache searchCache, SearchPhaseController searchPhaseController) {
        this.shardOperation = shardOperation;
        this.searchCache = searchCache;
        this.searchPhaseController = searchPhaseController;
    }

    @Override
    public Tuple<ActionListener, List<InternalSearchRequest>> prepare(DistributedContext context, SearchRequest searchRequest, DistributedActionListener listener) {
        this.context = context;
        this.distributedActionListener = listener;

        int shardsSize = context.searchShards().size();
        shardResultCounter = new AtomicInteger(shardsSize);
        dfsShardResults = searchCache.obtainDfsResults();
        ActionListener<DfsSearchResult> shardListener = new ActionListener<DfsSearchResult>() {

            @Override
            public void onResponse(DfsSearchResult result) {
                handleShardResult(result);
            }

            @Override
            public void onFailure(Throwable t) {
                distributedActionListener.onFailure(t);
            }

        };
        List<InternalSearchRequest> shardRequests = newArrayList();
        for (final ShardIterator shardIt : context.searchShards()) {
            final ShardRouting shard = shardIt.firstOrNull();
            if (shard != null) {
                InternalSearchRequest internalRequest = internalSearchRequest(
                        shard, shardsSize, searchRequest, context.getFilteringAliases(shard.index()), context.startTime()
                );
                shardRequests.add(internalRequest);
            }
        }
        return new Tuple<ActionListener, List<InternalSearchRequest>>(shardListener, shardRequests);
    }

    @Override
    public ShardOperation<InternalSearchRequest, DfsSearchResult> getShardOperation() {
        return shardOperation;
    }

    @Override
    public void handleShardResult(DfsSearchResult result) {
        dfsShardResults.add(result);
        if (shardResultCounter.decrementAndGet() == 0) {
            Map<SearchShardTarget, Long> sstToParsedSearchContext = newHashMap();
            for (DfsSearchResult dfsShardResult : dfsShardResults) {
                sstToParsedSearchContext.put(dfsShardResult.shardTarget(), dfsShardResult.id());
            }
            context.setSearchShardTargetToSearchContext(sstToParsedSearchContext);
            endResult = searchPhaseController.aggregateDfs(dfsShardResults);
            context.setAggregateDfs(endResult);
            distributedActionListener.onFinish(endResult);
        }
    }

    @Override
    public void enrich(IntermediateResult intermediateResult) {
    }

}