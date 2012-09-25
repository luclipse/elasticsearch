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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.poc.*;
import org.elasticsearch.action.search.type.TransportSearchCache;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.action.search.type.TransportSearchHelper.internalSearchRequest;

/**
 * @author Martijn van Groningen
 */
public class QueryAction implements DistributedAction<ShardSearchRequest, QuerySearchResult> {

    private final QueryShardOperation queryShardOperation;
    private final Map<SearchShardTarget, QuerySearchResultProvider> queryResults;

    private DistributedContext context;
    private DistributedActionListener distributedActionListener;
    private AtomicInteger shardResultCounter;

    public QueryAction(QueryShardOperation queryShardOperation, TransportSearchCache searchCache) {
        this.queryShardOperation = queryShardOperation;
        queryResults = searchCache.obtainQueryResults();
    }

    @Override
    public Tuple<ActionListener, List<ShardSearchRequest>> prepare(DistributedContext context, SearchRequest request, DistributedActionListener listener) {
        this.context = context;
        this.distributedActionListener = listener;

        int shardsSize = context.searchShards().size();
        shardResultCounter = new AtomicInteger(shardsSize);

        ActionListener<QuerySearchResult> shardListener = new ActionListener<QuerySearchResult>() {

            @Override
            public void onResponse(QuerySearchResult result) {
                handleShardResult(result);
            }

            @Override
            public void onFailure(Throwable t) {
                distributedActionListener.onFailure(t);
            }

        };

        List<ShardSearchRequest> shardRequests = newArrayList();
        for (final ShardIterator shardIt : context.searchShards()) {
            final ShardRouting shard = shardIt.firstOrNull();
            if (shard != null) {
                ShardSearchRequest shardRequest = new ShardSearchRequest();
                SearchShardTarget target = new SearchShardTarget(shard.currentNodeId(), shard.index(), shard.shardId().id());
                if (context.searchShardTargetToSearchContext().containsKey(target)) {
                    long id = context.searchShardTargetToSearchContext().get(target);
                    shardRequest.setRequest1(new QuerySearchRequest(id, context.aggregateDfs()));
                } else {
                    InternalSearchRequest internalRequest = internalSearchRequest(
                            shard, shardsSize, request, context.getFilteringAliases(shard.index()), context.startTime()
                    );
                    shardRequest.setRequest2(internalRequest);
                }
                shardRequests.add(shardRequest);
            }
        }

        return new Tuple<ActionListener, List<ShardSearchRequest>>(shardListener, shardRequests);
    }

    @Override
    public ShardOperation<ShardSearchRequest, QuerySearchResult> getShardOperation() {
        return queryShardOperation;
    }

    @Override
    public void handleShardResult(QuerySearchResult result) {
        queryResults.put(result.shardTarget(), result);
        if (shardResultCounter.decrementAndGet() == 0) {

        }
    }

    @Override
    public void enrich(IntermediateResult intermediateResult) {
    }
}
