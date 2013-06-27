/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.percolator.Percolator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportPercolateAction extends TransportBroadcastOperationAction<PercolateRequest, PercolateResponse, PercolateShardRequest, PercolateShardResponse> {

    private final Percolator percolator;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                    TransportService transportService, Percolator percolator) {
        super(settings, threadPool, clusterService, transportService);
        this.percolator = percolator;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected PercolateRequest newRequest() {
        return new PercolateRequest();
    }

    @Override
    protected String transportAction() {
        return PercolateAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PercolateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected PercolateResponse newResponse(PercolateRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;

        List<String[]> shardResults = null;
        List<ShardOperationFailedException> shardFailures = null;

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                PercolateShardResponse percolateShardResponse = (PercolateShardResponse) shardResponse;
                if (shardResults == null) {
                    shardResults = new ArrayList<String[]>();
                }
                shardResults.add(percolateShardResponse.matches());
                successfulShards++;
            }
        }

        if (shardResults == null) {
            return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
        }

        int size = 0;
        for (String[] shardResult : shardResults) {
            size += shardResult.length;
        }
        String[] finalMatches = new String[size];
        int offset = 0;
        for (String[] shardResult : shardResults) {
            System.arraycopy(shardResult, 0, finalMatches, offset, shardResult.length);
            offset += shardResult.length;
        }
        // TODO: Remove! (make the test pass, which is based on ordering)
        Arrays.sort(finalMatches);

        return new PercolateResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, finalMatches);
    }

    @Override
    protected PercolateShardRequest newShardRequest() {
        return new PercolateShardRequest();
    }

    @Override
    protected PercolateShardRequest newShardRequest(ShardRouting shard, PercolateRequest request) {
        return new PercolateShardRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected PercolateShardResponse newShardResponse() {
        return new PercolateShardResponse();
    }

    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, PercolateRequest request, String[] concreteIndices) {
        if (request.documentIndex() != null) {
            request.documentIndex(clusterState.metaData().concreteIndex(request.documentIndex()));
        }
        Map<String, Set<String>> routingMap = clusterState.metaData().resolveSearchRouting(request.routing(), request.indices());
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), concreteIndices, routingMap, request.preference());
    }

    @Override
    protected PercolateShardResponse shardOperation(PercolateShardRequest request) throws ElasticSearchException {
        Percolator.Response response = percolator.percolate(
                new Percolator.Request(
                        request.index(), request.documentIndex(), request.documentType(), request.shardId(),
                        request.documentSource()
                )
        );
        return new PercolateShardResponse(response.matches(), request.index(), request.shardId());
    }

}
