/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.indices.store;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransportShardActive extends AbstractComponent {

    public static final String ACTION_SHARD_EXISTS = "internal:index/shard/exists";
    private static final EnumSet<IndexShardState> ACTIVE_STATES = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    @Inject
    public TransportShardActive(Settings settings, ClusterService clusterService, IndicesService indicesService, TransportService transportService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        transportService.registerHandler(ACTION_SHARD_EXISTS, new ShardActiveRequestHandler());
    }

    public void shardActiveCount(ClusterState state, ShardId shardId, Iterable<ShardRouting> indexShardRoutingTable, ActionListener<Result> listener) {
        List<Tuple<DiscoveryNode, ShardActiveRequest>> requests = new ArrayList<>(4);
        String indexUUID = state.getMetaData().index(shardId.getIndex()).getUUID();
        assert indexUUID != null;

        ClusterName clusterName = state.getClusterName();
        for (ShardRouting shardRouting : indexShardRoutingTable) {
            DiscoveryNode currentNode = state.nodes().get(shardRouting.currentNodeId());
            if (currentNode != null) {
                requests.add(new Tuple<>(currentNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId())));
            }
            if (shardRouting.relocatingNodeId() != null) {
                DiscoveryNode relocatingNode = state.nodes().get(shardRouting.relocatingNodeId());
                if (relocatingNode != null) {
                    requests.add(new Tuple<>(relocatingNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId())));
                }
            }
        }

        boolean requireShardRequestToMaster = false;
        for (Tuple<DiscoveryNode, ShardActiveRequest> tuple : requests) {
            if (tuple.v1().equals(state.getNodes().masterNode())) {
                requireShardRequestToMaster = true;
                break;
            }
        }

        if (!requireShardRequestToMaster) {
            ShardActiveRequest request = new ShardActiveRequest(clusterName);
            requests.add(new Tuple<>(state.nodes().masterNode(), request));
        }

        ShardActiveResponseHandler responseHandler = new ShardActiveResponseHandler(shardId, requests.size(), listener);
        for (Tuple<DiscoveryNode, ShardActiveRequest> request : requests) {
            DiscoveryNode target = request.v1();
            if (clusterService.localNode().equals(target)) {
                responseHandler.handleResponse(nodeOperation(request.v2(), clusterService.localNode()));
            } else {
                logger.trace("Sending shard exists request to node [{}] for shard {}", target, shardId);
                transportService.sendRequest(target, ACTION_SHARD_EXISTS, request.v2(), responseHandler);
            }
        }
    }

    private ShardActiveResponse nodeOperation(ShardActiveRequest request, DiscoveryNode localNode) {
        boolean shardActive = false;
        ClusterState state = clusterService.state();
        ClusterName thisClusterName = state.getClusterName();
        if (!thisClusterName.equals(request.clusterName)) {
            logger.trace("shard exists request meant for cluster[{}], but this is cluster[{}], ignoring request", request.clusterName, thisClusterName);
        } else if (request.checkShardActive) {
            ShardId shardId = request.shardId;
            IndexService indexService = indicesService.indexService(shardId.index().getName());
            if (indexService != null && indexService.indexUUID().equals(request.indexUUID)) {
                IndexShard indexShard = indexService.shard(shardId.getId());
                if (indexShard != null) {
                    shardActive = ACTIVE_STATES.contains(indexShard.state());
                }
            }
        }

        return new ShardActiveResponse(shardActive, state.nodes().localNodeMaster(), localNode);
    }

    public static class Result {

        private final int targetedShards;
        private final int activeShards;
        private final boolean masterVerified;

        public Result(int targetedShards, int activeShards, boolean masterVerified) {
            this.targetedShards = targetedShards;
            this.activeShards = activeShards;
            this.masterVerified = masterVerified;
        }

        public int getTargetedShards() {
            return targetedShards;
        }

        public int getActiveShards() {
            return activeShards;
        }

        public boolean isMasterVerified() {
            return masterVerified;
        }
    }

    private static class ShardActiveRequest extends TransportRequest {

        private ClusterName clusterName;
        private String indexUUID;
        private ShardId shardId;
        private boolean checkShardActive;

        ShardActiveRequest() {
        }

        ShardActiveRequest(ClusterName clusterName, String indexUUID, ShardId shardId) {
            this.shardId = shardId;
            this.indexUUID = indexUUID;
            this.clusterName = clusterName;
            this.checkShardActive = true;
        }

        private ShardActiveRequest(ClusterName clusterName) {
            this.clusterName = clusterName;
            this.indexUUID = ""; // dummy value for bwc serialization
            this.shardId = new ShardId("", 0); // dummy value for bwc serialization
            this.checkShardActive = false;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterName = ClusterName.readClusterName(in);
            indexUUID = in.readString();
            shardId = ShardId.readShardId(in);
            if (in.getVersion().before(Version.V_1_5_0)) {
                checkShardActive = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterName.writeTo(out);
            out.writeString(indexUUID);
            shardId.writeTo(out);
            if (out.getVersion().before(Version.V_1_5_0)) {
                out.writeBoolean(checkShardActive);
            }
        }
    }

    private static class ShardActiveResponse extends TransportResponse {

        private boolean shardActive;
        private DiscoveryNode node;
        private boolean masterVerified;

        private ShardActiveResponse() {
        }

        ShardActiveResponse(boolean shardActive, boolean isMaster, DiscoveryNode node) {
            this.shardActive = shardActive;
            this.masterVerified = isMaster;
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardActive = in.readBoolean();
            node = DiscoveryNode.readNode(in);
            if (in.getVersion().onOrAfter(Version.V_1_5_0)) {
                masterVerified = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(shardActive);
            node.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_1_5_0)) {
                out.writeBoolean(masterVerified);
            }
        }
    }

    private class ShardActiveRequestHandler extends BaseTransportRequestHandler<ShardActiveRequest> {

        @Override
        public ShardActiveRequest newInstance() {
            return new ShardActiveRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(ShardActiveRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(nodeOperation(request, clusterService.localNode()));
        }
    }

    private class ShardActiveResponseHandler implements TransportResponseHandler<ShardActiveResponse> {

        private final ShardId shardId;
        private final int expectedActiveCopies;
        private final ActionListener<Result> listener;

        private final AtomicInteger awaitingResponses;
        private final AtomicInteger activeCopies;
        private final AtomicBoolean masterVerified;

        public ShardActiveResponseHandler(ShardId shardId, int expectedActiveCopies, ActionListener<Result> listener) {
            this.shardId = shardId;
            this.expectedActiveCopies = expectedActiveCopies;
            this.listener = listener;
            this.awaitingResponses = new AtomicInteger(expectedActiveCopies);
            this.activeCopies = new AtomicInteger();
            this.masterVerified = new AtomicBoolean(false);
        }

        @Override
        public ShardActiveResponse newInstance() {
            return new ShardActiveResponse();
        }

        @Override
        public void handleResponse(ShardActiveResponse response) {
            if (response.shardActive) {
                logger.trace("[{}] exists on node [{}]", shardId, response.node);
                activeCopies.incrementAndGet();
            }
            if (response.masterVerified) {
                masterVerified.set(true);
            }
            countDownAndFinish();
        }

        @Override
        public void handleException(TransportException exp) {
            logger.debug("shards active request failed for {}", exp, shardId);
            countDownAndFinish();
        }

        private void countDownAndFinish() {
            if (awaitingResponses.decrementAndGet() == 0) {
                listener.onResponse(new Result(expectedActiveCopies, activeCopies.get(), masterVerified.get()));
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

    }

}
