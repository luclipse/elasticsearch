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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.store.TransportShardActive;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class ElectPrimaryService extends AbstractComponent {

    private final TransportShardActive transportShardActive;
    private final ClusterService clusterService;
    private final String consistency;

    public ElectPrimaryService(Settings settings, TransportShardActive transportShardActive, ClusterService clusterService) {
        super(settings);
        this.transportShardActive = transportShardActive;
        this.clusterService = clusterService;
        this.consistency = settings.get("consistency", "quorum");
    }

    public boolean electUnassignedPrimaryShards(ClusterState state) {
        RoutingNodes nodes = state.routingNodes();
        RoutingNodes.UnassignedShards unassignedShards = nodes.unassigned();
        for (final MutableShardRouting unassignedShard : unassignedShards) {
            if (!unassignedShard.primary()) {
                continue;
            }

            List<MutableShardRouting> candidatePrimaryShards = new ArrayList<>();
            for (MutableShardRouting shardRouting : nodes.assignedShards(unassignedShard)) {
                if (!shardRouting.primary() && shardRouting.active()) {
                    candidatePrimaryShards.add(shardRouting);
                }
            }

            final int requiredSize;
            if (candidatePrimaryShards.size() == 1) {
                MutableShardRouting candidate = candidatePrimaryShards.get(0);
                swapPrimary(nodes, unassignedShard, candidate);
                return true;
            } else if (candidatePrimaryShards.size() == 2) {
                requiredSize = 2;
            } else {
                requiredSize = (candidatePrimaryShards.size() / 2) + 1;
            }


            transportShardActive.shardActiveCount(state, unassignedShard.shardId(), candidatePrimaryShards, new ActionListener<TransportShardActive.Result>() {
                @Override
                public void onResponse(TransportShardActive.Result result) {
                    if (result.getActiveShards() >= requiredSize) {
                        clusterService.submitStateUpdateTask("primary_election", Priority.IMMEDIATE, new ClusterStateUpdateTask() {

                            @Override
                            public ClusterState execute(ClusterState currentState) throws Exception {
                                RoutingNodes nodes = currentState.routingNodes();
                                RoutingNodes.UnassignedShards unassignedShards = nodes.unassigned();

                                MutableShardRouting unassignedPrimary = null;
                                for (MutableShardRouting shard : unassignedShards) {
                                    if (shard.shardId().equals(unassignedShard.shardId()) && shard.primary()) {
                                        unassignedPrimary = shard;
                                        break;
                                    }
                                }

                                if (unassignedPrimary == null) {
                                    return currentState;
                                }

                                // TODO: do something better here with the shard active result...
                                List<MutableShardRouting> candidatePrimaryShards = new ArrayList<>();
                                for (MutableShardRouting shardRouting : nodes.assignedShards(unassignedShard)) {
                                    if (!shardRouting.primary() && shardRouting.active()) {
                                        candidatePrimaryShards.add(shardRouting);
                                    }
                                }

                                RoutingNodes routingNodes = currentState.routingNodes();
                                swapPrimary(routingNodes, unassignedPrimary, candidatePrimaryShards.get(0));
                                return ClusterState.builder(currentState)
                                        .routingTable(new RoutingTable.Builder()
                                                        .updateNodes(routingNodes)
                                                        .build()
                                                        .validateRaiseException(currentState.metaData())
                                        ).build();
                            }

                            @Override
                            public void onFailure(String source, @Nullable Throwable t) {
                                logger.warn("[{}] Updating cluster state for primary election failed", t, source);
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.debug("Failed to send shard active request for primary election", e);
                }
            });
        }

        return false;
    }

    private void swapPrimary(RoutingNodes nodes, MutableShardRouting unassignedPrimary, MutableShardRouting newPrimary) {
        nodes.swapPrimaryFlag(unassignedPrimary, newPrimary);
        if (newPrimary.relocatingNodeId() != null) {
            // its also relocating, make sure to move the other routing to primary
            RoutingNode node = nodes.node(newPrimary.relocatingNodeId());
            if (node != null) {
                for (MutableShardRouting shardRouting : node) {
                    if (shardRouting.shardId().equals(newPrimary.shardId()) && !shardRouting.primary()) {
                        nodes.swapPrimaryFlag(shardRouting);
                        break;
                    }
                }
            }
        }
    }

}
