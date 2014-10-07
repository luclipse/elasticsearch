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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.store.TransportShardActive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ElectPrimaryService extends AbstractComponent {

    private final TransportShardActive transportShardActive;
    private final ClusterService clusterService;
    private final String consistency;

    @Inject
    public ElectPrimaryService(Settings settings, TransportShardActive transportShardActive, ClusterService clusterService) {
        super(settings);
        this.transportShardActive = transportShardActive;
        this.clusterService = clusterService;
        this.consistency = settings.get("consistency", "quorum");
    }

    public boolean electUnassignedPrimaryShards(RoutingNodes routingNodes, ClusterState state) {
        Iterator<MutableShardRouting> unassignedShards = routingNodes.unassigned().iterator();
        while (unassignedShards.hasNext()) {
            final MutableShardRouting unassignedShard = unassignedShards.next();
            if (!unassignedShard.primary()) {
                continue;
            }

            List<MutableShardRouting> candidatePrimaryShards = new ArrayList<>();
            for (MutableShardRouting shardRouting : routingNodes.assignedShards(unassignedShard)) {
                if (!shardRouting.primary() && shardRouting.active()) {
                    candidatePrimaryShards.add(shardRouting);
                }
            }
            int activeReplicas = candidatePrimaryShards.size();

            final int requiredSize;
            if (activeReplicas == 0) {
                logger.info("No active replicas");
                return false;
            } else if (activeReplicas == 1) {
                MutableShardRouting candidate = candidatePrimaryShards.get(0);
                logger.info("Just one active replica {}", candidate);
                return swapPrimary(routingNodes, unassignedShard, candidate);
            } else if (activeReplicas == 2) {
                requiredSize = 2;
            } else {
                requiredSize = (candidatePrimaryShards.size() / 2) + 1;
            }

            // Ignore the primary shard for the rest of the allocation,
            // in the primary_election cluster update task a new primary will get elected
            routingNodes.ignoredUnassigned().add(unassignedShard);
            unassignedShards.remove();

            logger.info("Electing a primary for shard group {} based on the following candidates [{}]", unassignedShard.shardId(), candidatePrimaryShards);
            transportShardActive.shardActiveCount(state, unassignedShard.shardId(), candidatePrimaryShards, new ActionListener<TransportShardActive.Result>() {
                @Override
                public void onResponse(final TransportShardActive.Result result) {
                    if (result.getActiveShards() < requiredSize) {
                        logger.warn("Not electing primary for shard group {}, require [{}], but found [{}]", unassignedShard.shardId(), result.getActiveShards(), requiredSize);
                        return;
                    }

                    logger.info("Electing primary for shard group {}, required [{}] and found [{}]", unassignedShard.shardId(), requiredSize, result.getActiveShards());
                    clusterService.submitStateUpdateTask("primary_election", Priority.IMMEDIATE, new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            RoutingNodes routingNodes = currentState.routingNodes();
                            RoutingNodes.UnassignedShards unassignedShards = routingNodes.unassigned();

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

                            long lowestTimeElapsedSinceLastWrite = Long.MAX_VALUE;
                            MutableShardRouting newPrimaryShard = null;
                            for (MutableShardRouting shardRouting : routingNodes.assignedShards(unassignedShard)) {
                                if (!shardRouting.primary() && shardRouting.active()) {
                                    Long timeElapsedSinceLastWrite = result.getTimeElapsedSinceLastWritePerNode().get(shardRouting.currentNodeId());
                                    if (timeElapsedSinceLastWrite != null) {
                                        if (timeElapsedSinceLastWrite < lowestTimeElapsedSinceLastWrite) {
                                            lowestTimeElapsedSinceLastWrite = timeElapsedSinceLastWrite;
                                            newPrimaryShard = shardRouting;
                                            logger.info("Shard {} is the best candidate, because time elapsed since last write is {}", newPrimaryShard, lowestTimeElapsedSinceLastWrite);
                                        }
                                    }
                                }
                            }

                            assert newPrimaryShard != null;
                            logger.info("Chose {} as the new primary shard", newPrimaryShard);

                            swapPrimary(routingNodes, unassignedPrimary, newPrimaryShard);

                            RoutingTable routingTable = new RoutingTable.Builder().updateNodes(routingNodes).build().validateRaiseException(currentState.metaData());
                            return ClusterState.builder(currentState).routingTable(routingTable).build();
                        }

                        @Override
                        public void onFailure(String source, @Nullable Throwable t) {
                            logger.warn("[{}] Updating cluster state for primary election failed", t, source);
                        }
                    });
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.debug("Failed to send shard active request for primary election", e);
                }
            });
        }

        return false;
    }

    private boolean swapPrimary(RoutingNodes nodes, MutableShardRouting unassignedPrimary, MutableShardRouting newPrimary) {
        logger.info("Swapping primary flag from {} to {}", unassignedPrimary, newPrimary);
        nodes.swapPrimaryFlag(unassignedPrimary, newPrimary);
        boolean b = false;
        if (newPrimary.relocatingNodeId() != null) {
            b = true;
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
        return b;
    }

}
