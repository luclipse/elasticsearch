/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.cluster.allocation;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class AwarenessAllocationTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(AwarenessAllocationTests.class);

    @AfterMethod
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    @Test
    public void testSimpleAwareness() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.schedule", "10ms")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();


        logger.info("--> starting 2 nodes on the same rack");
        startNode("node1", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));
        startNode("node2", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));

        client("node1").admin().indices().prepareCreate("test1").execute().actionGet();
        client("node1").admin().indices().prepareCreate("test2").execute().actionGet();

        ClusterHealthResponse health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> starting 1 node on a different rack");
        startNode("node3", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2"));

        Thread.sleep(500);

        health = client("node1").admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("3").setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(health.timedOut(), equalTo(false));

        ClusterState clusterState = client("node1").admin().cluster().prepareState().execute().actionGet().state();
        //System.out.println(clusterState.routingTable().prettyPrint());
        // verify that we have 10 shards on node3
        TObjectIntHashMap<String> counts = new TObjectIntHashMap<String>();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    counts.adjustOrPutValue(clusterState.nodes().get(shardRouting.currentNodeId()).name(), 1, 1);
                }
            }
        }
        assertThat(counts.get("node3"), equalTo(10));
    }

    @Test
    public void testNprShardAllocation() throws Exception {
        Settings commonSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.routing.schedule", "10ms")
                .put("index.number_of_shards", 6)
                .put("index.number_of_replicas", 1)
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build();


        logger.info("--> starting 6 nodes on three different racks. Each rack has two nodes");
        startNode("node1", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));
        startNode("node2", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_1"));
        startNode("node3", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2"));
        startNode("node4", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_2"));
        startNode("node5", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_3"));
        startNode("node6", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_3"));

        Client client = client("node1");
        client.admin().indices().prepareCreate("test").execute().actionGet();

        ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(health.timedOut(), equalTo(false));

        logger.info("--> Indexing some docs");

        long numDocs = 100;
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex("test", "type", Integer.toString(i)).setSource("field", "value").execute().actionGet();
        }
        client.admin().indices().prepareRefresh("test").execute().actionGet();
        CountResponse response = client.prepareCount("test")
                .setQuery(QueryBuilders.termQuery("field", "value"))
                .execute().actionGet();
        assertThat(response.count(), equalTo(numDocs));

        closeNode("node5");
        closeNode("node6");
        health = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(health.timedOut(), equalTo(false));

        response = client.prepareCount("test")
                .setQuery(QueryBuilders.termQuery("field", "value"))
                .execute().actionGet();
        assertThat(response.count(), equalTo(numDocs));

        startNode("node5", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_3"));
        startNode("node6", ImmutableSettings.settingsBuilder().put(commonSettings).put("node.rack_id", "rack_3"));

        health = client.admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .setWaitForGreenStatus().execute().actionGet();
        assertThat(health.status(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(health.timedOut(), equalTo(false));

        response = client.prepareCount("test")
                .setQuery(QueryBuilders.termQuery("field", "value"))
                .execute().actionGet();
        assertThat(response.count(), equalTo(numDocs));

        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().actionGet();
        ClusterState clusterState = clusterStateResponse.state();
        System.out.println(clusterState.routingNodes().prettyPrint());
        System.out.println(clusterState.routingTable().prettyPrint());

        Map<String, List<String>> rackToNodes = newHashMap();
        for (DiscoveryNode node : clusterState.nodes()) {
            String rackId = node.attributes().get("rack_id");
            if (!rackToNodes.containsKey(rackId)) {
                rackToNodes.put(rackId, new ArrayList<String>());
            }
            rackToNodes.get(rackId).add(node.getId());
        }

        for (String rackId : rackToNodes.keySet()) {
            Set<Integer> shardIds = newHashSet();
            for (String nodeId : rackToNodes.get(rackId)) {
                RoutingNode routingNode = clusterState.getRoutingNodes().nodesToShards().get(nodeId);
                if (routingNode == null) {
                    System.out.printf("rack_id=%s node_id=%s no shards allocated\n", rackId, nodeId);
                } else {
                    for (MutableShardRouting shardRouting : routingNode) {
                        int shardId = shardRouting.shardId().getId();
                        assertThat(shardIds.contains(shardId), equalTo(false));
                        System.out.printf("rack_id=%s node_id=%s shard_id=%d\n", rackId, nodeId, shardId);
                        shardIds.add(shardId);
                    }
                }

            }
            System.out.print("\n");
        }
    }

}
