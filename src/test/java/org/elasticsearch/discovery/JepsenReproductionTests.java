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

package org.elasticsearch.discovery;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisconnectPartition;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportModule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests designed to reproduce Jepsen tests: https://github.com/aphyr/jepsen/
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class JepsenReproductionTests extends ElasticsearchIntegrationTest {

    private static final Settings getNodeSettings(int offset, int port, String unicastList) {
        return ImmutableSettings.settingsBuilder()
                // name the node
                .put("node.name", "node_" + offset)
                // To override the local setting if set externally
                .put("discovery.type", "zen")
                // required for unicast to work
                .put("node.mode", "network")
                // Jepsen sets this
                .put("discovery.zen.fd.ping_timeout", "3s")
                // Jepsen sets this
                .put("discovery.zen.minimum_master_nodes", 3)
                // disable multicast
                .put("discovery.zen.ping.multicast.enabled", false)
                // Use unicast
                .put("discovery.zen.ping.unicast.hosts", unicastList)
                // Need to use custom tcp port range otherwise we collide with the shared cluster
                .put("transport.tcp.port", port)
                // force no rejoining
                //.put("discovery.zen.rejoin_on_master_gone", false)
                // Mock the transport layer so partitioning can happen
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .build();
    }

    private static final AtomicInteger docCount = new AtomicInteger(0);
    private static final AtomicInteger successfullDocs = new AtomicInteger(0);
    private static final AtomicInteger timedOutDocs = new AtomicInteger(0);
    private static final AtomicInteger failedDocs = new AtomicInteger(0);
    private static final Set<String> successIds = Collections.synchronizedSet(new HashSet<String>());

    @Override
    protected int numberOfShards() {
        // Unset by Jepsen, so defaults to 5
        return 5;
    }

    @Override
    protected int numberOfReplicas() {
        // Jepsen sets this to 4 shards
        return 4;
    }

    @Test
    @TestLogging("discovery.zen:DEBUG,action.index:TRACE,cluster.service:TRACE,indices.recovery:TRACE")
    public void jepsenCreateTest() throws Exception {
        final int portOffset = randomIntBetween(25000, 35000);
        int n0port = portOffset + 0;
        int n1port = portOffset + 1;
        int n2port = portOffset + 2;
        int n3port = portOffset + 3;
        int n4port = portOffset + 4;

        // Build a unicast list from each node's port
        StringBuilder unicastSB = new StringBuilder();
        unicastSB.append("localhost:" + n0port + ",");
        unicastSB.append("localhost:" + n1port + ",");
        unicastSB.append("localhost:" + n2port + ",");
        unicastSB.append("localhost:" + n3port + ",");
        unicastSB.append("localhost:" + n4port);
        String unicastString = unicastSB.toString();
        logger.info("--> Unicast setting: {}", unicastString);

        Settings n0settings = getNodeSettings(0, n0port, unicastString);
        Settings n1settings = getNodeSettings(1, n1port, unicastString);
        Settings n2settings = getNodeSettings(2, n2port, unicastString);
        Settings n3settings = getNodeSettings(3, n3port, unicastString);
        Settings n4settings = getNodeSettings(4, n4port, unicastString);

        ImmutableList<String> nodes = ImmutableList.copyOf(internalCluster().startNodesAsync(
                n0settings,
                n1settings,
                n2settings,
                n3settings,
                n4settings).get());

        logger.info("--> nodes: " + nodes);
        String node0 = "node_0";
        String node1 = "node_1";
        String node2 = "node_2";
        String node3 = "node_3";
        String node4 = "node_4";

        // Wait until 5 nodes are part of the cluster
        ensureStableCluster(5);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("--> master node: " + masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonIntersectingNodes = new HashSet<>(nodes);
        // Pick a non-master to be the intersecting node
        Set<String> nonMasterNodes = new HashSet<>(nodes);
        nonMasterNodes.remove(masterNode);
        String intersectingNode = randomFrom(nonMasterNodes.toArray(Strings.EMPTY_ARRAY));
        logger.info("--> intersecting node: " + intersectingNode);
        nonIntersectingNodes.remove(intersectingNode);

        // Now we're going to generate two sides of the cluster
        nonIntersectingNodes.remove(intersectingNode);
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>();

        // Generate two sides of the cluster, with 2 nodes each
        String s1n1 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s1n1);
        String s1n2 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s1n2);
        String s2n1 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s2n1);
        String s2n2 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s2n2);
        side1.add(s1n1);
        side1.add(s1n2);

        side2.add(s2n1);
        side2.add(s2n2);

        logger.info("--> creating index 'test'");
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("number")
                .startObject("properties")
                .startObject("num")
                .field("type", "integer")
                .field("store", "true")
                .endObject()
                .endObject()
                .endObject().endObject();
        prepareCreate("test").addMapping("number", mapping).get();
        ensureGreen("test");

        OperationProcessor op0 = new OperationProcessor(node0);
        Thread t0 = new Thread(op0);
        OperationProcessor op1 = new OperationProcessor(node1);
        Thread t1 = new Thread(op1);
        OperationProcessor op2 = new OperationProcessor(node2);
        Thread t2 = new Thread(op2);
        OperationProcessor op3 = new OperationProcessor(node3);
        Thread t3 = new Thread(op3);
        OperationProcessor op4 = new OperationProcessor(node4);
        Thread t4 = new Thread(op4);
        logger.info("--> starting processors...");
        t0.start();
        t1.start();
        t2.start();
        t3.start();
        t4.start();

        logger.info("--> side1: " + intersectingNode + " + " + side1);
        logger.info("--> side2: " + intersectingNode + " + " + side2);

        // Simulate the disconnection
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(side1, side2, getRandom());
        setDisruptionScheme(networkDisconnect);
        logger.info("--> starting partitioning...");
        networkDisconnect.startDisrupting();
        logger.info("--> partition forced");

        // Wait 30 seconds
        Thread.sleep(30000);

        // Everyone should see at *least* 3 nodes
        side1.add(intersectingNode);
        side2.add(intersectingNode);
        logger.info("--> checking node visibility, intersecting node: {}, side1: {}, side2: {}", intersectingNode, side1, side2);
        for (String node : nodes) {
            logger.info("--> {} should see at least 3 nodes", node);
            ClusterState state = getNodeClusterState(node);
            for (DiscoveryNode n : state.nodes()) {
                logger.info("--> {} sees {}", node, n.getName());
            }
            logger.info("--> {} sees {} nodes", node, state.nodes().size());
            //assertThat(node + " should see at least 3 nodes", state.nodes().size(), greaterThanOrEqualTo(3));
        }

        // See if we're in a split-brain scenario
        Set<String> masters = new HashSet<>();
        for (String n : nodes) {
            ClusterState state = getNodeClusterState(n);
            if (state.getNodes().getMasterNode() != null) {
                logger.info("--> {} says 'I think the master node is {}'", n, state.getNodes().getMasterNode().getName());
            } else {
                logger.info("--> {} says 'I don't know who the master is'", n);
            }
            masters.add(state.getNodes().getMasterNodeId());
        }
        masters.remove(null);
        logger.info("There are currently [" + masters.size() + "] master nodes in the cluster: " + masters);

        // Wait 200 seconds
        Thread.sleep(50000);

        logger.info("--> healing partition...");
        networkDisconnect.stopDisrupting();
        logger.info("--> partition stopped");

        // Wait 10 seconds
        Thread.sleep(10000);

        logger.info("--> stopping processors");
        op0.run = false;
        op1.run = false;
        op2.run = false;
        op3.run = false;
        op4.run = false;
        t0.join(10000);
        t1.join(10000);
        t2.join(10000);
        t3.join(10000);
        t4.join(10000);

        // wait for the cluster to be green
        ensureGreen();

        logger.info("--> checking node visibility, intersecting node: {}, side1: {}, side2: {}", intersectingNode, side1, side2);
        for (String node : nodes) {
            ClusterState state = getNodeClusterState(node);
            List<String> seenNodes = newArrayList();
            for (DiscoveryNode n : state.nodes()) {
                seenNodes.add(n.getName());
            }
            logger.info("--> {} sees {} nodes: {}", node, state.nodes().size(), seenNodes);
        }

        logger.info("--> indexed {} documents in total", docCount.get());
        logger.info("--> FINAL STATS - attempts: {}, success: {}, failed: {}, timed out: {}", docCount.get(), successfullDocs.get(), failedDocs.get(), timedOutDocs.get());
        assertThat("each indexed document should be successfully indexed", docCount.get(), equalTo(successfullDocs.get() + timedOutDocs.get()));

        refresh();
        int numDocs = docCount.get() * 2;
        Set<String> fetchedIds = newHashSet();
        SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(numDocs).get();
        for (SearchHit hit : resp.getHits().getHits()) {
            fetchedIds.add(hit.getId());
        }

        if (successfullDocs.get() != successIds.size()) {
            logger.info("--> expected {} successful docs, but {} actually succeeded [diff:{}] (some timed out docs were successful)",
                    successfullDocs.get(), successIds.size(), successIds.size() - successfullDocs.get());
        }

        // Check for documents that are part of the successful ids but not returned from the query
        Set<String> missingIds = newHashSet();
        missingIds.addAll(successIds);
        missingIds.removeAll(fetchedIds);
        assertThat("ids that were marked as successful but are missing: " + missingIds, missingIds.size(), equalTo(0));

        // Check for documents that were returned from the query but were not marked as successful
        Set<String> extraIds = newHashSet();
        extraIds.addAll(fetchedIds);
        extraIds.removeAll(successIds);
        assertThat("ids that were NOT marked as successful but actually exist: " + extraIds, extraIds.size(), equalTo(0));

        // The number of ids that we fetched should be the same as the number
        // of ids that were successful from the perspective of the processors
        assertThat("queried ids should match fetched:" + fetchedIds.size() + " expected successful: " + successIds.size() +
                        " missing: " + Math.abs(fetchedIds.size() - successIds.size()),
                fetchedIds.size(), equalTo(successIds.size()));
    }

    /**
     * Like the jepsenCreateTest, but instead of only partitioning once, it
     * randomly partitions and heals constantly for an amount of time
     * @throws Exception
     */
    @Test
    @TestLogging("discovery.zen:DEBUG,action.index:TRACE")
    public void muchMoreEvilJepsenCreateTest() throws Exception {
        final int portOffset = randomIntBetween(25000, 35000);
        int n0port = portOffset + 0;
        int n1port = portOffset + 1;
        int n2port = portOffset + 2;
        int n3port = portOffset + 3;
        int n4port = portOffset + 4;

        // Build a unicast list from each node's port
        StringBuilder unicastSB = new StringBuilder();
        unicastSB.append("localhost:" + n0port + ",");
        unicastSB.append("localhost:" + n1port + ",");
        unicastSB.append("localhost:" + n2port + ",");
        unicastSB.append("localhost:" + n3port + ",");
        unicastSB.append("localhost:" + n4port);
        String unicastString = unicastSB.toString();
        logger.info("--> Unicast setting: {}", unicastString);

        Settings n0settings = getNodeSettings(0, n0port, unicastString);
        Settings n1settings = getNodeSettings(1, n1port, unicastString);
        Settings n2settings = getNodeSettings(2, n2port, unicastString);
        Settings n3settings = getNodeSettings(3, n3port, unicastString);
        Settings n4settings = getNodeSettings(4, n4port, unicastString);

        ImmutableList<String> nodes = ImmutableList.copyOf(internalCluster().startNodesAsync(
                n0settings,
                n1settings,
                n2settings,
                n3settings,
                n4settings).get());

        logger.info("--> nodes: " + nodes);
        String node0 = "node_0";
        String node1 = "node_1";
        String node2 = "node_2";
        String node3 = "node_3";
        String node4 = "node_4";

        // Wait until 5 nodes are part of the cluster
        ensureStableCluster(5);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("--> master node: " + masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonIntersectingNodes = new HashSet<>(nodes);
        // Pick a non-master to be the intersecting node
        Set<String> nonMasterNodes = new HashSet<>(nodes);
        nonMasterNodes.remove(masterNode);
        String intersectingNode = randomFrom(nonMasterNodes.toArray(Strings.EMPTY_ARRAY));
        logger.info("--> intersecting node: " + intersectingNode);
        nonIntersectingNodes.remove(intersectingNode);

        // Now we're going to generate two sides of the cluster
        nonIntersectingNodes.remove(intersectingNode);
        Set<String> side1 = new HashSet<>();
        Set<String> side2 = new HashSet<>();

        // Generate two sides of the cluster, with 2 nodes each
        String s1n1 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s1n1);
        String s1n2 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s1n2);
        String s2n1 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s2n1);
        String s2n2 = randomFrom(nonIntersectingNodes.toArray(Strings.EMPTY_ARRAY));
        nonIntersectingNodes.remove(s2n2);
        side1.add(s1n1);
        side1.add(s1n2);

        side2.add(s2n1);
        side2.add(s2n2);

        logger.info("--> creating index 'test'");
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("number")
                .startObject("properties")
                .startObject("num")
                .field("type", "integer")
                .field("store", "true")
                .endObject()
                .endObject()
                .endObject().endObject();
        prepareCreate("test").addMapping("number", mapping).get();
        ensureGreen("test");

        OperationProcessor op0 = new OperationProcessor(node0);
        Thread t0 = new Thread(op0);
        OperationProcessor op1 = new OperationProcessor(node1);
        Thread t1 = new Thread(op1);
        OperationProcessor op2 = new OperationProcessor(node2);
        Thread t2 = new Thread(op2);
        OperationProcessor op3 = new OperationProcessor(node3);
        Thread t3 = new Thread(op3);
        OperationProcessor op4 = new OperationProcessor(node4);
        Thread t4 = new Thread(op4);
        logger.info("--> starting processors...");
        t0.start();
        t1.start();
        t2.start();
        t3.start();
        t4.start();

        logger.info("--> side1: " + intersectingNode + " + " + side1);
        logger.info("--> side2: " + intersectingNode + " + " + side2);

        // Simulate the disconnection
        NetworkDisconnectPartition networkDisconnect = new NetworkDisconnectPartition(side1, side2, getRandom());
        setDisruptionScheme(networkDisconnect);
        logger.info("--> starting partitioning...");
        networkDisconnect.startDisrupting();
        logger.info("--> partition forced");

        // Wait 30 seconds
        Thread.sleep(30000);

        // Everyone should see at *least* 3 nodes
        side1.add(intersectingNode);
        side2.add(intersectingNode);
        logger.info("--> checking node visibility, intersecting node: {}, side1: {}, side2: {}", intersectingNode, side1, side2);
        for (String node : nodes) {
            logger.info("--> {} should see at least 3 nodes", node);
            ClusterState state = getNodeClusterState(node);
            for (DiscoveryNode n : state.nodes()) {
                logger.info("--> {} sees {}", node, n.getName());
            }
            logger.info("--> {} sees {} nodes", node, state.nodes().size());
        }

        // See if we're in a split-brain scenario
        Set<String> masters = new HashSet<>();
        for (String n : nodes) {
            ClusterState state = getNodeClusterState(n);
            if (state.getNodes().getMasterNode() != null) {
                logger.info("--> {} says 'I think the master node is {}'", n, state.getNodes().getMasterNode().getName());
            } else {
                logger.info("--> {} says 'I don't know who the master is'", n);
            }
            masters.add(state.getNodes().getMasterNodeId());
        }
        masters.remove(null);
        logger.info("There are currently [" + masters.size() + "] master nodes in the cluster: " + masters);

        logger.info("--> starting random cluster partitioning");
        long startTime = System.currentTimeMillis();
        boolean disrupted = true;
        while (true) {
            // Do this for at least this long
            if ((System.currentTimeMillis() - startTime) > 200000) {
                break;
            }
            // Sleep a random about of time
            stagger(15000);
            if (disrupted) {
                logger.info("--> network was disrupted, healing...");
                networkDisconnect.stopDisrupting();
                disrupted = false;
            } else {
                logger.info("--> network was not disrupted, disrupting...");
                networkDisconnect.startDisrupting();
                disrupted = true;
            }
        }

        if (disrupted) {
            logger.info("--> healing partition...");
            networkDisconnect.stopDisrupting();
        }
        logger.info("--> partitioning stopped");

        // Wait 10 seconds
        Thread.sleep(10000);

        logger.info("--> stopping processors");
        op0.run = false;
        op1.run = false;
        op2.run = false;
        op3.run = false;
        op4.run = false;
        t0.join(10000);
        t1.join(10000);
        t2.join(10000);
        t3.join(10000);
        t4.join(10000);

        // wait for the cluster to be green
        ensureGreen();

        logger.info("--> checking node visibility, intersecting node: {}, side1: {}, side2: {}", intersectingNode, side1, side2);
        for (String node : nodes) {
            ClusterState state = getNodeClusterState(node);
            List<String> seenNodes = newArrayList();
            for (DiscoveryNode n : state.nodes()) {
                seenNodes.add(n.getName());
            }
            logger.info("--> {} sees {} nodes: {}", node, state.nodes().size(), seenNodes);
        }

        logger.info("--> indexed {} documents in total", docCount.get());
        logger.info("--> FINAL STATS - attempts: {}, success: {}, failed: {}, timed out: {}", docCount.get(), successfullDocs.get(), failedDocs.get(), timedOutDocs.get());
        assertThat("each indexed document should be successfully indexed", docCount.get(), equalTo(successfullDocs.get() + timedOutDocs.get()));

        refresh();
        int numDocs = docCount.get() * 2;
        Set<String> fetchedIds = newHashSet();
        SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(numDocs).get();
        for (SearchHit hit : resp.getHits().getHits()) {
            fetchedIds.add(hit.getId());
        }

        if (successfullDocs.get() != successIds.size()) {
            logger.info("--> expected {} successful docs, but {} actually succeeded [diff:{}] (some timed out docs were successful)",
                    successfullDocs.get(), successIds.size(), successIds.size() - successfullDocs.get());
        }

        // Check for documents that are part of the successful ids but not returned from the query
        Set<String> missingIds = newHashSet();
        missingIds.addAll(successIds);
        missingIds.removeAll(fetchedIds);
        assertThat("ids that were marked as successful but are missing: " + missingIds, missingIds.size(), equalTo(0));

        // Check for documents that were returned from the query but were not marked as successful
        Set<String> extraIds = newHashSet();
        extraIds.addAll(fetchedIds);
        extraIds.removeAll(successIds);
        assertThat("ids that were NOT marked as successful but actually exist: " + extraIds, extraIds.size(), equalTo(0));

        // The number of ids that we fetched should be the same as the number
        // of ids that were successful from the perspective of the processors
        assertThat("queried ids should match fetched:" + fetchedIds.size() + " expected successful: " + successIds.size() +
                        " missing: " + Math.abs(fetchedIds.size() - successIds.size()),
                fetchedIds.size(), equalTo(successIds.size()));
    }

    private ClusterState getNodeClusterState(String node) {
        return client(node).admin().cluster().prepareState().setLocal(true).get().getState();
    }

    private void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), null);
    }

    private void ensureStableCluster(int nodeCount, TimeValue timeValue) {
        ensureStableCluster(nodeCount, timeValue, null);
    }

    private void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), viaNode);
    }

    private void ensureStableCluster(int nodeCount, TimeValue timeValue, @Nullable String viaNode) {
        logger.info("ensuring cluster is stable with [{}] nodes. access node: [{}]. timeout: [{}]", nodeCount, viaNode, timeValue);
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes(Integer.toString(nodeCount))
                .setTimeout(timeValue)
                .setWaitForRelocatingShards(0)
                .get();
        assertThat("Cluster health should not time out via " + viaNode, clusterHealthResponse.isTimedOut(), is(false));
    }

    void stagger(int max) {
        try {
            Thread.sleep(randomIntBetween(0, max));
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public class OperationProcessor implements Runnable {
        public volatile boolean run = true;

        private final String node;
        private final ExecutorService service = Executors.newSingleThreadExecutor();

        public OperationProcessor(String node) {
            this.node = node;
        }

        @Override
        public void run() {
            while (run) {
                // Delay for 0 - 200 ms
                stagger(200);
                final int id = docCount.incrementAndGet();
                Callable<Boolean> call = new Callable<Boolean>() {
                    @Override
                    public Boolean call() {
                        try {
                            long start = System.currentTimeMillis();
                            IndexResponse resp = client(node).prepareIndex("test", "number")
                                    .setSource("{\"num\": " + id + "}")
                                    .setTimeout(TimeValue.timeValueSeconds(5000))
                                    .get();
                            // TODO: Move to a helper method in test cluster:
                            int shard = Math.abs(((InternalTestCluster) cluster()).getInstance(DjbHashFunction.class).hash(resp.getId()) % 5);
                            logger.info("--> {} indexed document {} into shard {} in {}ms", node, resp.getId(), shard, System.currentTimeMillis() - start);
                            successIds.add(resp.getId());
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }
                };

                // Uggggh java timeouts
                FutureTask future = new FutureTask(call);
                service.execute(future);
                try {
                    if ((Boolean) future.get(5000, TimeUnit.MILLISECONDS)) {
                        successfullDocs.incrementAndGet();
                    } else {
                        failedDocs.incrementAndGet();
                        logger.info("--> indexing unsuccessful");
                    }
                    // Jepsen adds a 1 second sleep after a write
                    Thread.sleep(1000);
                } catch (TimeoutException te) {
                    timedOutDocs.incrementAndGet();
                    logger.info("--> {} operation #{} timed out after 5 seconds", node, id);
                } catch (Exception te) {
                    logger.info("--> {} operation #{} had an error", node, id);
                }
            }
        }
    }
}
