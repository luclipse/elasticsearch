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
import org.junit.Before;
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

    private ClusterDiscoveryConfiguration discoveryConfig;

    private static final Settings DEFAULT_SETTINGS = ImmutableSettings.settingsBuilder()
            // Jepsen sets this
            .put("discovery.zen.fd.ping_timeout", "3s")
            // Jepsen sets this
            .put("discovery.zen.minimum_master_nodes", 3)
            // disable multicast
            .put("discovery.zen.ping.multicast.enabled", false)
            .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
            .put("http.enabled", false) // just to make test quicker
            .build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return discoveryConfig.node(nodeOrdinal);
    }

    @Before
    public void clearConfig() {
        discoveryConfig = null;
    }

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
    @TestLogging("discovery.zen:DEBUG,action.index:TRACE,cluster.service:TRACE,indices.recovery:TRACE,indices.store:TRACE")
    public void jepsenCreateTest() throws Exception {
        discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(5, DEFAULT_SETTINGS);
        List<String> nodes = internalCluster().startNodesAsync(5).get();

        logger.info("--> nodes: " + nodes);
        String node0 = nodes.get(0);
        String node1 = nodes.get(1);
        String node2 = nodes.get(2);
        String node3 = nodes.get(3);
        String node4 = nodes.get(4);

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

        Context context = new Context();
        OperationProcessor op0 = new OperationProcessor(node0, context);
        Thread t0 = new Thread(op0);
        OperationProcessor op1 = new OperationProcessor(node1, context);
        Thread t1 = new Thread(op1);
        OperationProcessor op2 = new OperationProcessor(node2, context);
        Thread t2 = new Thread(op2);
        OperationProcessor op3 = new OperationProcessor(node3, context);
        Thread t3 = new Thread(op3);
        OperationProcessor op4 = new OperationProcessor(node4, context);
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
        t0.join();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

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

        logger.info("--> indexed {} documents in total", context.docCount.get());
        logger.info("--> FINAL STATS - attempts: {}, success: {}, failed: {}, timed out: {}", context.docCount.get(), context.successfullDocs.get(), context.failedDocs.get(), context.timedOutDocs.get());
        assertThat("each indexed document should be successfully indexed", context.docCount.get(), equalTo(context.successfullDocs.get() + context.timedOutDocs.get()));

        refresh();
        int numDocs = context.docCount.get() * 2;
        Set<String> fetchedIds = newHashSet();
        SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(numDocs).get();
        for (SearchHit hit : resp.getHits().getHits()) {
            fetchedIds.add(hit.getId());
        }

        if (context.successfullDocs.get() != context.successIds.size()) {
            logger.info("--> expected {} successful docs, but {} actually succeeded [diff:{}] (some timed out docs were successful)",
                    context.successfullDocs.get(), context.successIds.size(), context.successIds.size() - context.successfullDocs.get());
        }

        // Check for documents that are part of the successful ids but not returned from the query
        Set<String> missingIds = newHashSet();
        missingIds.addAll(context.successIds);
        missingIds.removeAll(fetchedIds);
        assertThat("ids that were marked as successful but are missing: " + missingIds, missingIds.size(), equalTo(0));

        // Check for documents that were returned from the query but were not marked as successful
        Set<String> extraIds = newHashSet();
        extraIds.addAll(fetchedIds);
        extraIds.removeAll(context.successIds);
        assertThat("ids that were NOT marked as successful but actually exist: " + extraIds, extraIds.size(), equalTo(0));

        // The number of ids that we fetched should be the same as the number
        // of ids that were successful from the perspective of the processors
        assertThat("queried ids should match fetched:" + fetchedIds.size() + " expected successful: " + context.successIds.size() +
                        " missing: " + Math.abs(fetchedIds.size() - context.successIds.size()),
                fetchedIds.size(), equalTo(context.successIds.size()));
    }

    /**
     * Like the jepsenCreateTest, but instead of only partitioning once, it
     * randomly partitions and heals constantly for an amount of time
     * @throws Exception
     */
    @Test
    @TestLogging("discovery.zen:DEBUG,action.index:TRACE")
    public void muchMoreEvilJepsenCreateTest() throws Exception {
        discoveryConfig = new ClusterDiscoveryConfiguration.UnicastZen(5, DEFAULT_SETTINGS);
        List<String> nodes = internalCluster().startNodesAsync(5).get();

        logger.info("--> nodes: " + nodes);
        String node0 = nodes.get(0);
        String node1 = nodes.get(1);
        String node2 = nodes.get(2);
        String node3 = nodes.get(3);
        String node4 = nodes.get(4);

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

        Context context = new Context();
        OperationProcessor op0 = new OperationProcessor(node0, context);
        Thread t0 = new Thread(op0);
        OperationProcessor op1 = new OperationProcessor(node1, context);
        Thread t1 = new Thread(op1);
        OperationProcessor op2 = new OperationProcessor(node2, context);
        Thread t2 = new Thread(op2);
        OperationProcessor op3 = new OperationProcessor(node3, context);
        Thread t3 = new Thread(op3);
        OperationProcessor op4 = new OperationProcessor(node4, context);
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

        logger.info("--> indexed {} documents in total", context.docCount.get());
        logger.info("--> FINAL STATS - attempts: {}, success: {}, failed: {}, timed out: {}", context.docCount.get(), context.successfullDocs.get(), context.failedDocs.get(), context.timedOutDocs.get());
        assertThat("each indexed document should be successfully indexed", context.docCount.get(), equalTo(context.successfullDocs.get() + context.timedOutDocs.get()));

        refresh();
        int numDocs = context.docCount.get() * 2;
        Set<String> fetchedIds = newHashSet();
        SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery()).setSize(numDocs).get();
        for (SearchHit hit : resp.getHits().getHits()) {
            fetchedIds.add(hit.getId());
        }

        if (context.successfullDocs.get() != context.successIds.size()) {
            logger.info("--> expected {} successful docs, but {} actually succeeded [diff:{}] (some timed out docs were successful)",
                    context.successfullDocs.get(), context.successIds.size(), context.successIds.size() - context.successfullDocs.get());
        }

        // Check for documents that are part of the successful ids but not returned from the query
        Set<String> missingIds = newHashSet();
        missingIds.addAll(context.successIds);
        missingIds.removeAll(fetchedIds);
        assertThat("ids that were marked as successful but are missing: " + missingIds, missingIds.size(), equalTo(0));

        // Check for documents that were returned from the query but were not marked as successful
        Set<String> extraIds = newHashSet();
        extraIds.addAll(fetchedIds);
        extraIds.removeAll(context.successIds);
        assertThat("ids that were NOT marked as successful but actually exist: " + extraIds, extraIds.size(), equalTo(0));

        // The number of ids that we fetched should be the same as the number
        // of ids that were successful from the perspective of the processors
        assertThat("queried ids should match fetched:" + fetchedIds.size() + " expected successful: " + context.successIds.size() +
                        " missing: " + Math.abs(fetchedIds.size() - context.successIds.size()),
                fetchedIds.size(), equalTo(context.successIds.size()));
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

    private static final class Context {

        final AtomicInteger docCount = new AtomicInteger(0);
        final AtomicInteger successfullDocs = new AtomicInteger(0);
        final AtomicInteger timedOutDocs = new AtomicInteger(0);
        final AtomicInteger failedDocs = new AtomicInteger(0);
        final Set<String> successIds = Collections.synchronizedSet(new HashSet<String>());

    }

    public class OperationProcessor implements Runnable {
        public volatile boolean run = true;

        private final String node;
        private final Context context;
        private final ExecutorService service = Executors.newSingleThreadExecutor();

        public OperationProcessor(String node, Context context) {
            this.node = node;
            this.context = context;
        }

        @Override
        public void run() {
            while (run) {
                // Delay for 0 - 200 ms
                stagger(200);
                final int id = context.docCount.incrementAndGet();
                Callable<Boolean> call = new Callable<Boolean>() {
                    @Override
                    public Boolean call() {
                        try {
                            long start = System.currentTimeMillis();
                            IndexResponse resp = client(node).prepareIndex("test", "number")
                                    .setSource("{\"num\": " + id + "}")
                                    .setTimeout(TimeValue.timeValueSeconds(5000))
                                    .setValidateWriteConsistency(true)
                                    .get();
                            // TODO: Move to a helper method in test cluster:
                            int shard = Math.abs(((InternalTestCluster) cluster()).getInstance(DjbHashFunction.class).hash(resp.getId()) % 5);
                            logger.info("--> {} indexed document {} into shard {} in {}ms", node, resp.getId(), shard, System.currentTimeMillis() - start);
                            context.successIds.add(resp.getId());
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
                        context.successfullDocs.incrementAndGet();
                    } else {
                        context.failedDocs.incrementAndGet();
                        logger.info("--> indexing unsuccessful");
                    }
                    // Jepsen adds a 1 second sleep after a write
                    Thread.sleep(1000);
                } catch (TimeoutException te) {
                    context.timedOutDocs.incrementAndGet();
                    logger.info("--> {} operation #{} timed out after 5 seconds", node, id);
                } catch (Exception te) {
                    logger.info("--> {} operation #{} had an error", te, node, id);
                }
            }
        }
    }
}
