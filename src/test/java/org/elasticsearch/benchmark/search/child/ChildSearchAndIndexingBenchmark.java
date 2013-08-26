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

package org.elasticsearch.benchmark.search.child;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.hasChildFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 *
 */
public class ChildSearchAndIndexingBenchmark {

    static long COUNT = SizeValue.parseSizeValue("1m").singles();
    static int CHILD_COUNT = 5;
    static int BATCH = 100;
    static int QUERY_COUNT = 50;
    static String indexName = "test";
    static Random random = new Random();

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put("index.cache.parent-ordinals.type", "expandable")
                .put("gateway.type", "local")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        String clusterName = ChildSearchAndIndexingBenchmark.class.getSimpleName();
        Node node1 = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node1"))
                .clusterName(clusterName)
                .node();
        Client client = node1.client();

        client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10s").execute().actionGet();
        try {
            client.admin().indices().create(createIndexRequest(indexName)).actionGet();
            client.admin().indices().preparePutMapping(indexName).setType("child").setSource(XContentFactory.jsonBuilder().startObject().startObject("child")
                    .startObject("_parent").field("type", "parent").endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();

            System.out.println("--> Indexing [" + COUNT + "] parent document and [" + (COUNT * CHILD_COUNT) + " child documents");
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    request.add(Requests.indexRequest(indexName).type("parent").id(Integer.toString(counter))
                            .source(parentSource(Integer.toString(counter), "test" + counter)));
                    for (int k = 0; k < CHILD_COUNT; k++) {
                        request.add(Requests.indexRequest(indexName).type("child").id(Integer.toString(counter) + "_" + k)
                                .parent(Integer.toString(counter))
                                .source(childSource(Integer.toString(counter), "tag" + k)));
                    }
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("--> failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("--> Indexed " + (i * BATCH) * (1 + CHILD_COUNT) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            System.out.println("--> Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) (COUNT * (1 + CHILD_COUNT))) / stopWatch.totalTime().secondsFrac()));
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth(indexName).setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
        }
        client.admin().indices().prepareRefresh().execute().actionGet();
        System.out.println("--> Number of docs in index: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().getCount());

        SearchThread searchThread = new SearchThread(client);
        new Thread(searchThread).start();
        IndexThread indexThread = new IndexThread(client);
        new Thread(indexThread).start();

        System.in.read();

        indexThread.stop();
        searchThread.stop();
        client.close();
        node1.close();
    }

    private static XContentBuilder parentSource(String id, String nameValue) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }

    private static XContentBuilder childSource(String id, String tag) throws IOException {
        return jsonBuilder().startObject().field("id", id).field("tag", tag).endObject();
    }

    static class IndexThread implements Runnable {

        private final Client client;

        private volatile boolean run = true;
        private final CountDownLatch shutDownLatch = new CountDownLatch(1);

        IndexThread(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                while (run) {
                    BulkRequestBuilder bulkRequest = client.prepareBulk();
                    for (int i = 1; run && i < COUNT; i++) {
                        long id = COUNT + i;
                        bulkRequest.add(client.prepareIndex(indexName, "parent", Long.toString(id))
                                .setSource("delete", "me"));
                        Thread.sleep(100);
                        if (i % 1000 == 0) {
                            bulkRequest.get();
                            bulkRequest = client.prepareBulk();
                            long refreshStart = System.currentTimeMillis();
                            client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                            long refreshTime = System.currentTimeMillis() - refreshStart;
                            System.out.println("Refresh took: " + refreshTime + " ms");
                            if (i % 5000 == 0) {
                                client.prepareDeleteByQuery(indexName)
                                        .setQuery(QueryBuilders.termQuery("delete", "me")).get();
                            }
                            NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                                    .clear().setIndices(true).execute().actionGet();
                            System.out.println("Deleted docs: " + statsResponse.getAt(0).getIndices().getDocs().getDeleted());
                        }
                    }
                    bulkRequest.get();
                    long refreshStart = System.currentTimeMillis();
                    client.admin().indices().prepareRefresh(indexName).execute().actionGet();
                    long refreshTime = System.currentTimeMillis() - refreshStart;
                    System.out.println("Refresh after delete took: " + refreshTime + " ms");
                }
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                shutDownLatch.countDown();
            }
        }

        public void stop() throws InterruptedException {
            run = false;
            shutDownLatch.await();
        }

    }

    static class SearchThread implements Runnable {

        private final Client client;

        private volatile boolean run = true;
        private final CountDownLatch shutDownLatch = new CountDownLatch(1);

        SearchThread(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            try {
                while (run) {
                    long totalQueryTime = 0;
                    for (int j = 0; j < QUERY_COUNT; j++) {
                        SearchResponse searchResponse = client.prepareSearch(indexName)
                                .setQuery(
                                        filteredQuery(
                                                matchAllQuery(),
                                                hasChildFilter("child", termQuery("tag", "tag" + random.nextInt(CHILD_COUNT)))
                                        )
                                )
                                .execute().actionGet();
                        if (searchResponse.getFailedShards() > 0) {
                            System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
                        }
                        if (searchResponse.getHits().totalHits() != COUNT) {
                            System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + COUNT + "]");
                        }
                        totalQueryTime += searchResponse.getTookInMillis();
                    }
                    System.out.println("--> has_child filter with term filter Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

                    totalQueryTime = 0;
                    for (int j = 1; j <= QUERY_COUNT; j++) {
                        SearchResponse searchResponse = client.prepareSearch(indexName)
                                .setQuery(
                                        filteredQuery(
                                                matchAllQuery(),
                                                hasChildFilter("child", matchAllQuery())
                                        )
                                )
                                .execute().actionGet();
                        if (searchResponse.getFailedShards() > 0) {
                            System.err.println("Search Failures " + Arrays.toString(searchResponse.getShardFailures()));
                        }
                        if (searchResponse.getHits().totalHits() != COUNT) {
                            System.err.println("--> mismatch on hits [" + j + "], got [" + searchResponse.getHits().totalHits() + "], expected [" + COUNT + "]");
                        }
                        totalQueryTime += searchResponse.getTookInMillis();
                    }
                    System.out.println("--> has_child filter with match_all child query, Query Avg: " + (totalQueryTime / QUERY_COUNT) + "ms");

                    NodesStatsResponse statsResponse = client.admin().cluster().prepareNodesStats()
                            .setJvm(true).execute().actionGet();
                    System.out.println("--> Committed heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapCommitted());
                    System.out.println("--> Used heap size: " + statsResponse.getNodes()[0].getJvm().getMem().getHeapUsed());
                    Thread.sleep(1000);
                }
            } catch (Throwable e) {
                e.printStackTrace();
            } finally {
                shutDownLatch.countDown();
            }
        }

        public void stop() throws InterruptedException {
            run = false;
            shutDownLatch.await();
        }

    }

}
