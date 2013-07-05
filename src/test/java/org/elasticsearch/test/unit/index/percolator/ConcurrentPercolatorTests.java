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

package org.elasticsearch.test.unit.index.percolator;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.memory.MemoryIndexPool;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@Test
public class ConcurrentPercolatorTests extends AbstractSharedClusterTest {

    @Test
    public void testSimpleConcurrentPerculator() throws InterruptedException, IOException {
        client().admin().indices().prepareCreate("index").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build()
        ).execute().actionGet();
        ensureGreen();

        final XContentBuilder onlyField1 = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .endObject().endObject();
        final XContentBuilder onlyField2 = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field2", "value")
                .endObject().endObject();
        final XContentBuilder bothFields = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();


        // We need to index a document / define mapping, otherwise field1 doesn't get reconized as number field.
        // If we don't do this, then 'test2' percolate query gets parsed as a TermQuery and not a RangeQuery.
        // The percolate api doesn't parse the doc if no queries have registered, so it can't lazily create a mapping
        client().prepareIndex("index", "type", "1").setSource(XContentFactory.jsonBuilder().startObject()
                .field("field1", 1)
                .field("field2", "value")
                .endObject()).execute().actionGet();

        client().prepareIndex("index", "_percolator", "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();
        client().prepareIndex("index", "_percolator", "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicBoolean stop = new AtomicBoolean(false);
        final AtomicInteger counts = new AtomicInteger(0);
        final AtomicBoolean assertionFailure = new AtomicBoolean(false);
        Thread[] threads = new Thread[5];

        for (int i = 0; i < threads.length; i++) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        while (!stop.get()) {
                            int count = counts.incrementAndGet();
                            if ((count % 100) == 0) {
                                ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
                                long poolMaxMemory = ByteSizeUnit.MB.toBytes(1 + (counts.get() % 10));
                                int poolSize = 1 + (counts.get() % 10);
                                long timeOut = TimeUnit.MILLISECONDS.toMillis(1 + (counts.get() % 1000));
                                builder.put(MemoryIndexPool.PERCOLATE_POOL_MAX_MEMORY, poolMaxMemory);
                                builder.put(MemoryIndexPool.PERCOLATE_POOL_SIZE, poolSize);
                                builder.put(MemoryIndexPool.PERCOLATE_TIMEOUT, timeOut);
                                ClusterUpdateSettingsResponse clusterRes = client().admin().cluster().prepareUpdateSettings()
                                        .setTransientSettings(builder.build()).execute().actionGet();
                                assertThat(
                                        clusterRes.getTransientSettings().getAsBytesSize(MemoryIndexPool.PERCOLATE_POOL_MAX_MEMORY, null).bytes(),
                                        equalTo(poolMaxMemory)
                                );
                                assertThat(
                                        clusterRes.getTransientSettings().getAsInt(MemoryIndexPool.PERCOLATE_POOL_SIZE, null),
                                        equalTo(poolSize)
                                );
                                assertThat(
                                        clusterRes.getTransientSettings().getAsTime(MemoryIndexPool.PERCOLATE_TIMEOUT, null).millis(),
                                        equalTo(timeOut)
                                );
                            }

                            if ((count > 10000)) {
                                stop.set(true);
                            }
                            PercolateResponse percolate;
                            if (count % 3 == 0) {
                                percolate = client().preparePercolate("index", "type").setSource(bothFields)
                                        .execute().actionGet();
                                assertThat(Arrays.asList(percolate.getMatches()), hasSize(2));
                                assertThat(Arrays.asList(percolate.getMatches()), hasItems("test1", "test2"));
                            } else if (count % 3 == 1) {
                                percolate = client().preparePercolate("index", "type").setSource(onlyField2)
                                        .execute().actionGet();
                                assertThat(Arrays.asList(percolate.getMatches()), hasSize(1));
                                assertThat(Arrays.asList(percolate.getMatches()), hasItems("test1"));
                            } else {
                                percolate = client().preparePercolate("index", "type").setSource(onlyField1)
                                        .execute().actionGet();
                                assertThat(Arrays.asList(percolate.getMatches()), hasSize(1));
                                assertThat(Arrays.asList(percolate.getMatches()), hasItems("test2"));
                            }
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (AssertionError e) {
                        assertionFailure.set(true);
                        Thread.currentThread().interrupt();
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(assertionFailure.get(), equalTo(false));
    }
}
