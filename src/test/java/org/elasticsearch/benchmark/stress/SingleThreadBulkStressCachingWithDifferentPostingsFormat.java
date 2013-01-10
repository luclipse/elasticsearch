package org.elasticsearch.benchmark.stress;

import jsr166y.ThreadLocalRandom;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class SingleThreadBulkStressCachingWithDifferentPostingsFormat {

    public static void main(String[] args) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "1s")
                .put("index.merge.async", true)
                .put("index.translog.flush_threshold_ops", 5000)
                .put("gateway.type", "none")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }
        Client client = nodes[0].client();

        String[] postingFormats = new String[]{"lucene40", "bloom_default", "m_bloom", "bloom_default_caching"};
        for (String postingFormat : postingFormats) {
            client.admin().indices().prepareCreate(postingFormat).setSettings(settings).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("_source").field("enabled", false).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("_type").field("index", "no").endObject()
                    .startObject("_id").field("index", "no").endObject()
                    .startObject("_uid").field("postings_format", postingFormat).endObject()
                    .startObject("properties")
                    .startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", true).endObject()
                    .endObject()
                    .endObject().endObject()).execute().actionGet();
            client.admin().cluster().prepareHealth(postingFormat).setWaitForGreenStatus().execute().actionGet();

            long COUNT = SizeValue.parseSizeValue("2m").singles();
            int BATCH = 500;
            long ITERS = COUNT / BATCH;
            long i = 1;
            int counter = 0;

            System.out.println("Indexing [" + COUNT + "] documents with postings format [" + postingFormat + "]");
            StopWatch stopWatch = new StopWatch().start();
            for (; i <= ITERS; i++) {
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    counter++;
                    request.add(Requests.indexRequest(postingFormat).type("type1").id(Integer.toString(random.nextInt())).source(source("test" + counter)));
                }
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("failures...");
                }
            }
            stopWatch.stop();
            System.out.println("Indexing [" + postingFormat + "] took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

            client.admin().indices().prepareRefresh(postingFormat).execute().actionGet();
            System.out.println("Count: " + client.prepareCount(postingFormat).setQuery(matchAllQuery()).execute().actionGet().count());
        }

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String nameValue) throws IOException {
        return jsonBuilder().startObject().field("field", nameValue).endObject();
    }

}
