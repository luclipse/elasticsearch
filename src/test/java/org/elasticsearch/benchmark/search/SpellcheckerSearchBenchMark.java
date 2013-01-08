package org.elasticsearch.benchmark.search;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
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
import org.elasticsearch.node.Node;
import org.elasticsearch.search.spellcheck.SpellcheckBuilder;
import org.elasticsearch.search.spellcheck.SuggestedWord;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class SpellcheckerSearchBenchMark {

    public static void main(String[] args) throws Exception {
        Settings settings = settingsBuilder()
//                .put("index.refresh_interval", "1s")
//                .put("index.merge.async", true)
//                .put("index.translog.flush_threshold_ops", 5000)
//                .put("gateway.type", "none")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        //Node client = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "client")).client(true).node();
        Client client = nodes[0].client();//client.client();

        Thread.sleep(1000);

        try {
            client.admin().indices().prepareCreate("test").setSettings(settings).addMapping("type1", XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("_source").field("enabled", false).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("_type").field("index", "no").endObject()
                    .startObject("_id").field("index", "no").endObject()
                    .startObject("properties")
                    .startObject("field").field("type", "string").field("index", "not_analyzed").field("omit_norms", true).endObject()
                    .endObject()
                    .endObject().endObject()).execute().actionGet();
            Thread.sleep(5000);

            StopWatch stopWatch = new StopWatch().start();
            long COUNT = SizeValue.parseSizeValue("2m").singles();
            int BATCH = 100;
            System.out.println("Indexing [" + COUNT + "] ...");
            long ITERS = COUNT / BATCH;
            long i = 1;
            char character = 'a';
            int idCounter = 0;
            for (; i <= ITERS; i++) {
                int termCounter = 0;
                BulkRequestBuilder request = client.prepareBulk();
                for (int j = 0; j < BATCH; j++) {
                    request.add(Requests.indexRequest("test").type("type1").id(Integer.toString(idCounter++)).source(source("prefix" + character + termCounter++)));
                }
                character++;
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("failures...");
                }
                if (((i * BATCH) % 10000) == 0) {
                    System.out.println("Indexed " + (i * BATCH) + " took " + stopWatch.stop().lastTaskTime());
                    stopWatch.start();
                }
            }
            System.out.println("Indexing took " + stopWatch.totalTime() + ", TPS " + (((double) COUNT) / stopWatch.totalTime().secondsFrac()));

            client.admin().indices().prepareRefresh().execute().actionGet();
            System.out.println("Count: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().count());
        } catch (Exception e) {
            System.out.println("--> Index already exists, ignoring indexing phase, waiting for green");
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().setTimeout("10m").execute().actionGet();
            if (clusterHealthResponse.timedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }
            client.admin().indices().prepareRefresh().execute().actionGet();
            System.out.println("Count: " + client.prepareCount().setQuery(matchAllQuery()).execute().actionGet().count());
        }

        char character = 'a';
        int ITERS = 100;
        long timeTaken = 0;
        for (int i = 0; i <= ITERS; i++) {
            String term = "prefix" + character;
            SearchResponse response = client.prepareSearch()
                    .setTypes("type1")
                    .setQuery(matchQuery("field", term))
                    .setGlobalSpellcheckField("field")
                    .setGlobalSuggestMode("always")
                    .addSpellcheckCommand("field", new SpellcheckBuilder.Command().setSpellCheckText(term))
                    .execute().actionGet();
            timeTaken += response.tookInMillis();
            if (response.spellcheck() == null) {
                System.err.println("No suggestions");
                continue;
            }
            List<SuggestedWord> words = response.spellcheck().commands().get(0).getSuggestedWords().get(term);
            if (words == null || words.isEmpty()) {
                System.err.println("No suggestions");
            }
            character++;
        }

        System.out.println("Avg time taken " + (timeTaken / ITERS));

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String nameValue) throws IOException {
        return jsonBuilder().startObject().field("field", nameValue).endObject();
    }

}
