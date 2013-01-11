package org.elasticsearch.benchmark.search;

import org.apache.lucene.benchmark.byTask.feeds.ContentSource;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.spellcheck.SpellcheckBuilder;
import org.elasticsearch.search.spellcheck.SuggestedWord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class EnWikiSpellcheckerSearchBenchMark {

    public static int MAX_DOCS = 5000000;
    public static int BATCH = 500;

    public static void main(String[] args) throws Exception {
        int SEARCH_ITERS = 200;

        Settings settings = settingsBuilder()
                .put("index.refresh_interval", "-1")
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

        Node[] nodes = new Node[1];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = nodeBuilder().settings(settingsBuilder().put(settings).put("name", "node" + i)).node();
        }

        Client client = nodes[0].client();
        try {
            client.admin().indices().prepareCreate("enwiki").setSettings(settings).addMapping("page", XContentFactory.jsonBuilder().startObject().startObject("page")
                    .startObject("_source").field("enabled", false).endObject()
                    .startObject("_all").field("enabled", false).endObject()
                    .startObject("_type").field("index", "no").endObject()
                    .startObject("_id").field("index", "no").endObject()
                    .startObject("properties")
                    .startObject("text").field("type", "string").field("omit_norms", true).endObject()
                    .endObject()
                    .endObject().endObject()).execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth("test").setWaitForGreenStatus().execute().actionGet();
            if (clusterHealthResponse.timedOut()) {
                System.err.println("--> Timed out waiting for cluster health");
            }


            Properties properties = new Properties();
            properties.setProperty("docs.file", "/Users/mvg/Temp/enwiki-20121201-pages-articles.xml.gz2");
            properties.setProperty("content.source.forever", "false");
            properties.setProperty("keep.image.only.docs", "false");
            Config config = new Config(properties);

            ContentSource source = new EnwikiContentSource();
            source.setConfig(config);

            DocMaker docMaker = new DocMaker();
            docMaker.setConfig(config, source);
            docMaker.resetInputs();

            System.out.println("Starting Extraction");
            StopWatch stopWatch = new StopWatch().start();

            BulkRequestBuilder request = client.prepareBulk();
            int counter = 0;
            try {
                for (Document doc = docMaker.makeDocument(); doc != null; doc = docMaker.makeDocument()) {
                    request.add(Requests.indexRequest("enwiki").type("page").id(doc.get(DocMaker.ID_FIELD)).source(
                            source(doc.get(DocMaker.TITLE_FIELD), doc.get(DocMaker.DATE_FIELD), doc.get(DocMaker.BODY_FIELD)))
                    );
                    if (counter % BATCH == 0) {
                        BulkResponse response = request.execute().actionGet();
                        if (response.hasFailures()) {
                            System.err.println("failures...");
                        }
                        request = client.prepareBulk();
                    }

                    if (counter % 100000 == 0) {
                        System.out.println("Indexed " + counter + " documents...");
                    }

                    if (++counter > MAX_DOCS) {
                        break;
                    }
                }
            } catch (NoMoreDataException e) {
                //continue
            }

            if (request.numberOfActions() != 0) {
                BulkResponse response = request.execute().actionGet();
                if (response.hasFailures()) {
                    System.err.println("failures...");
                }
            }
            stopWatch.stop();
            System.out.println("Indexing took " + stopWatch.totalTime());
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

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("./50k-misspelled-terms.txt")));
        List<String> spellTexts = new ArrayList<String>(50000);
        for (String spellText = reader.readLine(); spellText != null; spellText = reader.readLine()) {
            spellTexts.add(spellText);
        }

        System.out.println("Warming up...");
        int i = 0;
        for (String term : spellTexts) {
            client.prepareSearch()
                    .setSpellcheckGlobalField("body")
                    .setSpellcheckGlobalSuggestMode("always")
                    .addSpellcheckCommand("body", new SpellcheckBuilder.Command().setSpellCheckText(term))
                    .execute().actionGet();
        }


        System.out.println("Starting benchmarking spellchecking without filter.");
        long timeTaken = 0;
        int noSuggestions = 0;
        for (String term : spellTexts) {
            SearchResponse response = client.prepareSearch()
                    .setSpellcheckGlobalField("body")
                    .setSpellcheckGlobalSuggestMode("always")
                    .addSpellcheckCommand("body", new SpellcheckBuilder.Command().setSpellCheckText(term))
                    .execute().actionGet();
            timeTaken += response.tookInMillis();
            if (response.spellcheck() == null) {
                noSuggestions++;
                continue;
            }
            List<SuggestedWord> words = response.spellcheck().commands().get(0).getSuggestedWords().get(term);
            if (words == null || words.isEmpty()) {
                noSuggestions++;
            }
        }

        System.out.println("Avg time taken without filter " + (timeTaken / spellTexts.size()));
        System.out.println("Number of no suggestions " + noSuggestions);

        RangeFilterBuilder[] rangeFilterBuilders = new RangeFilterBuilder[]{
                FilterBuilders.rangeFilter("date").gte("01-JAN-2011 00:00:00.000").lte("01-FEB-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-FEB-2011 00:00:00.000").lte("01-MAR-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-MAR-2011 00:00:00.000").lte("01-APR-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-APR-2011 00:00:00.000").lte("01-MAY-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-MAY-2011 00:00:00.000").lte("01-JUN-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-JUN-2011 00:00:00.000").lte("01-JUL-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-JUL-2011 00:00:00.000").lte("01-AUG-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-AUG-2011 00:00:00.000").lte("01-SEP-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-SEP-2011 00:00:00.000").lte("01-OCT-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-OCT-2011 00:00:00.000").lte("01-NOV-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-NOV-2011 00:00:00.000").lte("01-DEC-2011 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-JAN-2012 00:00:00.000").lte("01-FEB-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-FEB-2012 00:00:00.000").lte("01-MAR-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-MAR-2012 00:00:00.000").lte("01-APR-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-APR-2012 00:00:00.000").lte("01-MAY-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-MAY-2012 00:00:00.000").lte("01-JUN-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-JUN-2012 00:00:00.000").lte("01-JUL-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-JUL-2012 00:00:00.000").lte("01-AUG-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-AUG-2012 00:00:00.000").lte("01-SEP-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-SEP-2012 00:00:00.000").lte("01-OCT-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-OCT-2012 00:00:00.000").lte("01-NOV-2012 00:00:00.000"),
                FilterBuilders.rangeFilter("date").gte("01-NOV-2012 00:00:00.000").lte("01-DEC-2012 00:00:00.000"),
        };

        System.out.println("Starting benchmarking spellchecking with filter.");
        timeTaken = 0;
        i = 0;
        noSuggestions = 0;
        for (String term : spellTexts) {
            RangeFilterBuilder builder = rangeFilterBuilders[i++ % rangeFilterBuilders.length];
            SearchResponse response = client.prepareSearch()
                    .setSpellcheckGlobalField("body")
                    .setSpellcheckGlobalSuggestMode("always")
                    .setSpellcheckGlobalFilter(builder)
                    .addSpellcheckCommand("body", new SpellcheckBuilder.Command().setSpellCheckText(term))
                    .execute().actionGet();
            timeTaken += response.tookInMillis();
            if (response.spellcheck() == null) {
                noSuggestions++;
                continue;
            }
            List<SuggestedWord> words = response.spellcheck().commands().get(0).getSuggestedWords().get(term);
            if (words == null || words.isEmpty()) {
                noSuggestions++;
            }
        }
        System.out.println("Avg time taken with filter " + (timeTaken / spellTexts.size()));
        System.out.println("Number of no suggestions " + noSuggestions);

        client.close();
        for (Node node : nodes) {
            node.close();
        }
    }

    private static XContentBuilder source(String title, String date, String body) throws IOException {
        return jsonBuilder().startObject()
                .field("title", title)
                .field("date", date)
                .field("body", body)
                .endObject();
    }

}
