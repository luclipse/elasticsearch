package org.elasticsearch.test.integration.search.grouping;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SimpleGroupingTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_shards", numberOfShards()).put("index.number_of_replicas", 0).build();
        for (int i = 0; i < numberOfNodes(); i++) {
            startNode("node" + i, settings);
        }
        client = getClient();
    }

    protected int numberOfShards() {
        return 2;
    }

    protected int numberOfNodes() {
        return 2;
    }

    protected int numberOfRuns() {
        return 5;
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node0");
    }

    @Test
    public void simpleTest() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("title", "b")
                .field("source", "1")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("title", "a")
                .field("source", "1")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("title", "d")
                .field("source", "2")
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("title", "c")
                .field("source", "2")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse response = client.prepareSearch("test")
                .setSearchType(SearchType.GROUPING)
                .setQuery(QueryBuilders.matchAllQuery())
                .addSort("title", SortOrder.ASC)
                .groupByField("source")
                .sortWithinGroup(SortBuilders.fieldSort("title").order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(response.getHits().totalHits(), equalTo(4l));
        assertThat(response.getHits().hits().length, equalTo(2));
        assertThat(response.getHits().hits()[0].id(), equalTo("2"));
        assertThat(response.getHits().hits()[0].group().string(), equalTo("1"));
        assertThat(response.getHits().hits()[1].id(), equalTo("4"));
        assertThat(response.getHits().hits()[1].group().string(), equalTo("2"));
    }

}
