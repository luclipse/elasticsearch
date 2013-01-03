package org.elasticsearch.test.integration.search.spellcheck;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.spellcheck.SpellCheckBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class SpellcheckSearchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void testSimple() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client.admin().indices().prepareCreate("test").execute().actionGet();

        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "Testing Elasticsearch's spellchecker functionality")
                        .endObject()
                )
                .execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellcecker"))
                .setSpellchecker(new SpellCheckBuilder().setSpellCheckField("text").setSpellCheckText("spellcecker"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.spellcheck(), notNullValue());
        assertThat(search.spellcheck().suggestedWords().size(), equalTo(1));
        assertThat(search.spellcheck().suggestedWords().get("spellcecker").get(0).string, equalTo("spellchecker"));
    }

}
