package org.elasticsearch.test.integration.search.spellcheck;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.spellcheck.SpellcheckBuilder.createCommand;
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
                        .field("text", "abcd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "aacd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abbd")
                        .endObject()
                )
                .execute().actionGet();
        client.prepareIndex("test", "type1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("text", "abcc")
                        .endObject()
                )
                .execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse search = client.prepareSearch()
                .setQuery(matchQuery("text", "spellcecker"))
                .addSpellcheckCommand("test",
                        createCommand().setSuggestMode("always") // Always, otherwise the results can vary between requests.
                                .setSpellCheckText("abcd")
                                .setSpellCheckField("text"))
                .execute().actionGet();

        assertThat(Arrays.toString(search.shardFailures()), search.failedShards(), equalTo(0));
        assertThat(search.spellcheck(), notNullValue());
        assertThat(search.spellcheck().commands().size(), equalTo(1));
        assertThat(search.spellcheck().commands().get(0).getName(), equalTo("test"));
        assertThat(search.spellcheck().commands().get(0).getSuggestedWords().size(), equalTo(1));
        assertThat(search.spellcheck().commands().get(0).getSuggestedWords().get("abcd").size(), equalTo(3));
        assertThat(search.spellcheck().commands().get(0).getSuggestedWords().get("abcd").get(0).getSuggestion(), equalTo("abcc"));
        assertThat(search.spellcheck().commands().get(0).getSuggestedWords().get("abcd").get(1).getSuggestion(), equalTo("abbd"));
        assertThat(search.spellcheck().commands().get(0).getSuggestedWords().get("abcd").get(2).getSuggestion(), equalTo("aacd"));
    }

}
