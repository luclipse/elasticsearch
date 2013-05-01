package org.elasticsearch.test.integration.document;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class BulkTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testBulkUpdate() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client.prepareBulk()
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 2).setCreate(true))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 3))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("4").setSource("field", 4))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("5").setSource("field", 5))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(5));

        bulkResponse = client.prepareBulk()
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("1").setScript("ctx._source.field += 1"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("3").setDoc(jsonBuilder().startObject().field("field1", "test").endObject()))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("1"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("3"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(2l));

        GetResponse getResponse = client.prepareGet().setIndex("test").setType("type1").setId("1").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(2l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("2").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(3l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("3").setFields("field1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(getResponse.getField("field1").getValue().toString(), equalTo("test"));
    }

}
