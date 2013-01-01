package org.elasticsearch.search.nestedhits;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class NestedHitsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleNestedHits_source() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.builder()
                        .put("number_of_shards", 2)
                        .put("number_of_replicas", 0)
                )
                .addMapping("type1", jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                            .startObject("field1").field("type", "string").endObject()
                            .startObject("nested").field("type", "nested")
                                .startObject("properties")
                                    .startObject("field2").field("type", "string").endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value3 value2 value3")
                .endObject()
                .startObject()
                .field("field2", "value3 value4 value3 value5 value3")
                .endObject()
                .startObject()
                .field("field2", "value5 value6 value7")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value2")
                .endObject()
                .startObject()
                .field("field2", "value2 value3")
                .endObject()
                .startObject()
                .field("field2", "value4 value5")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value2")
                .endObject()
                .startObject()
                .field("field2", "value2 value4")
                .endObject()
                .startObject()
                .field("field2", "value4 value5")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                    .startObject()
                        .field("field2", "value1 value2")
                    .endObject()
                    .startObject()
                        .field("field2", "value2 value4")
                    .endObject()
                    .startObject()
                        .field("field2", "value4 value3")
                    .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.nestedQuery("nested", QueryBuilders.termQuery("field2", "value3")))
                .addNestedHit("1", "nested")
                .execute().actionGet();

        logger.info("Response:\n {}", response);

        assertThat(response.getShardFailures().length, equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getHits()[0].id(), equalTo("1"));
        assertThat(response.getHits().getHits()[0].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getHits().length, equalTo(2));
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getAt(0).offset(), equalTo(1));
        assertThat(
                response.getHits().getHits()[0].nestedHits().get("1").getAt(0).nestedSource().toUtf8(),
                equalTo(jsonBuilder().startObject().field("field2", "value3 value4 value3 value5 value3").endObject().string())
        );
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getAt(1).offset(), equalTo(0));
        assertThat(
                response.getHits().getHits()[0].nestedHits().get("1").getAt(1).nestedSource().toUtf8(),
                equalTo(jsonBuilder().startObject().field("field2", "value1 value3 value2 value3").endObject().string())
        );

        assertThat(response.getHits().getHits()[1].id(), equalTo("2"));
        assertThat(response.getHits().getHits()[1].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[1].nestedHits().get("1").getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[1].nestedHits().get("1").getAt(0).offset(), equalTo(1));
        assertThat(
                response.getHits().getHits()[1].nestedHits().get("1").getAt(0).nestedSource().toUtf8(),
                equalTo(jsonBuilder().startObject().field("field2", "value2 value3").endObject().string())
        );

        assertThat(response.getHits().getHits()[2].id(), equalTo("4"));
        assertThat(response.getHits().getHits()[2].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[2].nestedHits().get("1").getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[2].nestedHits().get("1").getAt(0).offset(), equalTo(2));
        assertThat(
                response.getHits().getHits()[2].nestedHits().get("1").getAt(0).nestedSource().toUtf8(),
                equalTo(jsonBuilder().startObject().field("field2", "value4 value3").endObject().string())
        );
    }

    @Test
    public void testSimpleNestedHits_fields() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.builder()
                                .put("number_of_shards", 2)
                                .put("number_of_replicas", 0)
                )
                .addMapping("type1", jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("_source")
                        .field("enabled", false)
                        .endObject()
                        .startObject("properties")
                        .startObject("field1").field("type", "string").endObject()
                        .startObject("nested").field("type", "nested")
                        .startObject("properties")
                        .startObject("field2").field("type", "string").field("store", "yes").endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value3 value2 value3")
                .endObject()
                .startObject()
                .field("field2", "value3 value4 value3 value5 value3")
                .endObject()
                .startObject()
                .field("field2", "value5 value6 value7")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value2")
                .endObject()
                .startObject()
                .field("field2", "value2 value3")
                .endObject()
                .startObject()
                .field("field2", "value4 value5")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value2")
                .endObject()
                .startObject()
                .field("field2", "value2 value4")
                .endObject()
                .startObject()
                .field("field2", "value4 value5")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("field1", "value1")
                .startArray("nested")
                .startObject()
                .field("field2", "value1 value2")
                .endObject()
                .startObject()
                .field("field2", "value2 value4")
                .endObject()
                .startObject()
                .field("field2", "value4 value3")
                .endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client().admin().indices().prepareRefresh("test").execute().actionGet();

        SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.nestedQuery("nested", QueryBuilders.termQuery("field2", "value3")))
                .addNestedHit("1", "nested", "nested.field2")
                .execute().actionGet();

        logger.info("Response:\n {}", response);

        assertThat(response.getShardFailures().length, equalTo(0));
        assertThat(response.getHits().totalHits(), equalTo(3l));
        assertThat(response.getHits().getHits()[0].id(), equalTo("1"));
        assertThat(response.getHits().getHits()[0].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getHits().length, equalTo(2));
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getAt(0).offset(), equalTo(1));
        assertThat(
                response.getHits().getHits()[0].nestedHits().get("1").getAt(0).nestedHits().get("nested.field2").getValue().toString(),
                equalTo("value3 value4 value3 value5 value3")
        );
        assertThat(response.getHits().getHits()[0].nestedHits().get("1").getAt(1).offset(), equalTo(0));
        assertThat(
                response.getHits().getHits()[0].nestedHits().get("1").getAt(1).nestedHits().get("nested.field2").getValue().toString(),
                equalTo("value1 value3 value2 value3")
        );

        assertThat(response.getHits().getHits()[1].id(), equalTo("2"));
        assertThat(response.getHits().getHits()[1].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[1].nestedHits().get("1").getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[1].nestedHits().get("1").getAt(0).offset(), equalTo(1));
        assertThat(
                response.getHits().getHits()[1].nestedHits().get("1").getAt(0).nestedHits().get("nested.field2").getValue().toString(),
                equalTo("value2 value3")
        );

        assertThat(response.getHits().getHits()[2].id(), equalTo("4"));
        assertThat(response.getHits().getHits()[2].nestedHits().isEmpty(), equalTo(false));
        assertThat(response.getHits().getHits()[2].nestedHits().get("1").getHits().length, equalTo(1));
        assertThat(response.getHits().getHits()[2].nestedHits().get("1").getAt(0).offset(), equalTo(2));
        assertThat(
                response.getHits().getHits()[2].nestedHits().get("1").getAt(0).nestedHits().get("nested.field2").getValue().toString(),
                equalTo("value4 value3")
        );
    }

}
