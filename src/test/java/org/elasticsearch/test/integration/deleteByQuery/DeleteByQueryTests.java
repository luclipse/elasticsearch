/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.integration.deleteByQuery;

import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DeleteByQueryTests extends AbstractNodesTests {

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
    public void testDeleteAllNoIndices() {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
        deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
        assertThat(actionGet.getIndices().size(), equalTo(0));
    }

    @Test
    public void testDeleteAllOneIndex() {
        client.admin().indices().prepareDelete().execute().actionGet();

        String json = "{" + "\"user\":\"kimchy\"," + "\"postDate\":\"2013-01-30\"," + "\"message\":\"trying out Elastic Search\"" + "}";

        client.prepareIndex("twitter", "tweet").setSource(json).setRefresh(true).execute().actionGet();

        SearchResponse search = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(1l));
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(client);
        deleteByQueryRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        DeleteByQueryResponse actionGet = deleteByQueryRequestBuilder.execute().actionGet();
        assertThat(actionGet.getIndex("twitter"), notNullValue());
        assertThat(actionGet.getIndex("twitter").getFailedShards(), equalTo(0));

        client.admin().indices().prepareRefresh().execute().actionGet();
        search = client.prepareSearch().setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        assertThat(search.getHits().totalHits(), equalTo(0l));
    }

    @Test
    public void testDeleteByParentChildQuery() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        client.admin().indices().prepareCreate("products")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 1)
                                .put("index.number_of_replicas", 0)
                )
                .addMapping(
                        "offer",
                        jsonBuilder().startObject().startObject("offer")
                                .startObject("_parent").field("type", "product").endObject()
                                .endObject().endObject()
                ).execute().actionGet();
        client.admin().cluster().prepareHealth("products").setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("products", "product", "1")
                .setSource(jsonBuilder().startObject().field("p", "1").endObject())
                .execute().actionGet();
        client.prepareIndex("products", "product", "2")
                .setSource(jsonBuilder().startObject().field("p", "2").endObject())
                .execute().actionGet();
        client.prepareIndex("products", "offer", "1")
                .setParent("2")
                .setSource(jsonBuilder().startObject().field("o", "1").endObject())
                .execute().actionGet();

        client.admin().indices().prepareRefresh("products").execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch("products")
                .setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(false));
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(3l));

        DeleteByQueryResponse deleteByQueryResponse = client.prepareDeleteByQuery("products")
                .setQuery(QueryBuilders.hasChildQuery("offer", QueryBuilders.matchQuery("o", "1")))
                .execute().actionGet();
        assertThat(deleteByQueryResponse.getIndex("products").getFailedShards(), equalTo(0));
        assertThat(deleteByQueryResponse.getIndex("products").getSuccessfulShards(), equalTo(1));

        searchResponse = client.prepareSearch("products")
                .setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
        assertThat(searchResponse.isTimedOut(), equalTo(false));
        assertThat(searchResponse.getFailedShards(), equalTo(0));
        assertThat(searchResponse.getHits().totalHits(), equalTo(2l));
    }

}
