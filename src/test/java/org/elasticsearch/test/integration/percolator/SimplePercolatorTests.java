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

package org.elasticsearch.test.integration.percolator;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimplePercolatorTests extends AbstractSharedClusterTest {

    private long purgeInterval = 200;

    @BeforeClass
    public void createNodes() throws Exception {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local") // <-- For opening and closing index, so that the data doesn't gets purged.
                .put("indices.ttl.interval", purgeInterval).build(); // <-- For the ttl test.
        cluster().ensureAtLeastNumNodes(settings, 2);
    }

    @Test
    public void testSimple1() throws Exception {
        client().admin().indices().prepareCreate("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field", "value").execute().actionGet();

        logger.info("--> register a queries");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "b")).field("a", "b").endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "c")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "3")
                .setSource(jsonBuilder().startObject().field("query", boolQuery()
                        .must(matchQuery("field1", "b"))
                        .must(matchQuery("field1", "c"))
                ).endObject())
                .execute().actionGet();
        client().prepareIndex("test", "_percolator", "4")
                .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh("test").execute().actionGet();

        logger.info("--> Percolate doc with field1=b");
        PercolateResponse response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "b").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(response.getMatches(), arrayContainingInAnyOrder("1", "4"));

        logger.info("--> Percolate doc with field1=c");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "c").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(response.getMatches(), arrayContainingInAnyOrder("2", "4"));

        logger.info("--> Percolate doc with field1=b c");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "b c").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(4));
        assertThat(response.getMatches(), arrayContainingInAnyOrder("1", "2", "3", "4"));

        logger.info("--> Percolate doc with field1=d");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "d").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches(), arrayContaining("4"));

        logger.info("--> Search dummy doc, percolate queries must not be included");
        SearchResponse searchResponse = client().prepareSearch("test").execute().actionGet();
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).type(), equalTo("type"));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testSimple2() throws Exception {
        client().admin().indices().prepareCreate("index").setSettings(
                ImmutableSettings.settingsBuilder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build()
        ).execute().actionGet();
        ensureGreen();

        // introduce the doc
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().startObject("doc")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject();

        XContentBuilder docWithType = XContentFactory.jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", 1)
                .field("field2", "value")
                .endObject().endObject().endObject();

        PercolateResponse response = client().preparePercolate("index", "type1").setSource(doc)
                .execute().actionGet();
        assertThat(response.getMatches(), emptyArray());

        // add a query
        client().prepareIndex("test", "_percolator", "test1")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field2", "value")).endObject())
                .execute().actionGet();

        response = client().preparePercolate("test", "type1").setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches(), arrayContaining("test1"));

        response = client().preparePercolate("test", "type1").setSource(docWithType).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches(), arrayContaining("test1"));

        client().prepareIndex("test", "_percolator", "test2")
                .setSource(XContentFactory.jsonBuilder().startObject().field("query", termQuery("field1", 1)).endObject())
                .execute().actionGet();

        response = client().preparePercolate("test", "type1")
                .setSource(doc)
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(2));
        assertThat(response.getMatches(), arrayContainingInAnyOrder("test1", "test2"));


        client().prepareDelete("test", "_percolator", "test2").execute().actionGet();
        response = client().preparePercolate("test", "type1").setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches(), arrayContaining("test1"));

        // add a range query (cached)
        // add a query
        client().prepareIndex("test1", "_percolator")
                .setSource(
                        XContentFactory.jsonBuilder().startObject().field("query",
                                constantScoreQuery(FilterBuilders.rangeFilter("field2").from("value").includeLower(true))
                        ).endObject()
                )
                .execute().actionGet();

        response = client().preparePercolate("test", "type1").setSource(doc).execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches(), arrayContaining("test1"));
    }

    @Test
    public void testLoadingPercolateQueries() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> Add dummy doc");
        client().prepareIndex("test", "type", "1").setSource("field1", 0).execute().actionGet();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    // TODO: Think about the following:
                    // The range_query only works in this because document with `field1` exists in the this index,
                    // and is off type long. The range_query now gets parsed to the Lucene NumericRangeQuery.
                    // If the percolate queries were to be stored in a dedicated index then the range_query would
                    // have been parsed to the Lucene TermRangeQuery. I think we should add a option (reserved metadata?)
                    // that specifies the target index (`_index`), so that queries get parsed properly.
                    //
                    // Other queries and filter may run into similar issues. For example geo_shape and its precision.
                    .setSource(jsonBuilder().startObject().field("query", rangeQuery("field1").from(0).to(i)).endObject())
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc with field1=95");
        PercolateResponse response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 95).endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(6));
        assertThat(response.getMatches(), arrayContainingInAnyOrder("95", "96", "97", "98", "99", "100"));

        logger.info("--> Close and open index to trigger percolate queries loading...");
        client().admin().indices().prepareClose("test").execute().actionGet();
        ensureGreen();
        client().admin().indices().prepareOpen("test").execute().actionGet();
        ensureGreen();

        logger.info("--> Percolate doc with field1=100");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", 100).endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(1));
        assertThat(response.getMatches()[0], equalTo("100"));
    }

    @Test
    public void testPercolateQueriesWithRouting() throws Exception {
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a queries");
        for (int i = 1; i <= 100; i++) {
            client().prepareIndex("test", "_percolator", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
                    .setRouting(Integer.toString(i % 2))
                    .execute().actionGet();
        }

        logger.info("--> Percolate doc with no routing");
        PercolateResponse response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(100));

        logger.info("--> Percolate doc with routing=0");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("0")
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(50));

        logger.info("--> Percolate doc with routing=1");
        response = client().preparePercolate("test", "type")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value").endObject().endObject())
                .setRouting("1")
                .execute().actionGet();
        assertThat(response.getMatches(), arrayWithSize(50));
    }

    @Test
    public void percolateOnRecreatedIndex() throws Exception {
        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        wipeIndex("test");
        prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "test", "1").setSource("field1", "value1").execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex("my-queries-index", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();
    }
    
    @Test
    // see #2814
    public void percolateCustomAnalyzer() throws Exception {
        Builder builder = ImmutableSettings.builder();
        builder.put("index.analysis.analyzer.lwhitespacecomma.tokenizer", "whitespacecomma");
        builder.putArray("index.analysis.analyzer.lwhitespacecomma.filter", "lowercase");
        builder.put("index.analysis.tokenizer.whitespacecomma.type", "pattern");
        builder.put("index.analysis.tokenizer.whitespacecomma.pattern", "(,|\\s+)");
        
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("doc")
        .startObject("properties")
            .startObject("filingcategory").field("type", "string").field("analyzer", "lwhitespacecomma").endObject()
        .endObject()
        .endObject().endObject();

        client().admin().indices().prepareCreate("test")
            .addMapping("doc", mapping)
            .setSettings(builder.put("index.number_of_shards", 1))
            .execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "1")
                .setSource(jsonBuilder().startObject()
                        .field("source", "productizer")
                        .field("query", QueryBuilders.constantScoreQuery(QueryBuilders.queryString("filingcategory:s")))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate("test", "doc").setSource(jsonBuilder().startObject()
                .startObject("doc").field("filingcategory", "s").endObject()
                .field("query", termQuery("source", "productizer"))
                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
      
    }

    @Test
    public void registerPercolatorAndThenCreateAnIndex() throws Exception {
        logger.info("--> register a query");
        client().prepareIndex("my-percolate-index", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        PercolateResponse percolate = client().preparePercolate("my-percolate-index", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .setDocumentIndex("test")
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));

        percolate = client().preparePercolate("my-percolate-index", "type1")
                .setDocumentIndex("test")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().field("query", matchAllQuery()).endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
    }

    @Test
    public void createIndexAndThenRegisterPercolator() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .execute().actionGet();

        refresh();
        CountResponse countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes("_percolator")
                .execute().actionGet();
        assertThat(countResponse.getCount(), equalTo(1l));


        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate("test", "type1")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }

        for (int i = 0; i < 10; i++) {
            PercolateResponse percolate = client().preparePercolate("test", "type1")
                    .setPreference("_local")
                    .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                    .execute().actionGet();
            assertThat(percolate.getMatches(), arrayWithSize(1));
        }


        logger.info("--> delete the index");
        client().admin().indices().prepareDelete("test").execute().actionGet();
        logger.info("--> make sure percolated queries for it have been deleted as well");
        countResponse = client().prepareCount()
                .setQuery(matchAllQuery()).setTypes("_percolator")
                .execute().actionGet();
        assertThat(countResponse.getCount(), equalTo(0l));
    }

    @Test
    public void multiplePercolators() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> register a query 2");
        client().prepareIndex("test", "_percolator", "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate("test", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(Arrays.asList(percolate.getMatches()), hasItem("kuku"));

        percolate = client().preparePercolate("test", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1").field("field1", "value2").endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(percolate.getMatches(), arrayContaining("bubu"));

    }

    @Test
    public void dynamicAddingRemovingQueries() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        ensureGreen();

        logger.info("--> register a query 1");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate("test", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").field("field1", "value1").endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(percolate.getMatches(), arrayContaining("kuku"));

        logger.info("--> register a query 2");
        client().prepareIndex("test", "_percolator", "bubu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "green")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate("test", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1").field("field1", "value2").endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(percolate.getMatches(), arrayContaining("bubu"));

        logger.info("--> register a query 3");
        client().prepareIndex("test", "_percolator", "susu")
                .setSource(jsonBuilder().startObject()
                        .field("color", "red")
                        .field("query", termQuery("field1", "value2"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        percolate = client().preparePercolate("test", "type1")
                .setSource(jsonBuilder().startObject().startObject("doc").startObject("type1").field("field1", "value2").endObject().endObject()
                        .field("query", termQuery("color", "red")).endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(percolate.getMatches(), arrayContaining("susu"));

        logger.info("--> deleting query 1");
        client().prepareDelete("test", "_percolator", "kuku").setRefresh(true).execute().actionGet();

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc").startObject("type1")
                .field("field1", "value1")
                .endObject().endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), emptyArray());
    }

    @Test
    public void percolateWithSizeField() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("_size").field("enabled", true).field("stored", "yes").endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("type1", mapping)
                .execute().actionGet();
        ensureGreen();

        logger.info("--> register a query");
        client().prepareIndex("test", "_percolator", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        logger.info("--> percolate a document");
        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject()
                .startObject("doc").startObject("type1")
                .field("field1", "value1")
                .endObject().endObject()
                .endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches(), arrayWithSize(1));
        assertThat(percolate.getMatches(), arrayContaining("kuku"));
    }

    @Test
    public void testThatPercolatingWithTimeToLiveWorks() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("_percolator")
                .startObject("_ttl").field("enabled", true).endObject()
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject().string();

        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 2))
                .addMapping("_percolator", mapping)
                .addMapping("type1", mapping)
                .execute().actionGet();
        ensureGreen();

        long ttl = 1500;
        long now = System.currentTimeMillis();
        client().prepareIndex("test", "_percolator", "kuku").setSource(jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("term")
                .field("field1", "value1")
                .endObject()
                .endObject()
                .endObject()
        ).setRefresh(true).setTTL(ttl).execute().actionGet();

        PercolateResponse percolateResponse = client().preparePercolate("test", "type1").setSource(jsonBuilder()
                .startObject()
                    .startObject("doc")
                        .field("field1", "value1")
                    .endObject()
                .endObject()
        ).execute().actionGet();
        assertThat(percolateResponse.getMatches(), arrayContaining("kuku"));

        long now1 = System.currentTimeMillis();
        if ((now1 - now) <= (ttl + purgeInterval)) {
            logger.info("Waiting for ttl purger...");
            Thread.sleep((ttl + purgeInterval) - (now1 - now));
        }
        percolateResponse = client().preparePercolate("test", "type1").setSource(jsonBuilder()
                .startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .endObject()
        ).execute().actionGet();
        assertThat(percolateResponse.getMatches(), emptyArray());
    }
}
