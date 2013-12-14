/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.action.admin.indices.gateway.snapshot.GatewaySnapshotRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.status.IndicesStatusRequestBuilder;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.elasticsearch.action.admin.indices.warmer.get.GetWarmersRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.percolate.MultiPercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.action.percolate.PercolateSourceBuilder.docBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.*;

public class IndicesOptionsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSpecifiedIndexUnavailable() throws Exception {
        assertAcked(prepareCreate("test1"));
        ensureYellow();

        // Verify defaults
        verify(search("test1", "test2"), true, 0);
        verify(msearch(null, "test1", "test2"), true, 0);
        verify(count("test1", "test2"), true, 0);
        verify(clearCache("test1", "test2"), true, 0);
        verify(_flush("test1", "test2"),true, 0);
        verify(gatewatSnapshot("test1", "test2"), true, 0);
        verify(segments("test1", "test2"), true, 0);
        verify(stats("test1", "test2"), true, 0);
        verify(status("test1", "test2"), true, 0);
        verify(optimize("test1", "test2"), true, 0);
        verify(refresh("test1", "test2"), true, 0);
        verify(validateQuery("test1", "test2"), true, 0);
        verify(aliasExists("test1", "test2"), true, 0);
        verify(typesExists("test1", "test2"), true, 0);
        verify(deleteByQuery("test1", "test2"), true, 0);
        verify(percolate("test1", "test2"), true, 0);
        verify(mpercolate(null, "test1", "test2"), true, 0);
        verify(suggest("test1", "test2"), true, 0);
        verify(getAliases("test1", "test2"), true, 0);
        verify(getFieldMapping("test1", "test2"), true, 0);
        verify(getMapping("test1", "test2"), true, 0);
        verify(getWarmer("test1", "test2"), true, 0);

        IndicesOptions options = IndicesOptions.strict();
        verify(search("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(msearch(options, "test1", "test2"), true, 0);
        verify(count("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(clearCache("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(_flush("test1", "test2").setIgnoreIndices(options),true, 0);
        verify(gatewatSnapshot("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(segments("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(stats("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(status("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(optimize("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(refresh("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(validateQuery("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(aliasExists("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(typesExists("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(deleteByQuery("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(percolate("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(mpercolate(options, "test1", "test2").setIgnoreIndices(options), true, 0);
        verify(suggest("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(getAliases("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(getFieldMapping("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(getMapping("test1", "test2").setIgnoreIndices(options), true, 0);
        verify(getWarmer("test1", "test2").setIgnoreIndices(options), true, 0);

        options = IndicesOptions.lenient();
        verify(search("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(msearch(options, "test1", "test2").setIgnoreIndices(options), false, 0);
        verify(count("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(clearCache("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(_flush("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(gatewatSnapshot("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(segments("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(stats("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(status("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(optimize("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(refresh("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(validateQuery("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(aliasExists("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(typesExists("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(deleteByQuery("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(percolate("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(mpercolate(options, "test1", "test2").setIgnoreIndices(options), false, 0);
        verify(suggest("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getAliases("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getFieldMapping("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getMapping("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getWarmer("test1", "test2").setIgnoreIndices(options), false, 0);

        options = IndicesOptions.strict();
        assertAcked(prepareCreate("test2"));
        ensureYellow();
        verify(search("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(msearch(options, "test1", "test2").setIgnoreIndices(options), false, 0);
        verify(count("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(clearCache("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(_flush("test1", "test2").setIgnoreIndices(options),false, 0);
        verify(gatewatSnapshot("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(segments("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(stats("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(status("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(optimize("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(refresh("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(validateQuery("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(aliasExists("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(typesExists("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(deleteByQuery("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(percolate("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(mpercolate(options, "test1", "test2").setIgnoreIndices(options), false, 0);
        verify(suggest("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getAliases("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getFieldMapping("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getMapping("test1", "test2").setIgnoreIndices(options), false, 0);
        verify(getWarmer("test1", "test2").setIgnoreIndices(options), false, 0);
    }

    @Test
    public void testSpecifiedIndexUnavailable_snapshotRestore() throws Exception {
        assertAcked(prepareCreate("test1"));
        ensureYellow();

        PutRepositoryResponse putRepositoryResponse = client().admin().cluster().preparePutRepository("dummy-repo")
                .setType("fs").setSettings(ImmutableSettings.settingsBuilder().put("location", newTempDir())).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        client().admin().cluster().prepareCreateSnapshot("dummy-repo", "snap1").setWaitForCompletion(true).get();

        verify(snapshot("snap2", "test1", "test2"), true, 0);
        verify(restore("snap1", "test1", "test2"), true, 0);

        IndicesOptions options = IndicesOptions.strict();
        verify(snapshot("snap2", "test1", "test2").setIndicesOptions(options), true, 0);
        verify(restore("snap1", "test1", "test2").setIgnoreIndices(options), true, 0);

        options = IndicesOptions.lenient();
        verify(snapshot("snap2", "test1", "test2").setIndicesOptions(options), false, 0);
        verify(restore("snap2", "test1", "test2").setIgnoreIndices(options), false, 0);

        options = IndicesOptions.strict();
        assertAcked(prepareCreate("test2"));
        ensureYellow();
        verify(snapshot("snap3", "test1", "test2").setIndicesOptions(options), false, 0);
        verify(restore("snap3", "test1", "test2").setIgnoreIndices(options), false, 0);
    }

    @Test
    public void testAllMissing_lenient() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test1"));
        client().prepareIndex("test1", "type", "1").setSource("k", "v").setRefresh(true).execute().actionGet();
        SearchResponse response = client().prepareSearch("test2")
                .setIgnoreIndices(IndicesOptions.lenient())
                .setQuery(matchAllQuery())
                .execute().actionGet();
        assertHitCount(response, 0l);

        response = client().prepareSearch("test2","test3").setQuery(matchAllQuery())
                .setIgnoreIndices(IndicesOptions.lenient())
                .execute().actionGet();
        assertHitCount(response, 0l);
        
        //you should still be able to run empty searches without things blowing up
        response  = client().prepareSearch()
                .setIgnoreIndices(IndicesOptions.lenient())
                .setQuery(matchAllQuery())
                .execute().actionGet();
        assertHitCount(response, 1l);
    }

    @Test
    public void testAllMissing_strict() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test1"));
        ensureYellow();
        try {
            client().prepareSearch("test2")
                    .setQuery(matchAllQuery())
                    .execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        try {
            client().prepareSearch("test2","test3")
                    .setQuery(matchAllQuery())
                    .execute().actionGet();
            fail("Exception should have been thrown.");
        } catch (IndexMissingException e) {
        }

        //you should still be able to run empty searches without things blowing up
        client().prepareSearch().setQuery(matchAllQuery()).execute().actionGet();
    }

    @Test
    // For now don't handle closed indices
    public void testClosed() throws Exception {
        assertAcked(prepareCreate("test1"));
        assertAcked(prepareCreate("test2"));
        ensureYellow();
        verify(search("test1", "test2"), false, 0);
        verify(count("test1", "test2"), false, 0);
        CloseIndexResponse closeIndexResponse = client().admin().indices().prepareClose("test2").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));

        try {
            search("test1", "test2").get();
            fail("Exception should have been thrown");
        } catch (ClusterBlockException e) {
        }
        try {
            count("test1", "test2").get();
            fail("Exception should have been thrown");
        } catch (ClusterBlockException e) {
        }

        verify(search(), false, 0);
        verify(count(), false, 0);

        verify(search("t*"), false, 0);
        verify(count("t*"), false, 0);
    }

    private static SearchRequestBuilder search(String... indices) {
        return client().prepareSearch(indices).setQuery(matchAllQuery());
    }

    private static MultiSearchRequestBuilder msearch(IndicesOptions options, String... indices) {
        MultiSearchRequestBuilder multiSearchRequestBuilder = client().prepareMultiSearch();
        if (options != null) {
            multiSearchRequestBuilder.setIgnoreIndices(options);
        }
        return multiSearchRequestBuilder.add(client().prepareSearch(indices).setQuery(matchAllQuery()));
    }

    private static CountRequestBuilder count(String... indices) {
        return client().prepareCount(indices).setQuery(matchAllQuery());
    }

    private static ClearIndicesCacheRequestBuilder clearCache(String... indices) {
        return client().admin().indices().prepareClearCache(indices);
    }

    private static FlushRequestBuilder _flush(String... indices) {
        return client().admin().indices().prepareFlush(indices);
    }

    private static GatewaySnapshotRequestBuilder gatewatSnapshot(String... indices) {
        return client().admin().indices().prepareGatewaySnapshot(indices);
    }

    private static IndicesSegmentsRequestBuilder segments(String... indices) {
        return client().admin().indices().prepareSegments(indices);
    }

    private static IndicesStatsRequestBuilder stats(String... indices) {
        return client().admin().indices().prepareStats(indices);
    }

    private static IndicesStatusRequestBuilder status(String... indices) {
        return client().admin().indices().prepareStatus(indices);
    }

    private static OptimizeRequestBuilder optimize(String... indices) {
        return client().admin().indices().prepareOptimize(indices);
    }

    private static RefreshRequestBuilder refresh(String... indices) {
        return client().admin().indices().prepareRefresh(indices);
    }

    private static ValidateQueryRequestBuilder validateQuery(String... indices) {
        return client().admin().indices().prepareValidateQuery(indices);
    }

    private static AliasesExistRequestBuilder aliasExists(String... indices) {
        return client().admin().indices().prepareAliasesExist("dummy").addIndices(indices);
    }

    private static TypesExistsRequestBuilder typesExists(String... indices) {
        return client().admin().indices().prepareTypesExists(indices).setTypes("dummy");
    }

    private static DeleteByQueryRequestBuilder deleteByQuery(String... indices) {
        return client().prepareDeleteByQuery(indices).setQuery(matchAllQuery());
    }

    private static PercolateRequestBuilder percolate(String... indices) {
        return client().preparePercolate().setIndices(indices)
                .setSource(new PercolateSourceBuilder().setDoc(docBuilder().setDoc("k", "v")))
                .setDocumentType("type");
    }

    private static MultiPercolateRequestBuilder mpercolate(IndicesOptions options, String... indices) {
        MultiPercolateRequestBuilder builder = client().prepareMultiPercolate();
        if (options != null) {
            builder.setIgnoreIndices(options);
        }
        return builder.add(percolate(indices));
    }

    private static SuggestRequestBuilder suggest(String... indices) {
        return client().prepareSuggest(indices).addSuggestion(SuggestBuilder.termSuggestion("name").field("a"));
    }

    private static GetAliasesRequestBuilder getAliases(String... indices) {
        return client().admin().indices().prepareGetAliases("dummy").addIndices(indices);
    }

    private static GetFieldMappingsRequestBuilder getFieldMapping(String... indices) {
        return client().admin().indices().prepareGetFieldMappings(indices);
    }

    private static GetMappingsRequestBuilder getMapping(String... indices) {
        return client().admin().indices().prepareGetMappings(indices);
    }

    private static GetWarmersRequestBuilder getWarmer(String... indices) {
        return client().admin().indices().prepareGetWarmers(indices);
    }

    private static CreateSnapshotRequestBuilder snapshot(String name, String... indices) {
        return client().admin().cluster().prepareCreateSnapshot("dummy-repo", name).setWaitForCompletion(true).setIndices(indices);
    }

    private static RestoreSnapshotRequestBuilder restore(String name, String... indices) {
        return client().admin().cluster().prepareRestoreSnapshot("dummy-repo", name)
                .setRenamePattern("(.+)").setRenameReplacement("$1-copy-" + name)
                .setWaitForCompletion(true)
                .setIndices(indices);
    }

    private static void verify(ActionRequestBuilder requestBuilder, boolean fail, long expectedCount) {
        if (fail) {
            if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());
            } else {
                try {
                    requestBuilder.get();
                    fail("IndexMissingException was expected");
                } catch (IndexMissingException e) {}
            }
        } else {
            if (requestBuilder instanceof SearchRequestBuilder) {
                SearchRequestBuilder searchRequestBuilder = (SearchRequestBuilder) requestBuilder;
                assertHitCount(searchRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof CountRequestBuilder) {
                CountRequestBuilder countRequestBuilder = (CountRequestBuilder) requestBuilder;
                assertHitCount(countRequestBuilder.get(), expectedCount);
            } else if (requestBuilder instanceof MultiSearchRequestBuilder) {
                MultiSearchResponse multiSearchResponse = ((MultiSearchRequestBuilder) requestBuilder).get();
                assertThat(multiSearchResponse.getResponses().length, equalTo(1));
                assertThat(multiSearchResponse.getResponses()[0].getResponse(), notNullValue());
            } else {
                requestBuilder.get();
            }
        }
    }

}
