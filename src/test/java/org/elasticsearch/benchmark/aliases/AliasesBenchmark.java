package org.elasticsearch.benchmark.aliases;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.AliasMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;

/**
 */
public class AliasesBenchmark {

    private static String INDEX_NAME = "my-index";
    private static int NUM_ADDITIONAL_NODES = 1;
    private static int BASE_ALIAS_COUNT = 100000;
    private static int NUM_ADD_ALIAS_REQUEST = 1000;

    public static void main(String[] args) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type", "local")
                .put("node.master", false)
                .put("cluster.name", "mvg").build();
        Node node1 = NodeBuilder.nodeBuilder().settings(
                ImmutableSettings.settingsBuilder().put(settings).put("node.master", true)
        ).node();

        Node[] otherNodes = new Node[NUM_ADDITIONAL_NODES];
        for (int i = 0; i < otherNodes.length; i++) {
            otherNodes[i] = NodeBuilder.nodeBuilder().settings(settings).node();
        }

        Client client = node1.client();
        try {
            client.admin().indices().prepareCreate(INDEX_NAME).execute().actionGet();
        } catch (Exception e) {
        }
        client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        int numberOfAliases = 0;
        try {
            GetAliasesResponse response = client.admin().indices().prepareGetAliases("a*")
                    .addIndices(INDEX_NAME)
                    .execute().actionGet();
            numberOfAliases = response.getAliases().get(INDEX_NAME).size();
        } catch (AliasMissingException e) {}

        System.out.println("Current number of aliases: " + numberOfAliases);

        if (numberOfAliases < BASE_ALIAS_COUNT) {
            int diff = BASE_ALIAS_COUNT - numberOfAliases;
            System.out.println("Adding " + diff + " more aliases to get to the base " + BASE_ALIAS_COUNT);
            IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
            for (int i = 0; i <= diff; i++) {
                builder.addAlias(INDEX_NAME, "alias" + numberOfAliases + i);
                if (i % 1000 == 0) {
                    builder.execute().actionGet();
                    builder = client.admin().indices().prepareAliases();
                }
            }
            builder.execute().actionGet();
        } else if (numberOfAliases > BASE_ALIAS_COUNT) {
            IndicesAliasesRequestBuilder builder = client.admin().indices().prepareAliases();
            int diff = numberOfAliases - BASE_ALIAS_COUNT;
            System.out.println("Removing " + diff + " aliases to get to the base " + BASE_ALIAS_COUNT);
            for (int i = 0; i <= diff; i++) {
                builder.removeAlias(INDEX_NAME, "alias" + (BASE_ALIAS_COUNT + i));
                if (i % 1000 == 0) {
                    builder.execute().actionGet();
                    builder = client.admin().indices().prepareAliases();
                }
            }
            builder.execute().actionGet();
        }

        numberOfAliases = 0;
        try {
            GetAliasesResponse response = client.admin().indices().prepareGetAliases("a*")
                    .addIndices(INDEX_NAME)
                    .execute().actionGet();
            numberOfAliases = response.getAliases().get(INDEX_NAME).size();
        } catch (AliasMissingException e) {}

        System.out.println("Current number of aliases: " + numberOfAliases);

        long totalTime = 0;
        int max = numberOfAliases + NUM_ADD_ALIAS_REQUEST;
        int x = 100;
        for (int i = numberOfAliases; i <= max; i++) {
            long time = System.currentTimeMillis();
//            String filter = termFilter("field" + i, "value" + i).toXContent(XContentFactory.jsonBuilder(), null).string();
            client.admin().indices().prepareAliases().addAlias(INDEX_NAME, "alias" + i/*, filter*/)
                    .execute().actionGet();
            totalTime += System.currentTimeMillis() - time;
            if (i % x == 0) {
                long avgTime = totalTime / x;
                System.out.println("[" + i + " ] avg alias create time: "  + avgTime + " ms");
                totalTime = 0;
            }
        }

        client.close();
        node1.close();
        for (Node otherNode : otherNodes) {
            otherNode.close();
        }
    }

}
