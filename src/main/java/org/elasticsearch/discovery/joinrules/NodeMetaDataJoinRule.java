package org.elasticsearch.discovery.joinrules;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;

/**
 */
public class NodeMetaDataJoinRule extends AbstractComponent implements JoinRule {

    public static final String CLUSTER_NODES_INCLUDE = "cluster.nodes.include.";
    public static final String CLUSTER_NODES_EXCLUDE = "cluster.nodes.exclude.";

    private volatile DiscoveryNodeFilters includes;
    private volatile DiscoveryNodeFilters excludes;

    @Inject
    public NodeMetaDataJoinRule(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        ImmutableMap<String, String> includeMap = settings.getByPrefix(CLUSTER_NODES_INCLUDE).getAsMap();
        if (includeMap.isEmpty()) {
            includes = null;
        } else {
            includes = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
        }

        ImmutableMap<String, String> excludeMap = settings.getByPrefix(CLUSTER_NODES_EXCLUDE).getAsMap();
        if (excludeMap.isEmpty()) {
            excludes = null;
        } else {
            excludes = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
        }

        nodeSettingsService.addListener(new ApplySettings());
    }

    @Override
    public boolean accept(DiscoveryNode node) {
        if (includes == null && excludes == null) {
            return true;
        }

        if (includes.match(node)) {
            return true;
        } else if (excludes.match(node)) {
            return false;
        } else {
            return false;
        }

    }

    class ApplySettings implements NodeSettingsService.Listener {

        @Override
        public void onRefreshSettings(Settings settings) {
            ImmutableMap<String, String> includeMap = settings.getByPrefix(CLUSTER_NODES_INCLUDE).getAsMap();
            if (!includeMap.isEmpty()) {
                includes = DiscoveryNodeFilters.buildFromKeyValue(OR, includeMap);
            }

            ImmutableMap<String, String> excludeMap = settings.getByPrefix(CLUSTER_NODES_EXCLUDE).getAsMap();
            if (!excludeMap.isEmpty()) {
                excludes = DiscoveryNodeFilters.buildFromKeyValue(OR, excludeMap);
            }
        }

    }
}
