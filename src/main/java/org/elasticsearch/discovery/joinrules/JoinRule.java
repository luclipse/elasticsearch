package org.elasticsearch.discovery.joinrules;

import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 */
public interface JoinRule {

    boolean accept(DiscoveryNode node);

}
