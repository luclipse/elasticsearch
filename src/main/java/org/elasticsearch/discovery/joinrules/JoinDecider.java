package org.elasticsearch.discovery.joinrules;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 */
public class JoinDecider extends AbstractComponent {

    private Set<JoinRule> rules;

    @Inject
    public JoinDecider(Settings settings, Set<JoinRule> rules) {
        super(settings);
        this.rules = rules;
    }

    public boolean decide(DiscoveryNode node) {
        for (JoinRule rule : rules) {
            if (!rule.accept(node)) {
                return false;
            }
        }
        return true;
    }

}
