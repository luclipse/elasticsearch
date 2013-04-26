package org.elasticsearch.discovery.joinrules;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 */
public class JoinDeciderModule extends AbstractModule {

    private final Settings settings;

    private List<Class<? extends JoinRule>> joinRules = Lists.newArrayList();

    public JoinDeciderModule(Settings settings) {
        this.settings = settings;
    }

    public JoinDeciderModule add(Class<? extends JoinRule> allocationDecider) {
        this.joinRules.add(allocationDecider);
        return this;
    }

    @Override
    protected void configure() {
        Multibinder<JoinRule> joinRulesBinder = Multibinder.newSetBinder(binder(), JoinRule.class);
        joinRulesBinder.addBinding().to(NodeMetaDataJoinRule.class);

        for (Class<? extends JoinRule> allocation : joinRules) {
            joinRulesBinder.addBinding().to(allocation);
        }

        bind(JoinDecider.class).asEagerSingleton();
    }
}
