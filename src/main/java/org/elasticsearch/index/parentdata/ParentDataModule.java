package org.elasticsearch.index.parentdata;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.parentdata.paged.PagedParentData;

/**
 */
public class ParentDataModule extends AbstractModule {

    public static final class IdCacheSettings {
        public static final String PARENT_TYPE = "index.parentdata.type";
    }

    private final Settings settings;

    public ParentDataModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        bind(ParentData.class)
                .to(settings.getAsClass(IdCacheSettings.PARENT_TYPE, PagedParentData.class, "org.elasticsearch.index.parentdata.", "ParentData"))
                .in(Scopes.SINGLETON);
    }

}
