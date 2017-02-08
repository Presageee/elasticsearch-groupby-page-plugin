package org.elasticsearch.plugin.gtoplugin;

import org.elasticsearch.common.inject.AbstractModule;

/**
 * Created by LJT on 16-11-17.
 */
public class GTORestModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(GTORestHandler.class).asEagerSingleton();
    }
}
