package org.elasticsearch.plugin.gtoplugin;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by LJT on 16-11-17.
 * email: linjuntan@sensetime.com
 */
public class GTOPlugin extends Plugin {
    @Override
    public String name() {
        return "elasticsearch-senseface-plugin";
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new GTORestModule());
    }

    @Override
    public String description() {
        return "history search";
    }
}
