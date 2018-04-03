package org.apache.hadoop.fs.shell;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class PrefixFileFilter implements PathFilter {
    private final String name;

    PrefixFileFilter(String name) {
        this.name = name;
    }
    @Override
    public boolean accept(Path path) {
        return path.getName().startsWith(name);
    }
}
