package org.fusesource.fabric.zookeeper.internal.curator;

import org.apache.curator.ensemble.EnsembleProvider;

import java.io.IOException;

public class DynamicEnsembleProvider implements EnsembleProvider {

    private String connectionString;

    public void update(String connectionString) {
        this.connectionString = connectionString;
    }

    @Override
    public String getConnectionString() {
        return connectionString;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void close() throws IOException {
    }
}
