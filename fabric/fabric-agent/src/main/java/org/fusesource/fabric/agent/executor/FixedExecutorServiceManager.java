package org.fusesource.fabric.agent.executor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FixedExecutorServiceManager implements ExecutorServiceManager {

    private final int threads;
    private ExecutorService service;

    public FixedExecutorServiceManager(int threads) {
        this.threads = threads;
    }

    @Override
    public synchronized ExecutorService get() {
        if (service == null) {
            this.service = Executors.newFixedThreadPool(threads);
        }
        return service;
    }

    @Override
    public void close() throws IOException {
        if (service != null) {
            service.shutdown();
        }
    }
}
