package org.fusesource.fabric.agent.executor;


import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class CompositeExecutorServiceManager implements ExecutorServiceManager {

    private final Set<ExecutorServiceManager> factories = new LinkedHashSet<ExecutorServiceManager>();

    @Override
    public ExecutorService get() {
        for (ExecutorServiceManager factory : factories) {
            try {
                ExecutorService service = factory.get();
                if (service != null) {
                    return service;
                }
            } catch (Exception ex) {
                //ignore and move to the next factory
            }
        }
        return null;
    }

    public boolean add(ExecutorServiceManager executorServiceManager) {
        return factories.add(executorServiceManager);
    }

    public boolean remove(ExecutorServiceManager executorServiceManager) {
        return factories.remove(executorServiceManager);
    }

    @Override
    public void close() throws IOException {
        for (ExecutorServiceManager factory : factories) {
            factory.close();
        }
        factories.clear();
    }
}
