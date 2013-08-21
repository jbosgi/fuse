package org.fusesource.fabric.agent.executor;

import org.apache.felix.framework.monitor.MonitoringService;
import org.osgi.framework.Bundle;
import org.osgi.framework.ServiceReference;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class FelixExecutorServiceManager implements ExecutorServiceManager {

    private final Bundle bundle;
    private final ServiceReference serviceReference;

    public FelixExecutorServiceManager(Bundle bundle) {
        this.bundle = bundle;
        this.serviceReference = bundle.getBundleContext().getServiceReference(MonitoringService.class.getName());
    }

    @Override
    public ExecutorService get() {
        if (serviceReference == null) {
            throw new UnsupportedOperationException();
        }
        return ((MonitoringService) bundle.getBundleContext().getService(serviceReference)).getExecutor(bundle);
    }

    @Override
    public void close() throws IOException {
        if (serviceReference != null) {
            bundle.getBundleContext().ungetService(serviceReference);
        }
    }
}
