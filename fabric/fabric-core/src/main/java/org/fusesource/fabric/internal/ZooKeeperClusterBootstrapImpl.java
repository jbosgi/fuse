/**
 * Copyright (C) FuseSource, Inc.
 * http://fusesource.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.fabric.internal;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.fusesource.fabric.api.Container;
import org.fusesource.fabric.api.CreateEnsembleOptions;
import org.fusesource.fabric.api.DataStoreRegistrationHandler;
import org.fusesource.fabric.api.DynamicReference;
import org.fusesource.fabric.api.FabricException;
import org.fusesource.fabric.api.FabricService;
import org.fusesource.fabric.api.ZooKeeperClusterBootstrap;
import org.fusesource.fabric.api.jcip.ThreadSafe;
import org.fusesource.fabric.api.scr.AbstractComponent;
import org.fusesource.fabric.api.scr.ValidatingReference;
import org.fusesource.fabric.bootstrap.BootstrapConfiguration;
import org.fusesource.fabric.utils.BundleUtils;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

/**
 * ZooKeeperClusterBootstrap
 * |_ ConfigurationAdmin
 * |_ DataStoreRegistrationHandler (@see DataStoreManager)
 * |_ FabricService (optional,unary) (@see FabricServiceImpl)
 *    |_ ConfigurationAdmin
 *    |_ CuratorFramework (@see ManagedCuratorFramework)
 *    |_ DataStore (@see CachingGitDataStore)
 *    |  |_ CuratorFramework --^
 *    |  |_ GitService (@see FabricGitServiceImpl)
 *    |  |_ PlaceholderResolver (optional,multiple)
 *    |_ ContainerProvider (optional,multiple) (@see ChildContainerProvider)
 *    |  |_ FabricService --^
 *    |_ PortService (@see ZookeeperPortService)
 *       |_ CuratorFramework --^
 */
@ThreadSafe
@Component(name = "org.fusesource.fabric.zookeeper.cluster.bootstrap", description = "Fabric ZooKeeper Cluster Bootstrap", immediate = true)
@Service(ZooKeeperClusterBootstrap.class)
public final class ZooKeeperClusterBootstrapImpl extends AbstractComponent implements ZooKeeperClusterBootstrap {

    private static final Long FABRIC_SERVICE_TIMEOUT = 60000L;
    private static final String NAME = System.getProperty("karaf.name");

    @Reference(referenceInterface = ConfigurationAdmin.class)
    private final ValidatingReference<ConfigurationAdmin> configAdmin = new ValidatingReference<ConfigurationAdmin>();
    @Reference(referenceInterface = BootstrapConfiguration.class)
    private final ValidatingReference<BootstrapConfiguration> bootstrapConfiguration = new ValidatingReference<BootstrapConfiguration>();
    @Reference(referenceInterface = DataStoreRegistrationHandler.class)
    private final ValidatingReference<DataStoreRegistrationHandler> registrationHandler = new ValidatingReference<DataStoreRegistrationHandler>();
    @Reference(referenceInterface = FabricService.class, cardinality = ReferenceCardinality.OPTIONAL_UNARY, policy = ReferencePolicy.DYNAMIC)
    private final DynamicReference<FabricService> fabricService = new DynamicReference<FabricService>("Fabric Service", FABRIC_SERVICE_TIMEOUT, TimeUnit.MILLISECONDS);

    private BundleUtils bundleUtils;

    @Activate
    void activate(BundleContext bundleContext) throws Exception {
        this.bundleUtils = new BundleUtils(bundleContext);

        BootstrapConfiguration bootConfig = bootstrapConfiguration.get();
        CreateEnsembleOptions options = bootConfig.getBootstrapOptions();
        if (options.isEnsembleStart()) {
            startBundles(options);
        }

        activateComponent();
    }

    @Deactivate
    void deactivate() {
        deactivateComponent();
    }

    @Override
    public void create(CreateEnsembleOptions options) {
        assertValid();
        try {
            BootstrapConfiguration bootConfig = bootstrapConfiguration.get();
            String connectionUrl = bootConfig.getConnectionUrl(options);
            registrationHandler.get().setRegistrationCallback(new DataStoreBootstrapTemplate(connectionUrl, options));

            bootConfig.createOrUpdateDataStoreConfig(options);
            bootConfig.createZooKeeeperServerConfig(options);
            bootConfig.createZooKeeeperClientConfig(connectionUrl, options);

            startBundles(options);

            if (options.isWaitForProvision() && options.isAgentEnabled()) {
                waitForSuccessfulDeploymentOf(NAME, options.getProvisionTimeout());
            }
        } catch (RuntimeException rte) {
            throw rte;
        } catch (Exception ex) {
            throw new FabricException("Unable to create zookeeper server configuration", ex);
        }
	}

    private void waitForSuccessfulDeploymentOf(String containerName, long timeout) throws InterruptedException {
        System.out.println(String.format("Waiting for container %s to provision.", containerName));
        FabricService fabric = fabricService.get();
        long startedAt = System.currentTimeMillis();
        while (!Thread.interrupted() && startedAt + timeout > System.currentTimeMillis()) {
            try {
                Container container = fabric != null ? fabric.getContainer(containerName) : null;
                if (container != null && container.isAlive() && "success".equals(container.getProvisionStatus())) {
                    return;
                }
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                FabricException.launderThrowable(t);
            }
        }
    }

    @Override
    public void clean() {
        assertValid();
        try {
            Bundle bundleFabricZooKeeper = bundleUtils.findAndStopBundle("org.fusesource.fabric.fabric-zookeeper");
            Bundle bundleFabricGit = bundleUtils.findAndStopBundle("org.fusesource.fabric.fabric-git");

            for (; ; ) {
                Configuration[] configs = configAdmin.get().listConfigurations("(|(service.factoryPid=org.fusesource.fabric.zookeeper.server)(service.pid=org.fusesource.fabric.zookeeper))");
                if (configs != null && configs.length > 0) {
                    for (Configuration config : configs) {
                        config.delete();
                    }
                    Thread.sleep(100);
                } else {
                    break;
                }
            }

            File zkDir = new File("data/zookeeper");
            if (zkDir.isDirectory()) {
                File newZkDir = new File("data/zookeeper." + System.currentTimeMillis());
                if (!zkDir.renameTo(newZkDir)) {
                    newZkDir = zkDir;
                }
                delete(newZkDir);
            }

            File gitDir = new File("data/git");
            if (gitDir.isDirectory()) {
                delete(gitDir);
            }

            bundleFabricZooKeeper.start();
            bundleFabricGit.start();
        } catch (Exception e) {
            throw new FabricException("Unable to delete zookeeper configuration", e);
        }
    }

    private void startBundles(CreateEnsembleOptions options) throws BundleException {

        // Install the required bundles
        Bundle bundleFabricAgent = bundleUtils.findAndStopBundle("org.fusesource.fabric.fabric-agent");

        //Check if the agent is configured to auto start.
        if (options.isAgentEnabled()) {
            bundleFabricAgent.start();
        }
    }

    private static void delete(File dir) {
        if (dir.isDirectory()) {
            for (File child : dir.listFiles()) {
                delete(child);
            }
        }
        if (dir.exists()) {
            dir.delete();
        }
    }

    void bindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.bind(service);
    }

    void unbindConfigAdmin(ConfigurationAdmin service) {
        this.configAdmin.unbind(service);
    }

    void bindBootstrapConfiguration(BootstrapConfiguration service) {
        this.bootstrapConfiguration.bind(service);
    }

    void unbindBootstrapConfiguration(BootstrapConfiguration service) {
        this.bootstrapConfiguration.unbind(service);
    }

    void bindRegistrationHandler(DataStoreRegistrationHandler service) {
        this.registrationHandler.bind(service);
    }

    void unbindRegistrationHandler(DataStoreRegistrationHandler service) {
        this.registrationHandler.unbind(service);
    }

    void bindFabricService(FabricService service) {
        this.fabricService.bind(service);
    }

    void unbindFabricService(FabricService service) {
        this.fabricService.unbind(service);
    }
}
