package org.fusesource.fabric.tomcat.agent;


import org.apache.zookeeper.KeeperException;
import org.fusesource.fabric.zookeeper.ZkPath;
import org.fusesource.fabric.zookeeper.internal.ContainerRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TomcatContainerRegistration extends ContainerRegistration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TomcatContainerRegistration.class);
    private static final String JMX_URL_FORMAT = "service:jmx:rmi:///jndi/rmi://%s:%s/jmxrmi";


    public void init() {
        LOGGER.info("Initializing registration agent.");
        getZooKeeper().registerListener(this);
    }

    @Override
    public synchronized void onConnected() {
        LOGGER.info("Registering container.");
        super.onConnected();
        createContainerConfiguration();
    }

    /**
     * Returns the name of the container.
     *
     * @return
     */
    @Override
    public String getContainerName() {
        return System.getProperty("tomcat.name");
    }

    /**
     * Returns the JMX Url of the container.
     *
     * @param name
     * @return
     */
    @Override
    public String getJmxUrl(String name) throws IOException {
        String rmiHost = "${zk:" + name + "/ip}";
        String rmiPort = System.getProperty("com.sun.management.jmxremote.port", "1099");
        return String.format(JMX_URL_FORMAT, rmiHost, rmiPort);
    }

    /**
     * Returns the SSH Url of the container.
     *
     * @param name
     * @return
     */
    @Override
    public String getSshUrl(String name) throws IOException {
        return null;
    }

    @Override
    public void destroy() {
        super.destroy();
        getZooKeeper().removeListener(this);
    }

    private void createContainerConfiguration() {
        String name = getContainerName();
        String versionNode = ZkPath.CONFIG_CONTAINER.getPath(name);
        try {
            if (getZooKeeper().exists(versionNode) == null) {
                ZkPath.createContainerPaths(getZooKeeper(), name, null, "fabric");
            }
        } catch (InterruptedException e) {
            //noop
        } catch (KeeperException e) {
            //noop
        }
    }
}
