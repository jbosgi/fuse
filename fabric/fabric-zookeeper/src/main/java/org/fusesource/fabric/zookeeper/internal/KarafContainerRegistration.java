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
package org.fusesource.fabric.zookeeper.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.management.MBeanServer;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.fusesource.fabric.utils.HostUtils;
import org.fusesource.fabric.zookeeper.IZKClient;
import org.fusesource.fabric.zookeeper.ZkDefs;
import org.fusesource.fabric.zookeeper.ZkPath;
import org.linkedin.zookeeper.client.LifecycleListener;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceException;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.cm.ConfigurationEvent;
import org.osgi.service.cm.ConfigurationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fusesource.fabric.zookeeper.ZkPath.*;

public class KarafContainerRegistration extends ContainerRegistration implements ConfigurationListener {

    private transient Logger logger = LoggerFactory.getLogger(KarafContainerRegistration.class);

    public static final String IP_REGEX = "([1-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\\.([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])){3}";
    public static final String HOST_REGEX = "[a-zA-Z][a-zA-Z0-9\\-\\.]*[a-zA-Z]";
    public static final String IP_OR_HOST_REGEX = "((" + IP_REGEX + ")|(" + HOST_REGEX + ")|0.0.0.0)";
    public static final String RMI_HOST_REGEX = "://" + IP_OR_HOST_REGEX;

    private ConfigurationAdmin configurationAdmin;
    private BundleContext bundleContext;


    /**
     * Returns the name of the container.
     *
     * @return
     */
    @Override
    public String getContainerName() {
        return System.getProperty("karaf.name");
    }

    public String getJmxUrl(String name) throws IOException {
        Configuration config = configurationAdmin.getConfiguration("org.apache.karaf.management");
        if (config.getProperties() != null) {
            String jmx = (String) config.getProperties().get("serviceUrl");
            jmx = replaceJmxHost(jmx, "\\${zk:" + name + "/ip}");
            return jmx;
        } else {
            return null;
        }
    }

    public String getSshUrl(String name) throws IOException {
        Configuration config = configurationAdmin.getConfiguration("org.apache.karaf.shell");
        if (config != null && config.getProperties() != null) {
            String port = (String) config.getProperties().get("sshPort");
            return "${zk:" + name + "/ip}:" + port;
        } else {
            return null;
        }
    }


    public synchronized void registerMBeanServer(ServiceReference ref) {
        try {
            String name = System.getProperty("karaf.name");
            setMbeanServer((MBeanServer) bundleContext.getService(ref));
            if (getMbeanServer() != null) {
                getMbeanServer().addNotificationListener(new ObjectName("JMImplementation:type=MBeanServerDelegate"), this, null, name);
                registerDomains();
            }
        } catch (Exception e) {
            logger.warn("An error occurred during mbean server registration. This exception will be ignored.", e);
        }
    }

    public synchronized void unregisterMBeanServer(ServiceReference ref) {
        if (getMbeanServer() != null) {
            try {
                getMbeanServer().removeNotificationListener(new ObjectName("JMImplementation:type=MBeanServerDelegate"), this);
                unregisterDomains();
            } catch (Exception e) {
                logger.warn("An error occurred during mbean server unregistration. This exception will be ignored.", e);
            }
        }
        setMbeanServer(null);
        bundleContext.ungetService(ref);
    }

    @Override
    public synchronized void handleNotification(Notification notif, Object o) {
        logger.trace("handleNotification[{}]", notif);

        // we may get notifications when zookeeper client is not really connected
        // handle mbeans registration and de-registration events
        if (isConnected() && getMbeanServer() != null && notif instanceof MBeanServerNotification) {
            MBeanServerNotification notification = (MBeanServerNotification) notif;
            String domain = notification.getMBeanName().getDomain();
            String path = CONTAINER_DOMAIN.getPath((String) o, domain);
            try {
                if (MBeanServerNotification.REGISTRATION_NOTIFICATION.equals(notification.getType())) {
                    if (getDomains().add(domain) && getZooKeeper().exists(path) == null) {
                        getZooKeeper().createOrSetWithParents(path, "", CreateMode.PERSISTENT);
                    }
                } else if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION.equals(notification.getType())) {
                    getDomains().clear();
                    getDomains().addAll(Arrays.asList(getMbeanServer().getDomains()));
                    if (!getDomains().contains(domain)) {
                        // domain is no present any more
                        getZooKeeper().delete(path);
                    }
                }
//            } catch (KeeperException.SessionExpiredException e) {
//                logger.debug("Session expiry detected. Handling notification once again", e);
//                handleNotification(notif, o);
            } catch (Exception e) {
                logger.warn("Exception while jmx domain synchronization from event: " + notif + ". This exception will be ignored.", e);
            }
        }
    }

    /**
     * Replaces hostname/ip occurances inside the jmx url, with the specified hostname
     *
     * @param jmxUrl
     * @param hostName
     * @return
     */
    public static String replaceJmxHost(String jmxUrl, String hostName) {
        if (jmxUrl == null) {
            return null;
        }
        return jmxUrl.replaceAll(RMI_HOST_REGEX, "://" + hostName);
    }


    private boolean isConnected() {
        // we are only considered connected if we have a client and its connected
        return getZooKeeper() != null && getZooKeeper().isConnected();
    }

    /**
     * Receives notification of a Configuration that has changed.
     *
     * @param event The <code>ConfigurationEvent</code>.
     */
    @Override
    public void configurationEvent(ConfigurationEvent event) {
        try {
            if (getZooKeeper().isConnected()) {
                String name = getContainerName();
                if (event.getPid().equals("org.apache.karaf.shell") && event.getType() == ConfigurationEvent.CM_UPDATED) {
                    String sshUrl = getSshUrl(name);
                    if (sshUrl != null) {
                        getZooKeeper().createOrSetWithParents(CONTAINER_SSH.getPath(name), sshUrl, CreateMode.PERSISTENT);
                    }
                }
                if (event.getPid().equals("org.apache.karaf.management") && event.getType() == ConfigurationEvent.CM_UPDATED) {
                    String jmxUrl = getJmxUrl(name);
                    if (jmxUrl != null) {
                        getZooKeeper().createOrSetWithParents(CONTAINER_JMX.getPath(name), jmxUrl, CreateMode.PERSISTENT);
                    }
                }
            }
        } catch (Exception e) {

        }
    }

    public ConfigurationAdmin getConfigurationAdmin() {
        return configurationAdmin;
    }

    public void setConfigurationAdmin(ConfigurationAdmin configurationAdmin) {
        this.configurationAdmin = configurationAdmin;
    }

    public BundleContext getBundleContext() {
        return bundleContext;
    }

    public void setBundleContext(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }
}
