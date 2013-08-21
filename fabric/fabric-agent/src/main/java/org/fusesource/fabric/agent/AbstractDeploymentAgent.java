package org.fusesource.fabric.agent;


import org.apache.felix.bundlerepository.Resource;
import org.apache.zookeeper.CreateMode;
import org.fusesource.fabric.agent.download.DownloadManager;
import org.fusesource.fabric.agent.executor.CompositeExecutorServiceManager;
import org.fusesource.fabric.agent.executor.FixedExecutorServiceManager;
import org.fusesource.fabric.zookeeper.IZKClient;
import org.fusesource.fabric.zookeeper.ZkDefs;
import org.fusesource.fabric.zookeeper.ZkPath;
import org.fusesource.fabric.zookeeper.utils.ZooKeeperUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Dictionary;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractDeploymentAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDeploymentAgent.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory("fabric-agent"));
    private final CompositeExecutorServiceManager downloadExecutorManager = new CompositeExecutorServiceManager();
    private DownloadManager downloadManager;


    public abstract IZKClient getZooKeeper();

    public abstract IZKClient waitForZooKeeper() throws InterruptedException;

    public abstract boolean doUpdate(Dictionary props) throws Exception;

    public abstract String getContainerName();


    public void start() throws IOException {
        downloadExecutorManager.add(new FixedExecutorServiceManager(5));
    }


    public void stop() throws InterruptedException, IOException {
        LOGGER.info("Stopping DeploymentAgent");
        // We can't wait for the threads to finish because the agent needs to be able to
        // update itself and this would cause a deadlock
        getExecutor().shutdown();
        downloadExecutorManager.close();
        getDownloadManager().shutdown();
    }


    public void updated(final Dictionary props)  {
        LOGGER.info("DeploymentAgent updated with {}", props);
        if (getExecutor().isShutdown() || props == null) {
            return;
        }
        getExecutor().submit(new Runnable() {
            public void run() {
                Throwable result = null;
                boolean success = false;
                try {
                    success = doUpdate(props);
                } catch (Throwable e) {
                    result = e;
                    LOGGER.error("Unable to update agent", e);
                }
                // This update is critical, so
                if (success || result != null) {
                    updateStatus(success ? ZkDefs.SUCCESS : ZkDefs.ERROR, result, null, true);
                }
            }
        });
    }

    protected void updateStatus(String status, Throwable result) {
        updateStatus(status, result, null, false);
    }

    protected void updateStatus(String status, Throwable result, List<Resource> resources, boolean force) {
        try {
            IZKClient zk = getZooKeeper();
            if (force) {
                zk = waitForZooKeeper();
            } else {
                zk = getZooKeeper();
            }
            if (zk != null) {
                String name = getContainerName();
                String e;
                if (result == null) {
                    e = null;
                } else {
                    StringWriter sw = new StringWriter();
                    result.printStackTrace(new PrintWriter(sw));
                    e = sw.toString();
                }
                if (resources != null) {
                    StringWriter sw = new StringWriter();
                    for (Resource res : resources) {
                        sw.write(res.getURI() + "\n");
                    }
                    zk.createOrSetWithParents(ZkPath.CONTAINER_PROVISION_LIST.getPath(name), sw.toString(), CreateMode.PERSISTENT);
                }
                zk.createOrSetWithParents(ZkPath.CONTAINER_PROVISION_RESULT.getPath(name), status, CreateMode.PERSISTENT);
                zk.createOrSetWithParents(ZkPath.CONTAINER_PROVISION_EXCEPTION.getPath(name), e, CreateMode.PERSISTENT);
            } else {
                LOGGER.info("ZooKeeper not available");
            }
        } catch (Throwable e) {
            LOGGER.warn("Unable to set provisioning result");
        }
    }

    /**
     * Retrieves the registered Maven Proxies and adds them to the configuraion.
     *
     * @param props
     */
    protected void addMavenProxies(Dictionary props) {
        try {
            IZKClient zooKeeper = getZooKeeper();
            if (zooKeeper.exists(ZkPath.MAVEN_PROXY.getPath("download")) != null) {
                StringBuffer sb = new StringBuffer();
                List<String> proxies = zooKeeper.getChildren(ZkPath.MAVEN_PROXY.getPath("download"));
                //We want the maven proxies to be sorted in the same manner that the fabric service does.
                //That's because when someone uses the fabric service to pick a repo for deployment, we want that repo to be used first.
                Collections.sort(proxies);
                for (String proxy : proxies) {
                    try {
                        String mavenRepo = ZooKeeperUtils.getSubstitutedPath(zooKeeper, ZkPath.MAVEN_PROXY.getPath("download") + "/" + proxy);
                        if (mavenRepo != null && mavenRepo.length() > 0) {
                            if (!mavenRepo.endsWith("/")) {
                                mavenRepo += "/";
                            }
                            if (sb.length() > 0) {
                                sb.append(",");
                            }
                            sb.append(mavenRepo);
                            sb.append("@snapshots");
                        }
                    } catch (Throwable t) {
                        LOGGER.warn("Failed to resolve proxy: " + proxy + ". It will be ignored.");
                    }
                }
                String existingRepos = (String) props.get("org.ops4j.pax.url.mvn.repositories");
                if (existingRepos != null) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(existingRepos);
                }
                props.put("org.ops4j.pax.url.mvn.repositories", sb.toString());
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to retrieve maven proxy urls: " + e.getMessage());
            LOGGER.debug("Unable to retrieve maven proxy urls: " + e.getMessage(), e);
        }
    }

    public CompositeExecutorServiceManager getDownloadExecutorManager() {
        return downloadExecutorManager;
    }

    /**
     * Returns an {@link ExecutorService} that is used for the {@link DownloadManager}.
     *
     * @return
     */
    protected synchronized ExecutorService getDownloadExecutor() {
        return downloadExecutorManager.get();
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public DownloadManager getDownloadManager() {
        return downloadManager;
    }

    public void setDownloadManager(DownloadManager downloadManager) {
        this.downloadManager = downloadManager;
    }

    static class NamedThreadFactory implements ThreadFactory {
        private static final AtomicInteger poolNumber = new AtomicInteger(1);
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(String prefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = prefix + "-" +
                    poolNumber.getAndIncrement() +
                    "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }

    }
}
