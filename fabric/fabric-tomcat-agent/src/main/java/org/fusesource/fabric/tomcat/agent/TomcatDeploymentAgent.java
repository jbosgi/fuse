package org.fusesource.fabric.tomcat.agent;


import com.google.common.io.Files;
import org.fusesource.fabric.agent.AbstractDeploymentAgent;
import org.fusesource.fabric.agent.download.DownloadFuture;
import org.fusesource.fabric.agent.download.DownloadManager;
import org.fusesource.fabric.agent.download.FutureListener;
import org.fusesource.fabric.agent.executor.FixedExecutorServiceManager;
import org.fusesource.fabric.agent.mvn.DictionaryPropertyResolver;
import org.fusesource.fabric.agent.mvn.MavenConfigurationImpl;
import org.fusesource.fabric.agent.mvn.MavenRepositoryURL;
import org.fusesource.fabric.agent.mvn.MavenSettingsImpl;
import org.fusesource.fabric.agent.mvn.PropertiesPropertyResolver;
import org.fusesource.fabric.agent.utils.MultiException;
import org.fusesource.fabric.zookeeper.IZKClient;
import org.fusesource.fabric.zookeeper.ZkDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class TomcatDeploymentAgent extends AbstractDeploymentAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(TomcatDeploymentAgent.class);
    private static final String[] EXCLUDES = {"META-INF", "WEB-INF", "ROOT", "host-manager", "manager"};
    private static final String AGENT_PROPRTIES_LOCATION = System.getProperty("catalina.base") + File.separator + "conf" + File.separator + "org.fusesource.fabric.agent.properties";
    private static final String WEBAPPS_LOCATION = System.getProperty("catalina.base") + File.separator + "webapps";
    private static final String ARTIFACT_STRORE_LOCATION = "file://" + System.getProperty("catalina.base") + File.separator + "store";

    private final MavenRepositoryURL localRepository;
    private final List<MavenRepositoryURL> repositories = new ArrayList<MavenRepositoryURL>();
    private IZKClient zooKeeper;

    public TomcatDeploymentAgent() throws Exception {
        this.localRepository = new MavenRepositoryURL(ARTIFACT_STRORE_LOCATION);
    }

    public void start() throws IOException {
        getDownloadExecutorManager().add(new FixedExecutorServiceManager(5));
        final MavenConfigurationImpl config = new MavenConfigurationImpl(
                new PropertiesPropertyResolver(System.getProperties()), "org.ops4j.pax.url.mvn"
        );
        File artifactStore = new File(new URL(ARTIFACT_STRORE_LOCATION).getFile());
        if (!artifactStore.exists() && !artifactStore.mkdirs()) {
            throw new IOException("Failed to create artifact store at:" + ARTIFACT_STRORE_LOCATION);
        }
        config.setSettings(new MavenSettingsImpl(config.getSettingsFileUrl(), config.useFallbackRepositories()));
        setDownloadManager(new DownloadManager(config, getDownloadExecutorManager().get(), localRepository, repositories));
    }

    @Override
    public IZKClient getZooKeeper() {
        return zooKeeper;
    }

    @Override
    public IZKClient waitForZooKeeper() throws InterruptedException {
        return zooKeeper;
    }

    public void setZooKeeper(IZKClient zookeeper) {
        this.zooKeeper = zookeeper;
    }

    @Override
    public boolean doUpdate(Dictionary props) throws Exception {
        if (props == null) {
            return false;
        }

        // Adding the maven proxy URL to the list of repositories.
        addMavenProxies(props);

        // Building configuration
        final MavenConfigurationImpl config = new MavenConfigurationImpl(
                new DictionaryPropertyResolver(props,
                        new PropertiesPropertyResolver(System.getProperties())),
                "org.ops4j.pax.url.mvn"
        );
        config.setSettings(new MavenSettingsImpl(config.getSettingsFileUrl(), config.useFallbackRepositories()));
        setDownloadManager(new DownloadManager(config, getDownloadExecutorManager().get(), localRepository, repositories));
        Map<String, String> properties = new HashMap<String, String>();
        for (Enumeration e = props.keys(); e.hasMoreElements(); ) {
            Object key = e.nextElement();
            Object val = props.get(key);
            properties.put(String.valueOf(key), String.valueOf(val));
        }

        updateStatus("analyzing", null);
        final Set<String> wars = new HashSet<String>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            //TODO: we should change that to bundle really.
            String key = entry.getKey();
            String url = entry.getValue();
            if (key.startsWith("bundle.") && (url.startsWith("war:") || url.endsWith("war"))) {
                wars.add(url);
            }
        }
        updateStatus("downloading", null);
        Map<String, File> downloads = downloadWars(wars);
        File webappsDir = new File(WEBAPPS_LOCATION);
        List<Throwable> exceptions = new ArrayList<Throwable>();

        //Delete undefined wars
        File[] toDeleteFiles = webappsDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                if (s.startsWith("fabric-tomcat-agent")) {
                    return false;
                }
                for (String exclude : EXCLUDES) {
                    //Check excludes
                    if (exclude.equals(s) || ((exclude + ".war")).equals(s)) {
                        return false;
                    }
                }
                for (String warUrl : wars) {
                    if (warUrl.contains(s)) {
                        return false;
                    }
                }
                return true;
            }
        });
        for (File toDelete : toDeleteFiles) {
            if (toDelete.exists() && !deleteFileOrDirectory(toDelete)) {
                throw new IOException("Failed to delete war:" + toDelete.getAbsolutePath());
            }
        }


        //Install new files.
        for (File f : downloads.values()) {
            try {
                String warName = f.getName();
                String explodedWarName = f.getName().substring(0, f.getName().lastIndexOf("."));
                File installedWar = new File(webappsDir, warName);
                File installedExploded = new File(webappsDir, explodedWarName);

                if (installedWar.exists() && !deleteFileOrDirectory(installedExploded)) {
                    throw new IOException("Failed to delete war:" + installedWar.getAbsolutePath());
                }
                if (installedExploded.exists() && !deleteFileOrDirectory(installedExploded)) {
                    throw new IOException("Failed to delete exploded war:" + installedWar.getAbsolutePath());
                }
                Files.copy(f, new File(webappsDir, warName));
            } catch (Exception ex) {
                exceptions.add(ex);
            }
        }
        if (!exceptions.isEmpty()) {
            throw new MultiException("Error updating agent", exceptions);
        } else {
            updateStatus("success", null);
        }
        return true;
    }

    @Override
    public String getContainerName() {
        return System.getProperty("tomcat.name");
    }

    public void updated(final Dictionary props) {
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

    protected Map<String, File> downloadWars(Set<String> wars) throws Exception {
        final CountDownLatch latch = new CountDownLatch(wars.size());
        final Map<String, File> downloads = new ConcurrentHashMap<String, File>();
        final List<Throwable> errors = new CopyOnWriteArrayList<Throwable>();
        for (final String location : wars) {

            getDownloadManager().download(location).addListener(new FutureListener<DownloadFuture>() {
                public void operationComplete(DownloadFuture future) {
                    try {
                        downloads.put(location, future.getFile());
                    } catch (Throwable e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        latch.await();
        if (!errors.isEmpty()) {
            throw new MultiException("Error while downloading wars", errors);
        }
        return downloads;
    }

    /**
     * Recursively delete a directory and its contents or a file.
     * @param f
     * @return
     */
    private boolean deleteFileOrDirectory(File f) {
        if (f == null) {
            return false;
        } else if (f.isDirectory()) {
            File[] children = f.listFiles();
            for (File child : children) {
                if (!deleteFileOrDirectory(child)) {
                    return false;
                }
            }
            return f.delete();
        } else {
            return f.delete();
        }
    }
}
