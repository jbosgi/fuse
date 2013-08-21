package org.fusesource.fabric.tomcat.agent;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.fusesource.fabric.zookeeper.IZKClient;
import org.fusesource.fabric.zookeeper.ZkPath;
import org.fusesource.fabric.zookeeper.utils.InterpolationHelper;
import org.linkedin.zookeeper.client.LifecycleListener;
import org.linkedin.zookeeper.tracker.NodeEvent;
import org.linkedin.zookeeper.tracker.NodeEventsListener;
import org.linkedin.zookeeper.tracker.TrackedNode;
import org.linkedin.zookeeper.tracker.ZKStringDataReader;
import org.linkedin.zookeeper.tracker.ZooKeeperTreeTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class ConfigurationBridge implements NodeEventsListener<String>, LifecycleListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationBridge.class);

    public static final String PARENTS = "parents"; // = Profile.PARENTS;
    public static final String DELETED = "#deleted#";
    public static final String FABRIC_ZOOKEEPER_PID = "fabric.zookeeper.pid";
    public static final String PROFILE_PROP_REGEX = "profile:[\\w\\.\\-]*/[\\w\\.\\-]*";
    private static final String AGENT_PID = "org.fusesource.fabric.agent";
    private static final String PROPERITES_SUFFIX = ".properties";

    private IZKClient zooKeeper;
    private String name = System.getProperty("tomcat.name");
    private String version;
    private String node;
    private String resolutionPolicy;
    private Map<String, ZooKeeperTreeTracker<String>> trees = new ConcurrentHashMap<String, ZooKeeperTreeTracker<String>>();
    private TomcatDeploymentAgent agent;
    private volatile boolean tracking = false;


    public void init() throws Exception {
        getZooKeeper().registerListener(this);
    }

    public void destroy() throws Exception {
        for (ZooKeeperTreeTracker<String> tree : trees.values()) {
            tree.destroy();
        }
        trees.clear();
        getZooKeeper().removeListener(this);
    }

    public void onConnected() {
        try {
            trees = new ConcurrentHashMap<String, ZooKeeperTreeTracker<String>>();
            tracking = true;
            try {
                // Find our root node
                String versionNode = ZkPath.CONFIG_CONTAINER.getPath(name);
                if (zooKeeper.exists(versionNode) == null) {
                    ZkPath.createContainerPaths(zooKeeper, name, null, "fabric");
                }
                version = zooKeeper.getStringData(versionNode);
                if (version == null) {
                    throw new IllegalStateException("Configuration for node " + name + " not found at " + ZkPath.CONFIG_CONTAINER.getPath(name));
                }
                track(versionNode);
                node = ZkPath.CONFIG_VERSIONS_CONTAINER.getPath(version, name);
                if (zooKeeper.exists(node) == null) {
                    zooKeeper.createWithParents(node, CreateMode.PERSISTENT);
                }
                track(node);
                resolutionPolicy = ZkPath.CONTAINER_RESOLVER.getPath(name);
                track(resolutionPolicy);
            } finally {
                tracking = false;
            }
            onEvents(null);
        } catch (Exception e) {
            LOGGER.warn("Exception when tracking configurations. This exception will be ignored.", e);
        }
    }

    public void onDisconnected() {
    }

    protected ZooKeeperTreeTracker<String> track(String path) throws InterruptedException, KeeperException, IOException {
        ZooKeeperTreeTracker<String> tree = trees.get(path);
        if (tree == null) {
            if (zooKeeper.exists(path) != null) {
                tree = new ZooKeeperTreeTracker<String>(zooKeeper, new ZKStringDataReader(), path);
                trees.put(path, tree);
                tree.track(this);
                String[] parents = getParents(tree.getTree().get(path));
                for (String parent : parents) {
                    track(ZkPath.CONFIG_VERSIONS_PROFILE.getPath(version, parent));
                }
            } else {
                // If the node does not exist yet, we track the parent to make
                // sure we receive the node creation event
                String p = ZkPath.CONFIG_VERSIONS_PROFILES.getPath(version);
                if (!trees.containsKey(p)) {
                    tree = new ZooKeeperTreeTracker<String>(zooKeeper, new ZKStringDataReader(), p, 1);
                    trees.put(p, tree);
                    tree.track(this);
                }
                return null;
            }
        }

        return tree;
    }

    public static Properties toProperties(String source) throws IOException {
        Properties rc = new Properties();
        rc.load(new StringReader(source));
        return rc;
    }

    public static  String stripSuffix(String value, String suffix) {
        if (value.endsWith(suffix)) {
            return value.substring(0, value.length() - suffix.length());
        } else {
            return value;
        }
    }

    public Map<String, Properties> load(Set<String> pids) throws IOException {
        final Map<String, Properties> configs = new HashMap<String, Properties>();
        for (String pid : pids) {
            try {
                Properties properties = new Properties();
                load(pid, node, properties);
                configs.put(pid, properties);
            } catch (InterruptedException e) {
                throw (IOException) new InterruptedIOException("Error loading pid " + pid).initCause(e);
            } catch (KeeperException e) {
                throw (IOException) new IOException("Error loading pid " + pid).initCause(e);
            }
        }

        for (Map.Entry<String, Properties> entry : configs.entrySet()) {
            Map props = entry.getValue();
            InterpolationHelper.performSubstitution(props, new InterpolationHelper.SubstitutionCallback() {
                public String getValue(String key) {
                    if (key.startsWith("zk:")) {
                        try {
                            return new String(ZkPath.loadURL(zooKeeper, key), "UTF-8");
                        } catch (KeeperException.ConnectionLossException e) {
                            throw new RuntimeException(e);
                        } catch (Exception e) {
                            LOGGER.warn("Could not load zk value: {}. This exception will be ignored.", key, e);
                        }
                    } else if (key.matches(PROFILE_PROP_REGEX)) {
                        String pid = key.substring("profile:".length(), key.indexOf('/'));
                        String propertyKey = key.substring(key.indexOf('/') + 1);
                        Map targetProps = configs.get(pid);
                        if (targetProps != null && targetProps.containsKey(propertyKey)) {
                            return (String) targetProps.get(propertyKey);
                        } else {
                            return key;
                        }
                    } else {
                        return "";
                    }
                    return "";
                }
            });
        }
        return configs;
    }

    private void load(String pid, String node, Dictionary dict) throws KeeperException, InterruptedException, IOException {
        ZooKeeperTreeTracker<String> tree = track(node);
        TrackedNode<String> root = tree != null ? tree.getTree().get(node) : null;
        String[] parents = getParents(root);
        for (String parent : parents) {
            load(pid, ZkPath.CONFIG_VERSIONS_PROFILE.getPath(version, parent), dict);
        }
        TrackedNode<String> cfg = tree != null ? tree.getTree().get(node + "/" + pid + PROPERITES_SUFFIX) : null;
        if (cfg != null) {
            //if (cfg != null && !DELETED.equals(cfg.getData())) {
            Properties properties = toProperties(cfg.getData());

            // clear out the dict if it had a deleted key.
            if (properties.remove(DELETED) != null) {
                Enumeration keys = dict.keys();
                while (keys.hasMoreElements()) {
                    dict.remove(keys.nextElement());
                }
            }

            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                if (DELETED.equals(entry.getValue())) {
                    dict.remove(entry.getKey());
                } else {
                    dict.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }

    private String[] getParents(TrackedNode<String> root) throws IOException {
        String[] parents;
        if (root != null && root.getData() != null) {
            Properties props = toProperties(root.getData());
            // For compatibility, check if we have instead the list of parents
            if (props.size() == 1) {
                String key = props.stringPropertyNames().iterator().next();
                if (!key.equals(PARENTS)) {
                    String val = props.getProperty(key);
                    props.remove(key);
                    props.setProperty(PARENTS, val.isEmpty() ? key : key + " " + val);
                }
            }
            parents = props.getProperty(PARENTS, "").split(" ");
        } else {
            parents = new String[0];
        }
        return parents;
    }

    private Set<String> getPids() throws KeeperException, InterruptedException, IOException {
        Set<String> pids = new HashSet<String>();
        getPids(node, pids);
        return pids;
    }

    private void getPids(String node, Set<String> pids) throws KeeperException, InterruptedException, IOException {
        ZooKeeperTreeTracker<String> tree = track(node);
        TrackedNode<String> root = tree != null ? tree.getTree().get(node) : null;
        String[] parents = getParents(root);
        for (String parent : parents) {
            getPids(ZkPath.CONFIG_VERSIONS_PROFILE.getPath(version, parent), pids);
        }
        for (String pid : getChildren(tree, node)) {
            if (pid.endsWith(PROPERITES_SUFFIX)) {
                pid = stripSuffix(pid, PROPERITES_SUFFIX);
                pids.add(pid);
            }
        }
    }

    protected List<String> getChildren(ZooKeeperTreeTracker<String> tree, String node) {
        List<String> children = new ArrayList<String>();
        if (tree != null) {
            Pattern p = Pattern.compile(node + "/[^/]*");
            for (String c : tree.getTree().keySet()) {
                if (p.matcher(c).matches()) {
                    children.add(c.substring(c.lastIndexOf('/') + 1));
                }
            }
        }
        return children;
    }

    public void onEvents(Collection<NodeEvent<String>> nodeEvents) {
        LOGGER.trace("onEvents", nodeEvents);
        try {
            if (!tracking) {
                String version = zooKeeper.getStringData(ZkPath.CONFIG_CONTAINER.getPath(name));

                if (zooKeeper.exists(ZkPath.CONTAINER_IP.getPath(name)) != null) {
                    String resolutionPointer = zooKeeper.getStringData(ZkPath.CONTAINER_IP.getPath(name));
                    resolutionPolicy = zooKeeper.getStringData(ZkPath.CONTAINER_RESOLVER.getPath(name));
                    if (resolutionPointer == null || !resolutionPointer.contains(resolutionPolicy)) {
                        zooKeeper.setData(ZkPath.CONTAINER_IP.getPath(name), "${zk:" + name + "/" + resolutionPolicy + "}");
                    }
                }

                if (!this.version.equals(version)) {
                    this.version = version;
                    node = ZkPath.CONFIG_VERSIONS_CONTAINER.getPath(version, name);
                    track(node);
                }
                final Set<String> pids = getPids();
                Map<String, Properties> pidProperties = load(pids);
                for (Map.Entry<String, Properties> entry : pidProperties.entrySet()) {
                    String pid = entry.getKey();
                    Properties conf = entry.getValue();
                    saveProperties(pid, conf);
                    if (AGENT_PID.equals(pid)) {
                        agent.updated(conf);
                    }
                }
            }
            LOGGER.trace("onEvents done");
        } catch (Exception e) {
            LOGGER.warn("Exception when tracking configurations. This exception will be ignored.", e);
        }
    }

    private void saveProperties(String pid, Properties properties) {
        String fileName = System.getProperty("catalina.base") + File.separator + "conf" + File.separator + pid + PROPERITES_SUFFIX;
        File propertyFile = new File(fileName);
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(propertyFile);
            properties.store(fos, pid);
        } catch (IOException ex) {
            //noop
        } finally {
            try {
                fos.close();
            } catch (IOException ex) {
                //noop
            }
        }
    }

    public IZKClient getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(IZKClient zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TomcatDeploymentAgent getAgent() {
        return agent;
    }

    public void setAgent(TomcatDeploymentAgent agent) {
        this.agent = agent;
    }
}
