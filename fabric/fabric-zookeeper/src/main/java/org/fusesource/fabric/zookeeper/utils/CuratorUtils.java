package org.fusesource.fabric.zookeeper.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.fusesource.fabric.zookeeper.ZkPath;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class CuratorUtils {

    private CuratorUtils() {
        //Utility Class
    }

    public static void copy(CuratorFramework source, CuratorFramework dest, String path) throws Exception {
        for (String child : source.getChildren().forPath(path)) {
            child = path + "/" + child;
            if (dest.checkExists().forPath(child) == null) {
                byte[] data = source.getData().forPath(child);
                set(dest, child, data);
                copy(source, dest, child);
            }
        }
    }

    public static void copy(CuratorFramework curator, String from, String to) throws Exception {
        for (String child : curator.getChildren().forPath(from)) {
            String fromChild = from + "/" + child;
            String toChild = to + "/" + child;
            if (curator.checkExists().forPath(toChild) == null) {
                byte[] data = curator.getData().forPath(fromChild);
                set(curator, toChild, data);
                copy(curator, fromChild, toChild);
            }
        }
    }

    public static void add(CuratorFramework curator, String path, String value) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            curator.setData().forPath(path, value.getBytes());
        } else {
            String data = get(curator, path);
            if (data == null) {
                data = "";
            }
            if (data.length() > 0) {
                data += " ";
            }
            data += value;
            curator.setData().forPath(path, data.getBytes());
        }
    }

    public static void remove(CuratorFramework curator, String path, String value) throws Exception {
        if (curator.checkExists().forPath(path) != null) {
            List<String> parts = new LinkedList<String>();
            String data = get(curator, path);
            if (data != null) {
                parts = new ArrayList<String>(Arrays.asList(data.trim().split(" +")));
            }
            boolean changed = false;
            StringBuilder sb = new StringBuilder();
            for (Iterator<String> it = parts.iterator(); it.hasNext(); ) {
                String v = it.next();
                if (v.matches(value)) {
                    it.remove();
                    changed = true;
                }
            }
            if (changed) {
                sb.delete(0, sb.length());
                for (String part : parts) {
                    if (data.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(part);
                }
                set(curator, path, sb.toString());
            }
        }
    }

    public static String get(CuratorFramework curator, String path) throws Exception {
        byte[] bytes = curator.getData().forPath(path);
        if (bytes == null) {
            return null;
        } else {
            return new String(bytes);
        }
    }

    public static void set(CuratorFramework curator, String path, String value) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            curator.create().creatingParentsIfNeeded().forPath(path, value != null ? value.getBytes() : null);
        }
        curator.setData().forPath(path, value != null ? value.getBytes() : null);
    }

    public static void set(CuratorFramework curator, String path, byte[] value) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            curator.create().creatingParentsIfNeeded().forPath(path, value);
        }
        curator.setData().forPath(path, value);
    }

    public static void create(CuratorFramework curator, String path) throws Exception {
        curator.create().creatingParentsIfNeeded().forPath(path);
    }

    public static void createDefault(CuratorFramework curator, String path, String value) throws Exception {
        if (curator.checkExists().forPath(path) == null) {
            curator.create().creatingParentsIfNeeded().forPath(path,  value != null ? value.getBytes() : null);
        }
    }

    public static void deleteSafe(CuratorFramework curator, String path) throws Exception {
        if (curator.checkExists().forPath(path) != null) {
            for (String child : curator.getChildren().forPath(path)) {
                deleteSafe(curator, path + "/" + child);
            }
            curator.delete().forPath(path);
        }
    }

    public static Stat exists(CuratorFramework curator, String path) throws Exception {
        return curator.checkExists().forPath(path);
    }

    public static Properties getProperties(CuratorFramework curator, String path) throws Exception {
        return getProperties(curator, path, null);
    }

    public static Properties getProperties(CuratorFramework curator, String path, Watcher watcher) throws Exception {
        byte[] data = watcher != null ? curator.getData().usingWatcher(watcher).forPath(path) : curator.getData().forPath(path);
        String value = data != null ? new String(data) : "";
        Properties properties = new Properties();
        if (value != null) {
            try {
                properties.load(new StringReader(value));
            } catch (IOException ignore) {
            }
        }
        return properties;
    }

    public static Map<String, String> getPropertiesAsMap(CuratorFramework curator, String path) throws Exception {
        Properties properties = getProperties(curator, path);
        Map<String, String> map = new HashMap<String, String>();
        for (String key : properties.stringPropertyNames()) {
            map.put(key, properties.getProperty(key));
        }
        return map;
    }



    public static void setPropertiesAsMap(CuratorFramework curator, String path, Map<String, String> map) throws Exception {
        Properties properties = new Properties();
        for (String key : map.keySet()) {
            properties.put(key, map.get(key));
        }
        setProperties(curator, path, properties);
    }

    public static void setProperties(CuratorFramework curator, String path, Properties properties) throws Exception {
        try {
            org.apache.felix.utils.properties.Properties p = new org.apache.felix.utils.properties.Properties();
            String org = get(curator, path);
            if (org != null) {
                p.load(new StringReader(org));
            }
            List<String> keys = new ArrayList<String>();
            for (String key : properties.stringPropertyNames()) {
                p.put(key, properties.getProperty(key));
                keys.add(key);
            }
            List<String> deleted = new ArrayList<String>(p.keySet());
            deleted.removeAll(keys);
            for (String key : deleted) {
                p.remove(key);
            }
            StringWriter writer = new StringWriter();
            p.save(writer);
            set(curator, path, writer.toString());
        } catch (IOException e) {
        }
    }

    public static String getSubstitutedPath(final CuratorFramework curator, String path) throws Exception {
        String normalizedPath = path != null && path.contains("#") ? path.substring(0, path.lastIndexOf('#')) : path;
        if (normalizedPath != null && curator.checkExists().forPath(normalizedPath) != null) {
            byte[] data = ZkPath.loadURL(curator, path);
            if (data != null && data.length > 0) {
                String str = new String(ZkPath.loadURL(curator, path), "UTF-8");
                return getSubstitutedData(curator, str);
            }
        }
        return null;
    }

    public static String getSubstitutedData(final CuratorFramework curator, String data) throws URISyntaxException {
        Map<String, String> props = new HashMap<String, String>();
        props.put("data", data);

        InterpolationHelper.performSubstitution(props, new InterpolationHelper.SubstitutionCallback() {
            @Override
            public String getValue(String key) {
                if (key.startsWith("zk:")) {
                    try {
                        return new String(ZkPath.loadURL(curator, key), "UTF-8");
                    } catch (Exception e) {
                        //ignore and just return null.
                    }
                }
                return null;
            }
        });
        return props.get("data");
    }

    /**
     * Generate a random String that can be used as a Zookeeper password.
     *
     * @return
     */
    public static String generatePassword() {
        StringBuilder password = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            long l = Math.round(Math.floor(Math.random() * (26 * 2 + 10)));
            if (l < 10) {
                password.append((char) ('0' + l));
            } else if (l < 36) {
                password.append((char) ('A' + l - 10));
            } else {
                password.append((char) ('a' + l - 36));
            }
        }
        return password.toString();
    }

    /**
     * Returns the last modified time of the znode taking childs into consideration.
     *
     * @param curator
     * @param path
     * @return
     * @throws org.apache.zookeeper.KeeperException
     * @throws InterruptedException
     */
    public static long getLastModified(CuratorFramework curator, String path) throws Exception {
        long lastModified = 0;
        List<String> children = curator.getChildren().forPath(path);
        if (children.isEmpty()) {
            return curator.checkExists().forPath(path).getMtime();
        } else {
            for (String child : children) {
                lastModified = Math.max(getLastModified(curator, path + "/" + child), lastModified);
            }
        }
        return lastModified;
    }


    private static String CONTAINERS_NODE = "/fabric/authentication/containers";

    public static String getContainerLogin(String container) {
        return "container#" + container;
    }

    public static boolean isContainerLogin(String login) {
        return login.startsWith("container#");
    }

    public static Properties getContainerTokens(CuratorFramework curator) throws Exception {
        Properties props = new Properties();
        for (String key : curator.getChildren().forPath(CONTAINERS_NODE)) {
            props.setProperty("container#" + key, get(curator, CONTAINERS_NODE + "/" + key));
        }
        return props;
    }

    public static String generateContainerToken(CuratorFramework curator, String container) throws Exception {
        String password = generatePassword();
        set(curator, CONTAINERS_NODE + "/" + container, password);
        return password;
    }
}
