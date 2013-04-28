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
package org.fusesource.fabric.service;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.fusesource.fabric.api.CreateContainerMetadata;
import org.fusesource.fabric.api.CreateContainerOptions;
import org.fusesource.fabric.api.DataStore;
import org.fusesource.fabric.api.FabricException;
import org.fusesource.fabric.api.FabricRequirements;
import org.fusesource.fabric.internal.DataStoreHelpers;
import org.fusesource.fabric.internal.RequirementsJson;
import org.fusesource.fabric.utils.Base64Encoder;
import org.fusesource.fabric.utils.Closeables;
import org.fusesource.fabric.utils.ObjectUtils;
import org.fusesource.fabric.zookeeper.ZkDefs;
import org.fusesource.fabric.zookeeper.ZkProfiles;
import org.fusesource.fabric.zookeeper.utils.CuratorImportUtils;
import org.fusesource.fabric.zookeeper.utils.CuratorUtils;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fusesource.fabric.zookeeper.ZkPath.CONFIGS;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIGS_CONTAINERS;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_CONTAINER;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_DEFAULT_VERSION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_ENSEMBLE_PROFILES;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_VERSION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_VERSIONS;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_VERSIONS_CONTAINER;
import static org.fusesource.fabric.zookeeper.ZkPath.CONFIG_VERSIONS_PROFILES;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_ALIVE;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_DOMAINS;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_ENTRY;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_GEOLOCATION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_HTTP;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_IP;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_JMX;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_LOCAL_HOSTNAME;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_LOCAL_IP;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_LOCATION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_MANUAL_IP;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_METADATA;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PARENT;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PORT_MAX;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PORT_MIN;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PROVISION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PROVISION_EXCEPTION;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PROVISION_LIST;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PROVISION_RESULT;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PUBLIC_HOSTNAME;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_PUBLIC_IP;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_RESOLVER;
import static org.fusesource.fabric.zookeeper.ZkPath.CONTAINER_SSH;
import static org.fusesource.fabric.zookeeper.ZkPath.POLICIES;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.copy;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.create;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.deleteSafe;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.exists;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.get;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.getProperties;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.getPropertiesAsMap;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.getSubstitutedPath;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.set;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.setProperties;
import static org.fusesource.fabric.zookeeper.utils.CuratorUtils.setPropertiesAsMap;

/**
 * @author Stan Lewis
 */
public class ZooKeeperDataStore extends SubstitutionSupport implements DataStore, PathChildrenCacheListener {

    public static final String REQUIREMENTS_JSON_PATH = "/fabric/configs/org.fusesource.fabric.requirements.json";
    public static final String JVM_OPTIONS_PATH = "/fabric/configs/org.fusesource.fabric.containers.jvmOptions";

    private CuratorFramework curator;
    private final ConcurrentMap<String, PathChildrenCache> treeCache = new ConcurrentHashMap<String, PathChildrenCache>();
    private final List<Runnable> callbacks = new ArrayList<Runnable>();
    private final AtomicInteger pendingEvents = new AtomicInteger(0);

    public void init() throws Exception {

    }

    public void destroy() throws IOException {
        for (PathChildrenCache cache : treeCache.values()) {
            Closeables.closeQuitely(cache);
        }
        treeCache.clear();
    }

    @Override
    public void importFromFileSystem(String from) {
        try {
            CuratorImportUtils.importFromFileSystem(curator, from, "/", null, null, false, false, false);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    public void bind(CuratorFramework curator) throws Exception {
        String connectionString = curator.getZookeeperClient().getCurrentConnectionString();
        if (connectionString != null && !connectionString.isEmpty()) {
            PathChildrenCache cache = new PathChildrenCache(curator, CONFIGS.getPath(), true);
            cache.getListenable().addListener(this);
            cache.start();
        }
    }

    public void unbind(CuratorFramework curator) throws IOException {
        for (PathChildrenCache cache : treeCache.values()) {
            Closeables.closeQuitely(cache);
        }
        treeCache.clear();
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        //If more children are added track those too.
        if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            String path = event.getData().getPath();
            PathChildrenCache cache = treeCache.putIfAbsent(path, new PathChildrenCache(curator, path, true));
            if (cache == null) {
                cache = treeCache.get(path);
                cache.getListenable().addListener(this);
                cache.start();
            }
        }

        if (pendingEvents.incrementAndGet() == 1) {
            try {
                List<Runnable> copyOfCallbacks = new ArrayList<Runnable>(callbacks);
                callbacks.clear();
                for (Runnable callback : copyOfCallbacks) {
                    try {
                        callback.run();
                    } catch (Throwable t) {
                        //ignore
                    }
                }
            } finally {
                pendingEvents.set(0);
            }
        }
    }

    @Override
    public void trackConfiguration(Runnable callback) {
        synchronized (callbacks) {
            callbacks.add(callback);
        }
    }

    @Override
    public List<String> getContainers() {
        try {
            return curator.getChildren().forPath(CONFIGS_CONTAINERS.getPath());
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public boolean hasContainer(String containerId) {
        return getContainers().contains(containerId);
    }

    @Override
    public String getContainerParent(String containerId) {
        try {
            String parentName = get(curator, CONTAINER_PARENT.getPath(containerId));
            return parentName != null ? parentName.trim() : "";
        } catch (KeeperException.NoNodeException e) {
            // Ignore
            return "";
        } catch (Throwable e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void deleteContainer(String containerId) {
        try {
            //Wipe all config entries that are related to the container for all versions.
            for (String version : getVersions()) {
                deleteSafe(curator, CONFIG_VERSIONS_CONTAINER.getPath(version, containerId));
            }
            deleteSafe(curator, CONFIG_CONTAINER.getPath(containerId));
            deleteSafe(curator, CONTAINER.getPath(containerId));
            deleteSafe(curator, CONTAINER_DOMAINS.getPath(containerId));
            deleteSafe(curator, CONTAINER_PROVISION.getPath(containerId));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void createContainerConfig(CreateContainerMetadata metadata) {
        try {
            CreateContainerOptions options = metadata.getCreateOptions();
            String containerId = metadata.getContainerName();
            String parent = options.getParent();
            String versionId = options.getVersion() != null ? options.getVersion() : getDefaultVersion();
            List<String> profileIds = options.getProfiles();
            if (profileIds == null || profileIds.isEmpty()) {
                profileIds = Collections.singletonList(ZkDefs.DEFAULT_PROFILE);
            }
            StringBuilder sb = new StringBuilder();
            for (String profileId : profileIds) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(profileId);
            }

            set(curator, CONFIG_CONTAINER.getPath(containerId), versionId);
            set(curator, CONFIG_VERSIONS_CONTAINER.getPath(versionId, containerId), sb.toString());
            set(curator, CONTAINER_PARENT.getPath(containerId), parent);

            //We encode the metadata so that they are more friendly to import/export.
            set(curator, CONTAINER_METADATA.getPath(containerId), Base64Encoder.encode(ObjectUtils.toBytes(metadata)));

            Map<String, String> configuration = metadata.getContainerConfiguration();
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                set(curator, CONTAINER_ENTRY.getPath(metadata.getContainerName(), key), value);
            }

            // If no resolver specified but a resolver is already present in the registry, use the registry value
            if (options.getResolver() == null && exists(curator, CONTAINER_RESOLVER.getPath(containerId)) != null) {
                options.setResolver(get(curator, CONTAINER_RESOLVER.getPath(containerId)));
            } else if (options.getResolver() != null) {
                // Use the resolver specified in the options and do nothing.
            } else if (exists(curator, POLICIES.getPath(ZkDefs.RESOLVER)) != null) {
                // If there is a globlal resolver specified use it.
                options.setResolver(get(curator, POLICIES.getPath(ZkDefs.RESOLVER)));
            } else {
                // Fallback to the default resolver
                options.setResolver(ZkDefs.DEFAULT_RESOLVER);
            }
            // Set the resolver if not already set
            set(curator, CONTAINER_RESOLVER.getPath(containerId), options.getResolver());
        } catch (FabricException e) {
            throw e;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public CreateContainerMetadata getContainerMetadata(String containerId) {
        try {
            byte[] encoded = curator.getData().forPath(CONTAINER_METADATA.getPath(containerId));
            byte[] decoded = Base64Encoder.decode(encoded);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decoded));
            return (CreateContainerMetadata) ois.readObject();
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public String getContainerVersion(String containerId) {
        try {
            return get(curator, CONFIG_CONTAINER.getPath(containerId));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setContainerVersion(String containerId, String versionId) {
        try {
            String oldVersionId = get(curator, CONFIG_CONTAINER.getPath(containerId));
            String oldProfileIds = get(curator, CONFIG_VERSIONS_CONTAINER.getPath(oldVersionId, containerId));

            set(curator, CONFIG_VERSIONS_CONTAINER.getPath(versionId, containerId), oldProfileIds);
            set(curator, CONFIG_CONTAINER.getPath(containerId), versionId);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public List<String> getContainerProfiles(String containerId) {
        try {
            String versionId = get(curator, CONFIG_CONTAINER.getPath(containerId));
            String str = get(curator, CONFIG_VERSIONS_CONTAINER.getPath(versionId, containerId));
            return str == null || str.isEmpty() ? Collections.<String>emptyList() : Arrays.asList(str.trim().split(" +"));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setContainerProfiles(String containerId, List<String> profileIds) {
        try {
            String versionId = get(curator, CONFIG_CONTAINER.getPath(containerId));
            StringBuilder sb = new StringBuilder();
            for (String profileId : profileIds) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(profileId);
            }
            set(curator, CONFIG_VERSIONS_CONTAINER.getPath(versionId, containerId), sb.toString());
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public boolean isContainerAlive(String id) {
        try {
            return exists(curator, CONTAINER_ALIVE.getPath(id)) != null;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public String getContainerAttribute(String containerId, ContainerAttribute attribute, String def, boolean mandatory, boolean substituted) {
        if (attribute == ContainerAttribute.Domains) {
            try {
                List<String> list = curator.getChildren().forPath(CONTAINER_DOMAINS.getPath(containerId));
                Collections.sort(list);
                StringBuilder sb = new StringBuilder();
                for (String l : list) {
                    if (sb.length() > 0) {
                        sb.append("\n");
                    }
                    sb.append(l);
                }
                return sb.toString();
            } catch (Exception e) {
                return "";
            }
        } else {
            try {
                if (substituted) {
                    return getSubstitutedPath(curator, getAttributePath(containerId, attribute));
                } else {
                    return get(curator, getAttributePath(containerId, attribute));
                }
            } catch (KeeperException.NoNodeException e) {
                if (mandatory) {
                    throw new FabricException(e);
                }
                return def;
            } catch (Exception e) {
                throw new FabricException(e);
            }
        }
    }

    @Override
    public void setContainerAttribute(String containerId, ContainerAttribute attribute, String value) {
        // Special case for resolver
        // TODO: we could use a double indirection on the ip so that it does not need to change
        // TODO: something like ${zk:container/${zk:container/resolver}}
        if (attribute == ContainerAttribute.Resolver) {
            try {
                set(curator, CONTAINER_IP.getPath(containerId), "${zk:" + containerId + "/" + value + "}");
                set(curator, CONTAINER_RESOLVER.getPath(containerId), value);
            } catch (Exception e) {
                throw new FabricException(e);
            }
        } else {
            try {
//                if (value == null) {
//                    ZooKeeperUtils.deleteSafe(zk, getAttributePath(containerId, attribute));
//                } else {
                set(curator, getAttributePath(containerId, attribute), value);
//                }
            } catch (KeeperException.NoNodeException e) {
                // Ignore
            } catch (Exception e) {
                throw new FabricException(e);
            }
        }
    }

    private String getAttributePath(String containerId, ContainerAttribute attribute) {
        switch (attribute) {
            case ProvisionStatus:
                return CONTAINER_PROVISION_RESULT.getPath(containerId);
            case ProvisionException:
                return CONTAINER_PROVISION_EXCEPTION.getPath(containerId);
            case ProvisionList:
                return CONTAINER_PROVISION_LIST.getPath(containerId);
            case Location:
                return CONTAINER_LOCATION.getPath(containerId);
            case GeoLocation:
                return CONTAINER_GEOLOCATION.getPath(containerId);
            case Resolver:
                return CONTAINER_RESOLVER.getPath(containerId);
            case Ip:
                return CONTAINER_IP.getPath(containerId);
            case LocalIp:
                return CONTAINER_LOCAL_IP.getPath(containerId);
            case LocalHostName:
                return CONTAINER_LOCAL_HOSTNAME.getPath(containerId);
            case PublicIp:
                return CONTAINER_PUBLIC_IP.getPath(containerId);
            case PublicHostName:
                return CONTAINER_PUBLIC_HOSTNAME.getPath(containerId);
            case ManualIp:
                return CONTAINER_MANUAL_IP.getPath(containerId);
            case JmxUrl:
                return CONTAINER_JMX.getPath(containerId);
            case HttpUrl:
                return CONTAINER_HTTP.getPath(containerId);
            case SshUrl:
                return CONTAINER_SSH.getPath(containerId);
            case PortMin:
                return CONTAINER_PORT_MIN.getPath(containerId);
            case PortMax:
                return CONTAINER_PORT_MAX.getPath(containerId);
            default:
                throw new IllegalArgumentException("Unsupported container attribute " + attribute);
        }
    }

    @Override
    public String getDefaultVersion() {
        try {
            String version = null;
            if (exists(curator, CONFIG_DEFAULT_VERSION.getPath()) != null) {
                version = get(curator, CONFIG_DEFAULT_VERSION.getPath());
            }
            if (version == null || version.isEmpty()) {
                version = ZkDefs.DEFAULT_VERSION;
                set(curator, CONFIG_DEFAULT_VERSION.getPath(), version);
                set(curator, CONFIG_VERSION.getPath(version), (String) null);
            }
            return version;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setDefaultVersion(String versionId) {
        try {
            set(curator, CONFIG_DEFAULT_VERSION.getPath(), versionId);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void createVersion(String version) {
        try {
            curator.create().forPath(CONFIG_VERSION.getPath(version));
            curator.create().forPath(CONFIG_VERSIONS_PROFILES.getPath(version));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void createVersion(String parentVersionId, String toVersion) {
        try {
            copy(curator, CONFIG_VERSION.getPath(parentVersionId), CONFIG_VERSION.getPath(toVersion));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void deleteVersion(String version) {
        try {
            curator.delete().guaranteed().forPath(CONFIG_VERSION.getPath(version));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public List<String> getVersions() {
        try {
            return curator.getChildren().forPath(CONFIG_VERSIONS.getPath());
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public boolean hasVersion(String name) {
        try {
            if (curator != null && curator.getZookeeperClient().isConnected() && exists(curator, CONFIG_VERSION.getPath(name)) == null) {
                return false;
            }
            return true;
        } catch (FabricException e) {
            throw e;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public List<String> getProfiles(String version) {
        try {
            List<String> profiles = new ArrayList<String>();
            profiles.addAll(curator.getChildren().forPath(CONFIG_ENSEMBLE_PROFILES.getPath()));
            profiles.addAll(curator.getChildren().forPath(CONFIG_VERSIONS_PROFILES.getPath(version)));
            return profiles;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public String getProfile(String version, String profile, boolean create) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            if (exists(curator, path) == null) {
                if (!create) {
                    return null;
                } else {
                    createProfile(version, profile);
                    return profile;
                }
            }
            return profile;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public boolean hasProfile(String version, String profile) {
        return getProfile(version, profile, false) != null;
    }

    @Override
    public void createProfile(String version, String profile) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            create(curator, path);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void deleteProfile(String version, String name) {
        try {
            String path = ZkProfiles.getPath(version, name);
            deleteSafe(curator, path);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, String> getVersionAttributes(String version) {
        try {
            String node = CONFIG_VERSION.getPath(version);
            return getPropertiesAsMap(curator, node);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setVersionAttribute(String version, String key, String value) {
        try {
            Map<String, String> props = getVersionAttributes(version);
            if (value != null) {
                props.put(key, value);
            } else {
                props.remove(key);
            }
            String node = CONFIG_VERSION.getPath(version);
            setPropertiesAsMap(curator, node, props);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }


    @Override
    public Map<String, String> getProfileAttributes(String version, String profile) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            return getPropertiesAsMap(curator, path);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setProfileAttribute(String version, String profile, String key, String value) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            Properties props = getProperties(curator, path);
            if (value != null) {
                props.setProperty(key, value);
            } else {
                props.remove(key);
            }
            setProperties(curator, path, props);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public long getLastModified(String version, String profile) {
        try {
            return CuratorUtils.getLastModified(curator, ZkProfiles.getPath(version, profile));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, byte[]> getFileConfigurations(String version, String profile) {
        try {
            Map<String, byte[]> configurations = new HashMap<String, byte[]>();
            String path = ZkProfiles.getPath(version, profile);
            List<String> pids = curator.getChildren().forPath(path);
            for (String pid : pids) {
                configurations.put(pid, getFileConfiguration(version, profile, pid));
            }
            return configurations;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public byte[] getFileConfiguration(String version, String profile, String pid) {
        try {
            String path = ZkProfiles.getPath(version, profile) + "/" + pid;
            if (exists(curator, path) == null) {
                return null;
            }
            if (curator.getData().forPath(path) == null) {
                List<String> children = curator.getChildren().forPath(path);
                StringBuilder buf = new StringBuilder();
                for (String child : children) {
                    String value = get(curator, path + "/" + child);
                    buf.append(String.format("%s = %s\n", child, value));
                }
                return buf.toString().getBytes();
            } else {
                return curator.getData().forPath(path);
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setFileConfigurations(String version, String profile, Map<String, byte[]> configurations) {
        try {
            Map<String, byte[]> oldCfgs = getFileConfigurations(version, profile);
            String path = ZkProfiles.getPath(version, profile);

            for (Map.Entry<String, byte[]> entry : configurations.entrySet()) {
                String pid = entry.getKey();
                oldCfgs.remove(pid);
                byte[] newCfg = entry.getValue();
                setFileConfiguration(version, profile, pid, newCfg);
            }

            for (String pid : oldCfgs.keySet()) {
                deleteSafe(curator, path + "/" + pid);
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setFileConfiguration(String version, String profile, String pid, byte[] configuration) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            String configPath = path + "/" + pid;
            if (exists(curator, configPath) != null && curator.getChildren().forPath(configPath).size() > 0) {
                List<String> kids = curator.getChildren().forPath(configPath);
                ArrayList<String> saved = new ArrayList<String>();
                // old format, we assume that the byte stream is in
                // a .properties format
                for (String line : new String(configuration).split("\n")) {
                    if (line.startsWith("#") || line.length() == 0) {
                        continue;
                    }
                    String nameValue[] = line.split("=", 2);
                    if (nameValue.length < 2) {
                        continue;
                    }
                    String newPath = configPath + "/" + nameValue[0].trim();
                    set(curator, newPath, nameValue[1].trim());
                    saved.add(nameValue[0].trim());
                }
                for (String kid : kids) {
                    if (!saved.contains(kid)) {
                        deleteSafe(curator, configPath + "/" + kid);
                    }
                }
            } else {
                set(curator, configPath, configuration);
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, Map<String, String>> getConfigurations(String version, String profile) {
        try {
            Map<String, Map<String, String>> configurations = new HashMap<String, Map<String, String>>();
            Map<String, byte[]> configs = getFileConfigurations(version, profile);
            for (Map.Entry<String, byte[]> entry : configs.entrySet()) {
                if (entry.getKey().endsWith(".properties")) {
                    String pid = DataStoreHelpers.stripSuffix(entry.getKey(), ".properties");
                    configurations.put(pid, DataStoreHelpers.toMap(DataStoreHelpers.toProperties(entry.getValue())));
                }
            }
            return configurations;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, String> getConfiguration(String version, String profile, String pid) {
        try {
            String path = ZkProfiles.getPath(version, profile) + "/" + pid + ".properties";
            if (exists(curator, path) == null) {
                return null;
            }
            byte[] data = curator.getData().forPath(path);
            return DataStoreHelpers.toMap(DataStoreHelpers.toProperties(data));
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setConfigurations(String version, String profile, Map<String, Map<String, String>> configurations) {
        try {
            Map<String, Map<String, String>> oldCfgs = getConfigurations(version, profile);
            // Store new configs
            String path = ZkProfiles.getPath(version, profile);
            for (Map.Entry<String, Map<String, String>> entry : configurations.entrySet()) {
                String pid = entry.getKey();
                oldCfgs.remove(pid);
                setConfiguration(version, profile, pid, entry.getValue());
            }
            for (String key : oldCfgs.keySet()) {
                deleteSafe(curator, path);
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setConfiguration(String version, String profile, String pid, Map<String, String> configuration) {
        try {
            String path = ZkProfiles.getPath(version, profile);
            byte[] data = DataStoreHelpers.toBytes(DataStoreHelpers.toProperties(configuration));
            String p = path + "/" + pid + ".properties";
            set(curator, p, data);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    public BundleContext getBundleContext() {
        try {
            return FrameworkUtil.getBundle(ZooKeeperDataStore.class).getBundleContext();
        } catch (Throwable t) {
            return null;
        }
    }

    @Override
    public String getDefaultJvmOptions() {
        try {
            if (curator.getZookeeperClient().isConnected() && exists(curator, JVM_OPTIONS_PATH) != null) {
                return get(curator, JVM_OPTIONS_PATH);
            } else {
                return "";
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setDefaultJvmOptions(String jvmOptions) {
        try {
            String opts = jvmOptions != null ? jvmOptions : "";
            set(curator, JVM_OPTIONS_PATH, opts);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public FabricRequirements getRequirements() {
        try {
            FabricRequirements answer = null;
            if (exists(curator, REQUIREMENTS_JSON_PATH) != null) {
                String json = get(curator, REQUIREMENTS_JSON_PATH);
                answer = RequirementsJson.fromJSON(json);
            }
            if (answer == null) {
                answer = new FabricRequirements();
            }
            return answer;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setRequirements(FabricRequirements requirements) throws IOException {
        try {
            requirements.removeEmptyRequirements();
            String json = RequirementsJson.toJSON(requirements);
            set(curator, REQUIREMENTS_JSON_PATH, json);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    public void setCurator(CuratorFramework curator) {
        this.curator = curator;
    }
}
