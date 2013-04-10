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
package org.fusesource.fabric.git.datastore;

import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CheckoutCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.CreateBranchCommand;
import org.eclipse.jgit.api.DeleteBranchCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.api.RmCommand;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Ref;
import org.fusesource.fabric.api.CreateContainerMetadata;
import org.fusesource.fabric.api.DataStore;
import org.fusesource.fabric.api.FabricException;
import org.fusesource.fabric.api.FabricRequirements;
import org.fusesource.fabric.git.FabricGitService;
import org.fusesource.fabric.internal.DataStoreHelpers;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GitDataStore implements DataStore {

    private FabricGitService gitService;
    private DataStore delegate;

    public void setGitService(FabricGitService gitService) {
        this.gitService = gitService;
    }

    public void setDelegate(DataStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<String> getContainers() {
        return delegate.getContainers();
    }

    @Override
    public boolean hasContainer(String containerId) {
        return delegate.hasContainer(containerId);
    }

    @Override
    public String getContainerParent(String containerId) {
        return delegate.getContainerParent(containerId);
    }

    @Override
    public void deleteContainer(String containerId) {
        delegate.deleteContainer(containerId);
    }

    @Override
    public void createContainerConfig(CreateContainerMetadata metadata) {
        delegate.createContainerConfig(metadata);
    }

    @Override
    public CreateContainerMetadata getContainerMetadata(String containerId) {
        return delegate.getContainerMetadata(containerId);
    }

    @Override
    public String getContainerVersion(String containerId) {
        return delegate.getContainerVersion(containerId);
    }

    @Override
    public void setContainerVersion(String containerId, String versionId) {
        delegate.setContainerVersion(containerId, versionId);
    }

    @Override
    public String getContainerAttribute(String containerId, ContainerAttribute attribute, String def, boolean mandatory, boolean substituted) {
        return delegate.getContainerAttribute(containerId, attribute, def, mandatory, substituted);
    }

    @Override
    public void setContainerAttribute(String containerId, ContainerAttribute attribute, String value) {
        delegate.setContainerAttribute(containerId, attribute, value);
    }

    @Override
    public boolean isContainerAlive(String id) {
        return delegate.isContainerAlive(id);
    }

    @Override
    public void setContainerProfiles(String containerId, List<String> profileIds) {
        delegate.setContainerProfiles(containerId, profileIds);
    }

    @Override
    public List<String> getContainerProfiles(String containerId) {
        return delegate.getContainerProfiles(containerId);
    }

    @Override
    public String getDefaultVersion() {
        return delegate.getDefaultVersion();
    }

    @Override
    public void setDefaultVersion(String versionId) {
        delegate.setDefaultVersion(versionId);
    }

    @Override
    public String getDefaultJvmOptions() {
        return delegate.getDefaultJvmOptions();
    }

    @Override
    public void setDefaultJvmOptions(String jvmOptions) {
        delegate.setDefaultJvmOptions(jvmOptions);
    }

    @Override
    public FabricRequirements getRequirements() {
        return delegate.getRequirements();
    }

    @Override
    public void setRequirements(FabricRequirements requirements) throws IOException {
        delegate.setRequirements(requirements);
    }

    @Override
    public void importFromFileSystem(String from) {
        try {
            File rootTemp = createTempDir();

        } catch (Exception e) {
            throw new FabricException(e);
        }
        // TODO
    }

    private File createTempDir() throws IOException {
        File file = File.createTempFile("fabric", null);
        file.delete();
        file.mkdirs();
        return file;
    }

    protected void pullIfNeeded() {
        // TODO
    }

    protected void pushIfNeeded() {
        // TODO
    }

    private void checkout(Git git, String version) throws IOException, GitAPIException {
        CheckoutCommand checkout = git.checkout();
        checkout.setForce(true);
        checkout.setName(version);
        checkout.call();
    }

    private void delete(File dir) {
        if (dir.isDirectory()) {
            for (File child : dir.listFiles()) {
                delete(child);
            }
        }
        if (dir.exists()) {
            dir.delete();
        }
    }

    private byte[] readFully(File file) throws IOException {
        InputStream is = new FileInputStream(file);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int l;
        while ((l = is.read(buffer)) >= 0) {
            os.write(buffer, 0, l);
        }
        is.close();
        os.close();
        return os.toByteArray();
    }

    private void writeFully(File file, byte[] data) throws IOException {
        FileOutputStream os = new FileOutputStream(file);
        os.write(data);
        os.close();
    }

    @Override
    public List<String> getVersions() {
        try {
            pullIfNeeded();
            List<String> versions = new ArrayList<String>();
            Git git = gitService.get();
            ListBranchCommand lb = git.branchList();
            for (Ref ref : lb.call()) {
                String name = ref.getName();
                if (name.startsWith("refs/heads/")) {
                    name = name.substring("refs/heads/".length());
                    if (!name.equals("master")) {
                        versions.add(name);
                    }
                }
            }
            return versions;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void trackConfiguration(Runnable callback) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasVersion(String name) {
        return getVersions().contains(name);
    }

    @Override
    public void createVersion(String version) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            CreateBranchCommand cb = git.branchCreate();
            cb.setStartPoint("refs/heads/master");
            cb.setName(version);
            cb.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void createVersion(String parentVersionId, String toVersion) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            CreateBranchCommand cb = git.branchCreate();
            cb.setName(toVersion);
            cb.setStartPoint(parentVersionId);
            cb.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void deleteVersion(String version) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, "master");
            DeleteBranchCommand db = git.branchDelete();
            db.setBranchNames(version);
            db.setForce(true);
            db.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    public static final String METADATA = ".metadata";

    @Override
    public Map<String, String> getVersionAttributes(String version) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), METADATA);
            Properties props = new Properties();
            if (file.isFile()) {
                props.load(new FileInputStream(file));
            }
            Map<String, String> map = new HashMap<String, String>();
            for (String key : props.stringPropertyNames()) {
                map.put(key, props.getProperty(key));
            }
            return map;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setVersionAttribute(String version, String key, String value) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), METADATA);
            Properties props = new Properties();
            if (file.isFile()) {
                props.load(new FileInputStream(file));
            }
            props.setProperty(key, value);
            props.store(new FileOutputStream(file), "");
            AddCommand add = git.add();
            add.addFilepattern(METADATA);
            add.call();
            CommitCommand commit = git.commit();
            commit.setMessage("setVersionAttribute");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public List<String> getProfiles(String version) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles");
            List<String> profiles = new ArrayList<String>();
            String[] names = file.list();
            if (names != null) {
                Collections.addAll(profiles, names);
            }
            return profiles;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public boolean hasProfile(String version, String profile) {
        return getProfiles(version).contains(profile);
    }

    @Override
    public void createProfile(String version, String profile) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File root = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            root.mkdirs();
            new FileOutputStream(new File(root, METADATA)).close();
            AddCommand add = git.add();
            add.addFilepattern("profiles/" + profile + "/" + METADATA);
            add.call();
            CommitCommand commit = git.commit();
            commit.setMessage("createProfile");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public String getProfile(String version, String profile, boolean create) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File root = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            if (!root.exists() && create) {
                root.mkdirs();
                new FileOutputStream(new File(root, METADATA)).close();
                AddCommand add = git.add();
                add.addFilepattern("profiles/" + profile + "/" + METADATA);
                add.call();
                CommitCommand commit = git.commit();
                commit.setMessage("createProfile");
                commit.call();
                pushIfNeeded();
            }
            return root.exists() ? profile : null;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void deleteProfile(String version, String profile) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File root = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            if (root.exists()) {
                delete(root);
                RmCommand rm = git.rm();
                rm.addFilepattern("profiles/" + profile);
                rm.call();
                CommitCommand commit = git.commit();
                commit.setMessage("deleteProfile");
                commit.call();
                pushIfNeeded();
            }
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, String> getProfileAttributes(String version, String profile) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile + "/" + METADATA);
            Properties props = new Properties();
            if (file.isFile()) {
                props.load(new FileInputStream(file));
            }
            Map<String, String> map = new HashMap<String, String>();
            for (String key : props.stringPropertyNames()) {
                map.put(key, props.getProperty(key));
            }
            return map;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setProfileAttribute(String version, String profile, String key, String value) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile + "/" + METADATA);
            Properties props = new Properties();
            if (file.isFile()) {
                props.load(new FileInputStream(file));
            }
            props.setProperty(key, value);
            props.store(new FileOutputStream(file), "");
            AddCommand add = git.add();
            add.addFilepattern("profiles/" + profile + "/" + METADATA);
            add.call();
            CommitCommand commit = git.commit();
            commit.setMessage("setVersionAttribute");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, byte[]> getFileConfigurations(String version, String profile) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            Map<String, byte[]> configs = new HashMap<String, byte[]>();
            for (File f : file.listFiles()) {
                if (!f.getName().equals(METADATA)) {
                    configs.put(f.getName(), readFully(f));
                }
            }
            return configs;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public byte[] getFileConfiguration(String version, String profile, String name) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile + "/" + name);
            return readFully(file);
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setFileConfigurations(String version, String profile, Map<String, byte[]> configurations) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            boolean added = false, removed = false;
            AddCommand add = git.add();
            RmCommand rm = git.rm();
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            List<String> oldConfigs = new ArrayList<String>();
            Collections.addAll(oldConfigs, file.list());
            for (String name : configurations.keySet()) {
                File cfg = new File(file, name);
                writeFully(cfg, configurations.get(name));
                oldConfigs.remove(name);
                add.addFilepattern("profiles/" + profile + "/" + name);
                added = true;
            }
            for (String name : oldConfigs) {
                delete(new File(file, name));
                rm.addFilepattern("profiles/" + profile + "/" + name);
                removed = true;
            }
            if (added) {
                add.call();
            }
            if (removed) {
                rm.call();
            }
            CommitCommand commit = git.commit();
            commit.setMessage("setFileConfigurations");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setFileConfiguration(String version, String profile, String name, byte[] configuration) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            File cfg = new File(file, name);
            writeFully(cfg, configuration);
            AddCommand add = git.add();
            add.addFilepattern("profiles/" + profile + "/" + name);
            add.call();
            CommitCommand commit = git.commit();
            commit.setMessage("setFileConfiguration");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public Map<String, Map<String, String>> getConfigurations(String version, String profile) {
        try {
            Map<String, Map<String, String>> configurations = new HashMap<String, Map<String, String>>();
            Map<String, byte[]> configs = getFileConfigurations(version, profile);
            for (Map.Entry<String, byte[]> entry: configs.entrySet()){
                if(entry.getKey().endsWith(".properties")) {
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
        byte[] data = getFileConfiguration(version, profile, pid + ".properties");
        try {
            if (data != null) {
                return DataStoreHelpers.toMap(DataStoreHelpers.toProperties(data));
            }
            return null;
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setConfigurations(String version, String profile, Map<String, Map<String, String>> configurations) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            boolean added = false, removed = false;
            AddCommand add = git.add();
            RmCommand rm = git.rm();
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            List<String> oldConfigs = new ArrayList<String>();
            Collections.addAll(oldConfigs, file.list());
            for (String pid : configurations.keySet()) {
                String name = pid + ".properties";
                File cfg = new File(file, name);
                byte[] data = DataStoreHelpers.toBytes(DataStoreHelpers.toProperties(configurations.get(pid)));
                writeFully(cfg, data);
                oldConfigs.remove(name);
                add.addFilepattern("profiles/" + profile + "/" + name);
                added = true;
            }
            for (String name : oldConfigs) {
                if (name.endsWith(".properties")) {
                    delete(new File(file, name));
                    rm.addFilepattern("profiles/" + profile + "/" + name);
                    removed = true;
                }
            }
            if (added) {
                add.call();
            }
            if (removed) {
                rm.call();
            }
            CommitCommand commit = git.commit();
            commit.setMessage("setConfigurations");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void setConfiguration(String version, String profile, String pid, Map<String, String> configuration) {
        try {
            pullIfNeeded();
            Git git = gitService.get();
            checkout(git, version);
            File file = new File(git.getRepository().getWorkTree(), "profiles/" + profile);
            String name = pid + ".properties";
            File cfg = new File(file, name);
            byte[] data = DataStoreHelpers.toBytes(DataStoreHelpers.toProperties(configuration));
            writeFully(cfg, data);
            AddCommand add = git.add();
            add.addFilepattern("profiles/" + profile + "/" + name);
            add.call();
            CommitCommand commit = git.commit();
            commit.setMessage("setConfiguration");
            commit.call();
            pushIfNeeded();
        } catch (Exception e) {
            throw new FabricException(e);
        }
    }

    @Override
    public void substituteConfigurations(final Map<String, Map<String, String>> configs) {
        delegate.substituteConfigurations(configs);
    }

    private static BundleContext getBundleContext() {
        try {
            return FrameworkUtil.getBundle(GitDataStore.class).getBundleContext();
        } catch (Throwable t) {
            return null;
        }
    }

}
