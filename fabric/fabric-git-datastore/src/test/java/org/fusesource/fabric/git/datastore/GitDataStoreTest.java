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

import org.eclipse.jgit.api.Git;
import org.fusesource.fabric.api.DataStore;
import org.fusesource.fabric.git.FabricGitService;
import org.fusesource.fabric.utils.Files;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GitDataStoreTest {

    private DataStore dataStore;

    @Before
    public void setUp() throws Exception {
        File localRepo = new File("target/data");
        delete(localRepo);
        final Git git = Git.init().setDirectory(localRepo).call();
        Files.writeToFile(new File(localRepo, "README"), "", Charset.forName("UTF-8"));
        git.add().addFilepattern("README").call();
        git.commit().setMessage("First Commit").setCommitter("fabric", "user@fabric").call();
        FabricGitService gitService = new FabricGitService() {
            @Override
            public Git get() throws IOException {
                return git;
            }
        };
        GitDataStore ds = new GitDataStore();
        ds.setGitService(gitService);
        dataStore = ds;
    }

    @Test
    public void testVersions() {
        assertEquals(0, dataStore.getVersions().size());
        dataStore.createVersion("1.0");
        assertEquals(1, dataStore.getVersions().size());
        assertEquals(0, dataStore.getVersionAttributes("1.0").size());
        dataStore.setVersionAttribute("1.0", "foo", "bar");
        assertEquals(1, dataStore.getVersionAttributes("1.0").size());
        assertEquals("bar", dataStore.getVersionAttributes("1.0").get("foo"));

        dataStore.createVersion("2.0");
        assertEquals(2, dataStore.getVersions().size());
        assertEquals(0, dataStore.getVersionAttributes("2.0").size());
        dataStore.deleteVersion("2.0");
        assertEquals(1, dataStore.getVersions().size());

        dataStore.createVersion("1.0", "2.0");
        assertEquals(2, dataStore.getVersions().size());
        assertEquals(1, dataStore.getVersionAttributes("2.0").size());
        assertEquals("bar", dataStore.getVersionAttributes("1.0").get("foo"));
        dataStore.deleteVersion("2.0");
        assertEquals(1, dataStore.getVersions().size());

        dataStore.deleteVersion("1.0");
        assertEquals(0, dataStore.getVersions().size());
    }

    @Test
    public void testProfiles() {
        assertEquals(0, dataStore.getVersions().size());
        dataStore.createVersion("1.0");
        assertEquals(1, dataStore.getVersions().size());
        assertEquals(0, dataStore.getProfiles("1.0").size());

        assertNull(dataStore.getProfile("1.0", "p1", false));
        assertEquals("p1", dataStore.getProfile("1.0", "p1", true));
        assertEquals(1, dataStore.getProfiles("1.0").size());
        assertEquals("p1", dataStore.getProfiles("1.0").get(0));

        assertEquals(0, dataStore.getProfileAttributes("1.0", "p1").size());
        dataStore.setProfileAttribute("1.0", "p1", "foo", "bar");
        assertEquals(1, dataStore.getProfileAttributes("1.0", "p1").size());
        assertEquals("bar", dataStore.getProfileAttributes("1.0", "p1").get("foo"));

        dataStore.deleteProfile("1.0", "p1");
        assertEquals(0, dataStore.getProfiles("1.0").size());

        dataStore.deleteVersion("1.0");
        assertEquals(0, dataStore.getVersions().size());
    }

    @Test
    public void testConfigurations() {
        assertEquals(0, dataStore.getVersions().size());
        dataStore.createVersion("1.0");
        dataStore.createProfile("1.0", "p1");

        assertEquals(0, dataStore.getFileConfigurations("1.0", "p1").size());
        dataStore.setFileConfiguration("1.0", "p1", "foo", "bar".getBytes());
        assertEquals(1, dataStore.getFileConfigurations("1.0", "p1").size());
        assertArrayEquals("bar".getBytes(), dataStore.getFileConfiguration("1.0", "p1", "foo"));

        Map<String, String> cfg = new HashMap<String, String>();
        cfg.put("foo", "bar");
        assertEquals(0, dataStore.getConfigurations("1.0", "p1").size());
        dataStore.setConfiguration("1.0", "p1", "pid", cfg);
        assertEquals(1, dataStore.getConfigurations("1.0", "p1").size());
        assertEquals(cfg, dataStore.getConfiguration("1.0", "p1", "pid"));

        dataStore.setConfigurations("1.0", "p1", new HashMap<String, Map<String, String>>());
        assertEquals(0, dataStore.getConfigurations("1.0", "p1").size());
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

}
