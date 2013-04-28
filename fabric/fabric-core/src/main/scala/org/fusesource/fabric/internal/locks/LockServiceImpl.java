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
package org.fusesource.fabric.internal.locks;

import org.apache.curator.framework.CuratorFramework;
import org.fusesource.fabric.api.Lock;
import org.fusesource.fabric.api.LockService;
import org.fusesource.fabric.zookeeper.IZKClient;

import java.util.HashMap;
import java.util.Map;

public class LockServiceImpl implements LockService {

    private final CuratorFramework curator;
    private final Map<String, Lock> locks = new HashMap<String, Lock>();

    public LockServiceImpl(CuratorFramework curator) {
        this.curator = curator;
    }

    @Override
    public synchronized Lock getLock(String path) {
        if (locks.containsKey(path)) {
            return locks.get(path);
        } else {
            locks.put(path, new LockImpl(curator, path));
            return locks.get(path);
        }
    }
}
