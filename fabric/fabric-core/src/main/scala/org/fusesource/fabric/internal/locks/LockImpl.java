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
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.fusesource.fabric.api.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LockImpl implements Lock {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockImpl.class);

    private final String path;
    private final CuratorFramework curator;
    private final InterProcessMutex mutex;


    /**
     * Constructor
     *
     * @param curator
     * @param path
     */
    protected LockImpl(CuratorFramework curator, String path) {
        this.path = path;
        this.curator = curator;
        this.mutex = new InterProcessMutex(curator, path);

    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
        try {
            mutex.acquire(time, unit);
            return true;
        } catch (Exception e) {
           return false;
        }
    }

    @Override
    public void unlock() {
        try {
            mutex.release();
        } catch (Exception e) {
            //release
        }
    }
}
