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

package org.fusesource.eca.eventcache;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.impl.ServiceSupport;

public class DefaultEventCacheManager extends ServiceSupport implements EventCacheManager {

    private final Map<Object, EventCache<?>> caches = new HashMap<Object, EventCache<?>>();

    public synchronized <T> EventCache<T> getCache(Class<T> type, Object id, String size) {
        EventCache result = caches.get(id);
        if (result == null) {
            result = new DefaultEventCache<T>(id, size);
            caches.put(id, result);
        }
        return result;
    }

    /**
     * retrieve an existing cache
     *
     * @return the cache or null, if it doesn't exist
     */
    public <T> EventCache<T> lookupCache(Class<T> type, Object id) {
        EventCache result = caches.get(id);
        return result;
    }

    public synchronized boolean removeCache(Object id) {
        EventCache result = caches.remove(id);
        return result != null;
    }

    @Override
    protected void doStart() throws Exception {
        // noop
    }

    @Override
    protected void doStop() throws Exception {
        caches.clear();
    }
}
