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
package org.fusesource.fabric.groups;

public interface Group {

    /**
     * Registers a change listener which will be called
     * when the cluster membership changes.
     */
    void add(ChangeListener listener);

    /**
     * Removes a previously added change listener.
     */
    void remove(ChangeListener listener);

    /**
     * Are we connected with the cluster?
     */
    boolean connected();

    /**
     * A group should be closed to release aquired resources used
     * to monitor the group membership.
     *
     * Whe the Group is closed, any memberships registered via this
     * Group will be removed from the group.
     */
    void close();

    <T extends NodeState> Singleton<T> createSingleton(Class<T> clazz);

    <T extends NodeState> Member<T> createMember(Class<T> clazz);

}
