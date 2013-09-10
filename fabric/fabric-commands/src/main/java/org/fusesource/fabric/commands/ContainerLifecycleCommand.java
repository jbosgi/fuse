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
package org.fusesource.fabric.commands;

import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Option;
import org.fusesource.fabric.api.Container;
import org.fusesource.fabric.api.CreateContainerMetadata;
import org.fusesource.fabric.boot.commands.support.FabricCommand;

public abstract class ContainerLifecycleCommand extends FabricCommand {

    @Option(name = "--user", description = "The username to use.")
    String user;

    @Option(name = "--password", description = "The password to use.")
    String password;

    @Argument(index = 0, name = "container", description = "The container name", required = true, multiValued = false)
    String container = null;

    void applyUpdatedCredentials(Container container) {
        if (user != null || password != null) {
            CreateContainerMetadata metadata = container.getMetadata();
            if (metadata != null) {
                metadata.updateCredentials(user, password);
                container.setMetadata(metadata);
            }
        }
    }
}