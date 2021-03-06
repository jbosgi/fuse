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

import java.util.List;
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.fusesource.fabric.api.Container;
import org.fusesource.fabric.api.Profile;
import org.fusesource.fabric.boot.commands.support.FabricCommand;

import static org.fusesource.fabric.utils.FabricValidations.validateContainersName;
import static org.fusesource.fabric.utils.FabricValidations.validateProfileName;

@Command(name = "container-add-profile", scope = "fabric", description = "Adds the specified profile to the container list of profiles.")
public class ContainerAddProfile extends FabricCommand {

    @Argument(index = 0, name = "container", description = "The container name", required = true, multiValued = false)
    private String container;

    @Argument(index = 1, name = "profiles", description = "The profiles to add to the container.", required = true, multiValued = true)
    private List<String> profiles;

    protected Object doExecute() throws Exception {
        checkFabricAvailable();
        validateContainersName(container);
        validateProfileName(profiles);

        Container cont = getContainer(container);
        Profile[] profs = getProfiles(cont.getVersion(), this.profiles);
        cont.addProfiles(profs);
        return null;
    }

}
