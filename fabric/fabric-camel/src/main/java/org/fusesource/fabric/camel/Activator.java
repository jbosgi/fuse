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
package org.fusesource.fabric.camel;

import org.fusesource.fabric.partition.PartitionListener;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.util.Dictionary;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class Activator implements BundleActivator {

    public static final AtomicReference<BundleContext> BUNDLE_CONTEXT = new AtomicReference<BundleContext>();



    private ServiceRegistration registration;

	public void start(BundleContext ctx) throws Exception {
        BUNDLE_CONTEXT.set(ctx);
        registration = registerCamelSpringListener(ctx);
	}

    public void stop(BundleContext ctx) throws Exception {
        BUNDLE_CONTEXT.set(null);
        if (registration != null) {
            registration.unregister();
        }
    }

    ServiceRegistration registerCamelSpringListener(BundleContext ctx) {
        Dictionary props = new Properties();
        PartitionListener camelPartitionListener = new CamelSpringPartitionListener(ctx);
        props.put("type", camelPartitionListener.getType());
        return ctx.registerService(PartitionListener.class.getName(), camelPartitionListener, props);
    }
}

