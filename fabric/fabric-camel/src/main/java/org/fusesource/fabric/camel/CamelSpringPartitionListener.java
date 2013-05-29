/*
 * Copyright 2010 Red Hat, Inc.
 *
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.fusesource.fabric.camel;

import org.apache.camel.component.properties.PropertiesComponent;
import org.fusesource.fabric.partition.Partition;
import org.fusesource.fabric.partition.PartitionListener;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.osgi.context.support.OsgiBundleXmlApplicationContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CamelSpringPartitionListener implements PartitionListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CamelSpringPartitionListener.class);

    private static final String TYPE = "camel-spring";
    private final Map<Partition, ConfigurableApplicationContext> tasks = new HashMap<Partition, ConfigurableApplicationContext>();

    private final BundleContext bundleContext;

    public CamelSpringPartitionListener(BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void start(String taskId, String taskDefinition, Set<Partition> partitions) {
        for (Partition partition : partitions) {
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                ConfigurableApplicationContext context = createContext(taskDefinition, partition);
                ConfigurableApplicationContext oldContext = tasks.put(partition, context);
                if (oldContext != null) {
                    oldContext.close();
                }
            } catch (Exception e) {
                LOGGER.error("Failed to start camel context.", e);
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }

    }


    @Override
    public void stop(String taskId, String taskDefinition, Set<Partition> partitions) {
        for (Partition partition : partitions) {
            ConfigurableApplicationContext context = tasks.get(partition);
            if (context != null) {
                try {
                    context.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to start camel context.", e);
                }
            }
        }
    }

    /**
     * Creates a {@link ConfigurableApplicationContext} for the specified route definition and {@link Partition} data.
     * @param routeDefinition       The route definition with the url to the route template.
     * @param partition             The partition, that will be feed to the template.
     * @return
     * @throws Exception
     */
    ConfigurableApplicationContext createContext(String routeDefinition, final Partition partition) throws Exception {
        //Create a parent context
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerSingleton("properties", createPropertiesComponent(partition));
        GenericApplicationContext parentContext = new GenericApplicationContext(beanFactory);
        parentContext.refresh();

        OsgiBundleXmlApplicationContext  applicationContext = new OsgiBundleXmlApplicationContext(new String[]{routeDefinition}, parentContext);
        applicationContext.setBundleContext(bundleContext);
        applicationContext.setAllowBeanDefinitionOverriding(true);
        applicationContext.refresh();
        return applicationContext;
    }


    /**
     * Creates a {@link PropertiesComponent} containing the {@link Partition} data as overrides.
     * @param partition
     * @return
     */
    static PropertiesComponent createPropertiesComponent(Partition partition) {
        PropertiesComponent pc = new PropertiesComponent();
        pc.setOverrideProperties(partitionToProperties(partition));
        pc.setLocation("undefined");
        pc.setIgnoreMissingLocation(true);
        return pc;
    }

    /**
     * Creates a {@link Properties} object from the {@link Partition}.
     *
     * @param partition
     * @return
     */
    static Properties partitionToProperties(Partition partition) {
        Properties properties = new Properties();
        for(Map.Entry<String, String> entry : partition.getData().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
