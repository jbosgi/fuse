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
package org.fusesource.mq.fabric;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.aries.blueprint.container.BlueprintContainerImpl;
import org.apache.aries.blueprint.container.SimpleNamespaceHandlerSet;
import org.apache.aries.blueprint.parser.NamespaceHandlerSet;
import org.apache.xbean.blueprint.context.impl.XBeanNamespaceHandler;
import org.apache.xbean.spring.context.ResourceXmlApplicationContext;
import org.osgi.framework.FrameworkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class BrokerLoader {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerLoader.class);

    public static interface EmbeddedBroker {

        BrokerService broker();

        long lastModified() throws IOException;

        void close();

    }

    public static EmbeddedBroker createBroker(String uri, Properties properties) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        Document document;
        URL url;
        File file = new File(uri);
        if (file.exists()) {
            url = file.toURI().toURL();
            document = dbf.newDocumentBuilder().parse(file);
        } else {
            url = new URL(uri);
            document = dbf.newDocumentBuilder().parse(uri);
        }
        String root = document.getDocumentElement().getNodeName();
        // Spring case
        if ("beans".equals(root)) {
            return createSpringBroker(uri, properties);
        // Blueprint case
        } else if ("blueprint".equals(root)) {
            return createBlueprintBroker(url, properties);
        } else {
            throw new IllegalArgumentException("Unrecognized root element: " + root);
        }
    }

    private static EmbeddedBroker createBlueprintBroker(URL url, Properties properties) throws Exception {
        Map<String, String> props = getMap(properties);
        BlueprintContainerImpl container = new BlueprintContainerImpl(
                    BrokerService.class.getClassLoader(),
                    Collections.singletonList(url), props, true) {
            @Override
            protected NamespaceHandlerSet createNamespaceHandlerSet(Set<URI> namespaces) {
                SimpleNamespaceHandlerSet handlerSet = new SimpleNamespaceHandlerSet();
                try {
                    XBeanNamespaceHandler ns = new XBeanNamespaceHandler(
                            "http://activemq.apache.org/schema/core",
                            "/activemq.xsd",
                            FrameworkUtil.getBundle(BrokerService.class),
                            "META-INF/services/org/apache/xbean/spring/http/activemq.apache.org/schema/core"
                    );
                    handlerSet.addNamespace(
                            URI.create("http://activemq.apache.org/schema/core"),
                            BrokerService.class.getResource("/activemq.xsd"),
                            ns);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to create namespace handler", e);
                }
                // Check namespaces
                Set<URI> unsupported = new LinkedHashSet<URI>();
                for (URI ns : namespaces) {
                    if (!handlerSet.getNamespaces().contains(ns)) {
                        unsupported.add(ns);
                    }
                }
                if (unsupported.size() > 0) {
                    throw new IllegalArgumentException("Unsupported namespaces: " + unsupported.toString());
                }
                return handlerSet;
            }
        };

        BrokerService broker = null;
        for (String id : container.getComponentIds()) {
            Object obj = container.getComponentInstance(id);
            if (obj instanceof BrokerService) {
                broker = (BrokerService) obj;
                break;
            }
        }
        if (broker == null) {
            throw new IllegalArgumentException("Configuration did not contain a BrokerService");
        }
        String[] networks = properties.getProperty("network", "").split(",");
        for (String name : networks) {
            if (!name.isEmpty()) {
                LOG.info("Adding network connector " + name);
                DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(new URI("fabric:" + name));
                nc.setName("fabric-" + name);
                IntrospectionSupport.setProperties(nc, getMap(properties), "network.");
                broker.addNetworkConnector(nc);
            }
        }
        return new BlueprintEmbeddedBroker(container, broker, url);
    }

    private static Map<String, String> getMap(Properties properties) {
        Map<String, String> props = new HashMap<String, String>();
        for (String key : properties.stringPropertyNames()) {
            props.put(key, properties.getProperty(key));
        }
        return props;
    }

    protected static EmbeddedBroker createSpringBroker(String uri, Properties properties) throws Exception {
        ConfigurationProperties.CONFIG_PROPERTIES.set(properties);
        try {
            Thread.currentThread().setContextClassLoader(BrokerService.class.getClassLoader());
            Resource resource = Utils.resourceFromString(uri);
            ResourceXmlApplicationContext ctx = new ResourceXmlApplicationContext((resource)) {
                protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
                    reader.setValidating(false);
                }
            };
            String[] names = ctx.getBeanNamesForType(BrokerService.class);
            if (names == null || names.length == 0) {
                throw new IllegalArgumentException("Configuration did not contain a BrokerService");
            }

            BrokerService broker = ctx.getBean(names[0], BrokerService.class);
            String[] networks = properties.getProperty("network", "").split(",");
            for (String name : networks) {
                if (!name.isEmpty()) {
                    LOG.info("Adding network connector " + name);
                    DiscoveryNetworkConnector nc = new DiscoveryNetworkConnector(new URI("fabric:" + name));
                    nc.setName("fabric-" + name);
                    IntrospectionSupport.setProperties(nc, getMap(properties), "network.");
                    broker.addNetworkConnector(nc);
                }
            }
            return new SpringEmbeddedBroker(ctx, broker, resource);
        } finally {
            ConfigurationProperties.CONFIG_PROPERTIES.remove();
        }
    }

    public static class SpringEmbeddedBroker implements EmbeddedBroker {
        private final ResourceXmlApplicationContext context;
        private final BrokerService broker;
        private final Resource resource;

        public SpringEmbeddedBroker(ResourceXmlApplicationContext context, BrokerService broker, Resource resource) {
            this.context = context;
            this.broker = broker;
            this.resource = resource;
        }

        @Override
        public BrokerService broker() {
            return broker;
        }

        @Override
        public long lastModified() throws IOException {
            return resource.lastModified();
        }

        @Override
        public void close() {
            context.close();
        }
    }

    public static class BlueprintEmbeddedBroker implements EmbeddedBroker {
        private final BlueprintContainerImpl container;
        private final BrokerService broker;
        private final URL url;

        public BlueprintEmbeddedBroker(BlueprintContainerImpl container, BrokerService broker, URL url) {
            this.container = container;
            this.broker = broker;
            this.url = url;
        }

        @Override
        public BrokerService broker() {
            return broker;
        }

        @Override
        public long lastModified() throws IOException {
            URLConnection con = url.openConnection();
            con.setUseCaches(false);
            if (con instanceof HttpURLConnection) {
                ((HttpURLConnection) con).setRequestMethod("HEAD");
            }
            return con.getLastModified();
        }

        @Override
        public void close() {
            container.destroy();
        }
    }

}
