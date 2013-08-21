package org.fusesource.fabric.zookeeper.utils;

import org.osgi.framework.BundleContext;

import java.util.Map;

public class OsgiInterpolationHelper {

    public OsgiInterpolationHelper() {
    }

    /**
     * Perform substitution on a property set
     *
     * @param properties the property set to perform substitution on
     */
    public static void performSubstitution(Map<String, String> properties, InterpolationHelper.SubstitutionCallback callback) {
        for (String name : properties.keySet()) {
            String value = properties.get(name);
            properties.put(name, InterpolationHelper.substVars(value, name, null, properties, callback));
        }
    }

    /**
     * Perform substitution on a property set
     *
     * @param properties the property set to perform substitution on
     */
    public static void performSubstitution(Map<String, String> properties) {
        performSubstitution(properties, (BundleContext) null);
    }

    /**
     * Perform substitution on a property set
     *
     * @param properties the property set to perform substitution on
     */
    public static void performSubstitution(Map<String, String> properties, final BundleContext context) {
        performSubstitution(properties, new InterpolationHelper.SubstitutionCallback() {
            public String getValue(String key) {
                String value = null;
                if (context != null) {
                    value = context.getProperty(key);
                }
                if (value == null) {
                    value = System.getProperty(value, "");
                }
                return value;
            }
        });
    }
}
