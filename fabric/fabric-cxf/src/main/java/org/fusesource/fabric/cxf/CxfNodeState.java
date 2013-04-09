package org.fusesource.fabric.cxf;

import org.codehaus.jackson.annotate.JsonProperty;
import org.fusesource.fabric.groups.NodeState;

public class CxfNodeState implements NodeState {

    @JsonProperty
    String id;

    @JsonProperty
    String agent;

    @JsonProperty
    String[] services;

    @JsonProperty
    String url;

    @Override
    public String id() {
        return id;
    }
}
