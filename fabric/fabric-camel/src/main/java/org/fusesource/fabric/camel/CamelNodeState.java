package org.fusesource.fabric.camel;

import org.codehaus.jackson.annotate.JsonProperty;
import org.fusesource.fabric.groups.NodeState;

public class CamelNodeState implements NodeState {

    @JsonProperty
    String id;

    @JsonProperty
    String agent;

    @JsonProperty
    String[] services;

    @JsonProperty
    String processor;

    @Override
    public String id() {
        return id;
    }

}
