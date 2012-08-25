/*
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
package org.fusesource.bai;

import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.management.event.AbstractExchangeEvent;
import org.fusesource.common.util.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the configuration of a kind of events
 */
public class EventTypeConfiguration {
    private boolean include = true;
    // TODO support exclude regex
    private List<String> includeRegexList = new ArrayList<String>();

    // TODO cache java.util.regex.Matcher objects for each expression!
    private List<Predicate> filters = new ArrayList<Predicate>();

    @Override
    public String toString() {
        return "EventTypeConfig(" + include + ", " + includeRegexList + ", " + filters + ")";
    }

    /**
     * Returns true if this configuration matches the given event and endpointURI
     */
    public boolean matchesEvent(String endpointUri, AbstractExchangeEvent exchangeEvent) {
        if (!isInclude()) {
            return false;
        }
        // if an include regex is specified then it matches if any of the match
        List<String> regexList = getIncludeRegexList();
        if (!regexList.isEmpty()) {
            if (endpointUri == null) {
                return false;
            }
            boolean matches = false;
            for (String regex : regexList) {
                if (endpointUri.matches(regex)) {
                    matches = true;
                    break;
                }
            }
            if (!matches) {
                return false;
            }
        }
        Exchange exchange = exchangeEvent.getExchange();
        if (exchange == null) {
            return false;
        }
        List<Predicate> filters = getFilters();
        if (filters.isEmpty()) {
            return true;
        } else {
            for (Predicate filter : filters) {
                if (filter.matches(exchange)) {
                    return true;
                }
            }
            return false;
        }
    }

    public List<Predicate> getFilters() {
        return filters;
    }

    public void setFilters(List<Predicate> filters) {
        this.filters = filters;
    }

    public boolean isInclude() {
        return include;
    }

    public void setInclude(boolean include) {
        this.include = include;
    }

    public List<String> getIncludeRegexList() {
        return includeRegexList;
    }

    public void setIncludeRegexList(List<String> includeRegexList) {
        this.includeRegexList = includeRegexList;
    }

    // Configuration API
    //-------------------------------------------------------------------------

    /**
     * Sets the event include flag to true/false (true if empty)
     */
    public void configureEventFlag(String value) {
        include = Strings.isNullOrBlank(value) || !value.equalsIgnoreCase("false");
    }

    public void addFilter(Predicate predicate) {
        getFilters().add(predicate);
    }

    public void addIncludeRegex(String regex) {
        getIncludeRegexList().add(regex);
    }

}
