/*
 * Copyright 2016-2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.store.csv.fieldmapping;

import com.fasterxml.jackson.annotation.JsonIgnore;
import uk.gov.gchq.gaffer.store.csv.buckets.BucketFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElementMapping {

    private Map<String, List<String>> properties;
    private List<String> timePropertyNames;
    private BucketFunction bucketFunction;
    private Map<String, GroupByConfig> groupByFunctions;
    private List<String> countProperties;

    public ElementMapping(){
        this.properties = new HashMap<String, List<String>>();
        this.timePropertyNames = new ArrayList<String>();
    }

    public Map<String, List<String>> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, List<String>> properties) {
        this.properties = properties;
    }

    public List<String> getTimePropertyNames() {
        return timePropertyNames;
    }

    public void setTimePropertyNames(List<String> timePropertyNames) {
        this.timePropertyNames = timePropertyNames;
    }

    @JsonIgnore
    public BucketFunction getBucketFunction() {
        return bucketFunction;
    }

    @JsonIgnore
    public void setBucketFunction(BucketFunction bucketFunction) {
        this.bucketFunction = bucketFunction;
    }

    public Map<String, GroupByConfig> getGroupByFunctions() {
        return groupByFunctions;
    }

    public void setGroupByFunctions(Map<String, GroupByConfig> groupByFunctions) {
        this.groupByFunctions = groupByFunctions;
    }

    public List<String> getCountProperties() {
        return countProperties;
    }

    public void setCountProperties(List<String> countProperties) {
        this.countProperties = countProperties;
    }

    @Override
    public String toString() {
        return "ElementMapping{" +
                "properties=" + properties +
                ", timePropertyNames=" + timePropertyNames +
                ", bucketFunction=" + bucketFunction +
                ", groupByFunctions=" + groupByFunctions +
                ", countProperties=" + countProperties +
                '}';
    }
}
