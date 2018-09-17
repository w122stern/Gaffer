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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class FieldMapping {

    private Header header;
    private Map<String, EdgeMapping> edges;
    private Map<String,EntityMapping> entities;
    private TimeConfig timeConfig;

    public FieldMapping(){}

    public FieldMapping(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        FieldMapping fieldMapping = mapper.readValue(json, FieldMapping.class);
        header = fieldMapping.getHeader();
        edges = fieldMapping.getEdges();
        entities = fieldMapping.getEntities();
        timeConfig = fieldMapping.getTimeConfig();
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public Map<String, EdgeMapping> getEdges() {
        return edges;
    }

    public void setEdges(Map<String, EdgeMapping> edges) {
        this.edges = edges;
    }

    public Map<String, EntityMapping> getEntities() {
        return entities;
    }

    public void setEntities(Map<String, EntityMapping> entities) {
        this.entities = entities;
    }

    public TimeConfig getTimeConfig() {
        return timeConfig;
    }

    public void setTimeConfig(TimeConfig timeConfig) {
        this.timeConfig = timeConfig;
    }

    @Override
    public String toString() {
        return "FieldMapping{" +
                "header=" + header +
                ", edges=" + edges +
                ", entities=" + entities +
                ", timeConfig=" + timeConfig +
                '}';
    }
}
