/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.migration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class", defaultImpl = SchemaMapping.class)
public class SchemaMapping {
    private String currentGroup;
    private String newGroup;
    private String selection;
    private String projection;

    public SchemaMapping() {
    }

    public SchemaMapping(final String selection, final String newGroup, final String projection) {
        this.selection = selection;
        this.newGroup = newGroup;
        this.projection = projection;
    }

    public String getCurrentGroup() {
        return currentGroup;
    }

    public void setCurrentGroup(final String currentGroup) {
        this.currentGroup = currentGroup;
    }

    public String getNewGroup() {
        return newGroup;
    }

    public void setNewGroup(final String newGroup) {
        this.newGroup = newGroup;
    }

    public String getProjection() {
        return projection;
    }

    public void setProjection(final String projection) {
        this.projection = projection;
    }

    public String getSelection() {
        return selection;
    }

    public void setSelection(final String selection) {
        this.selection = selection;
    }
}
