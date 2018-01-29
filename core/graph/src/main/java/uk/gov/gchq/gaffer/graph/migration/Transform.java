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

import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class", defaultImpl = Transform.class)
public class Transform {
    private Function function;
    private String selection;
    private String projection;

    public Transform() {
        this.function = null;
    }

    public Transform(final Function function, final String selection, final String projection) {
        this.function = function;
        this.selection = selection;
        this.projection = projection;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public void setFunction(final Function function) {
        this.function = function;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Function getFunction() {
        return function;
    }

    public String getSelection(){
        return selection;
    }

    public void setSelection(final String selection){
        this.selection = selection;
    }

    public String getProjection(){
        return projection;
    }

    public void setProjection(final String projection){
        this.projection = projection;
    }
}
