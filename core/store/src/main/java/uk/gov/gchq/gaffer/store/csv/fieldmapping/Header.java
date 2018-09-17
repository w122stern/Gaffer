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

import java.util.HashMap;
import java.util.Map;

public class Header {

    private Map<String,Integer> headerAsMap;

    public Header(String header){
        setHeader(header);
    }

    public Header(){
        headerAsMap = new HashMap<String, Integer>();
    }

    public Map<String, Integer> getHeader() {
        return headerAsMap;
    }

    public int numFields(){
        return headerAsMap.keySet().size();
    }

    @JsonIgnore
    public void setHeader(Map<String, Integer> headerAsMap) {
        this.headerAsMap = headerAsMap;
    }

    @JsonIgnore
    public void setHeader(String header, String delimiter){
        this.headerAsMap = parseHeader(header, delimiter);
    }

    public void setHeader(String header){
        this.headerAsMap = parseHeader(header, ",");
    }

    public Integer getIndex(String field){
        return headerAsMap.get(field);
    }

    private Map<String,Integer> parseHeader(String inputString, String delimiter){
        String[] t = inputString.split(delimiter);
        Map<String, Integer> map = new HashMap<String, Integer>();
        int i = 0;
        for(String s : t){
            map.put(s, i);
            i++;
        }
        return map;
    }

    @Override
    public String toString() {
        return "Header{" +
                "headerAsMap=" + headerAsMap +
                '}';
    }
}
