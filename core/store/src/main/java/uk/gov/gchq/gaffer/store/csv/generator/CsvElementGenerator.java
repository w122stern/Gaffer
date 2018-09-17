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

package uk.gov.gchq.gaffer.store.csv.generator;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.store.csv.fieldmapping.*;
import uk.gov.gchq.gaffer.data.type.TypeConstructorRegistry;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.csv.util.CsvUtil;
import uk.gov.gchq.gaffer.store.csv.buckets.BucketFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvElementGenerator implements OneToManyElementGenerator<String> {

    private Schema gafferSchema;
    private FieldMapping fieldMapping;
    private String delimiter = ",";
    private String quotesChar = "\"";
    private boolean includeQuotes = true;
    private TypeConstructorRegistry typeConstructorRegistry;

    public CsvElementGenerator(Schema gafferSchema, FieldMapping fieldMapping){
        setFieldMapping(fieldMapping);
        setGafferSchema(gafferSchema);
    }

    public CsvElementGenerator(Schema gafferSchema, FieldMapping fieldMapping, String delimiter, boolean ignoreQuotes, String quotesChar){
        setDelimiter(delimiter);
        setFieldMapping(fieldMapping);
        setGafferSchema(gafferSchema);
        setDelimiter(delimiter);
        setIncludeQuotes(ignoreQuotes);
        setQuotesChar(quotesChar);
    }

    public CsvElementGenerator(){}

    public Iterable<Element> _apply(String s) {
        List<Element> elements = new ArrayList<Element>();
        List<Edge> edges = new ArrayList<Edge>();
        List<Entity> entities = new ArrayList<Entity>();
        List<String> fields = CsvUtil.parseCSV(s, fieldMapping.getHeader().numFields(), delimiter, quotesChar, includeQuotes);
        for(String group : fieldMapping.getEdges().keySet()){
            edges = constructEdges(fields, gafferSchema, fieldMapping.getEdges().get(group),group, fieldMapping.getHeader());
        }
        elements.addAll(edges);
        for(String group : fieldMapping.getEntities().keySet()){
            entities = constructEntities(fields, gafferSchema, fieldMapping.getEntities().get(group), group, fieldMapping.getHeader());
        }
        elements.addAll(entities);
        return elements;

    }

    public void setGafferSchema(Schema gafferSchema){
        this.gafferSchema = gafferSchema;
    }

    public void setFieldMapping(FieldMapping fieldMapping){
        this.fieldMapping = fieldMapping;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getQuotesChar() {
        return quotesChar;
    }

    public void setQuotesChar(String quotesChar) {
        this.quotesChar = quotesChar;
    }

    public boolean isIncludeQuotes() {
        return includeQuotes;
    }

    public void setIncludeQuotes(boolean includeQuotes) {
        this.includeQuotes = includeQuotes;
    }

    private List<Edge> constructEdges(List<String> fields, Schema gafferSchema, EdgeMapping edgeMapping, String group, Header header){

        List<Edge> edges = new ArrayList<Edge>(edgeMapping.getSource().size());
        for(int i=0;i<edgeMapping.getSource().size();i++) {
            Edge edge = new Edge(group);
            Integer sourceIndex = header.getIndex(edgeMapping.getSource().get(i));
            String sourceString = fields.get(sourceIndex);
            Integer destinationIndex = header.getIndex(edgeMapping.getDestination().get(i));
            String destinationString = fields.get(destinationIndex);
            boolean directed = Boolean.parseBoolean(gafferSchema.getElement(group).getIdentifierMap().get(IdentifierType.DIRECTED));
            Object source = getIdentifierAsType(sourceString, IdentifierType.SOURCE, group, gafferSchema);
            Object destination = getIdentifierAsType(destinationString, IdentifierType.DESTINATION, group, gafferSchema);
            edge.setIdentifiers(source, destination, directed);

            Map<String, Object> properties = getProperties(header, edgeMapping, gafferSchema, i, fields, group);
            for(String propertyName : properties.keySet()){
                edge.putProperty(propertyName, properties.get(propertyName));
            }
            edges.add(edge);

        }
        return edges;
    }

    private List<Entity> constructEntities(List<String> fields, Schema gafferSchema, EntityMapping entityMapping, String group, Header header){
        List<Entity> entities = new ArrayList<Entity>(entityMapping.getVertex().size());
        for(int i=0;i<entityMapping.getVertex().size();i++) {
            Entity entity = new Entity(group);
            Integer vertexIndex = header.getIndex(entityMapping.getVertex().get(i));
            String vertexString = fields.get(vertexIndex);
            Object vertex = getIdentifierAsType(vertexString, IdentifierType.VERTEX, group, gafferSchema);
            entity.setVertex(vertex);
            Map<String, Object> properties = getProperties(header, entityMapping, gafferSchema, i, fields, group);
            for(String propertyName : properties.keySet()){
                entity.putProperty(propertyName, properties.get(propertyName));
            }
            entities.add(entity);
        }
        return entities;
    }

    private Map<String, Object> getProperties(Header header, ElementMapping elementMapping, Schema gafferSchema, int index, List<String> fields, String group){
        Map<String, Object> properties = new HashMap<String, Object>();
        for(String propertyName : elementMapping.getProperties().keySet()){
            String propertyFieldName = elementMapping.getProperties().get(propertyName).get(index);
            Object propertyValue;
            if(propertyFieldName.equals("none")){
                propertyValue = getPropertyAsType("none", propertyName,group, gafferSchema);
            }else{
                Integer propertyIndex = header.getIndex(propertyFieldName);
                String fieldValue = fields.get(propertyIndex);
                if(elementMapping.getTimePropertyNames().contains(propertyName)){
                    fieldValue = convertTimeToEpochMillisString(fields.get(propertyIndex));
                }
                propertyValue = getPropertyAsType(fieldValue, propertyName,group, gafferSchema);
                if(gafferSchema.getElement(group).getGroupBy().contains(propertyName)){
                    propertyValue = applyBucketing(propertyValue, elementMapping.getGroupByFunctions().get(propertyName));
                }
            }
            properties.put(propertyName, propertyValue);


        }
        for(String countPropertyName : elementMapping.getCountProperties()){
            Object propertyValue = getPropertyAsType("1", countPropertyName, group, gafferSchema);
            properties.put(countPropertyName, propertyValue);
        }
        return properties;
    }

    private Object applyBucketing(Object propertyValue, GroupByConfig bucketFunctionConfig) {

        BucketFunction bucketFunction = null;
        try {
            bucketFunction = (BucketFunction) Class.forName(bucketFunctionConfig.getBucketFunctionClassName()).newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        bucketFunction.setBucketSize(bucketFunctionConfig.getBucketSize());
        return bucketFunction.getBucket(propertyValue);
    }

    private String convertTimeToEpochMillisString(String field) {
        TimeConfig timeConfig = fieldMapping.getTimeConfig();
        Long timestamp = 0L;
        if(timeConfig.getFormat() != null){
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(fieldMapping.getTimeConfig().getFormat());
            LocalDateTime date = LocalDateTime.parse(field, formatter);
            timestamp = date.atZone(ZoneId.systemDefault())
                    .toInstant().toEpochMilli();
        }else{
            timestamp = (Long.valueOf(field) + timeConfig.getTimestampOffset())*timeConfig.getMillisMultiplier();
        }
        return String.valueOf(timestamp);
    }

    private Object getPropertyAsType(String field, String propertyName, String group, Schema gafferSchema) {
        SchemaElementDefinition elementDefinition = gafferSchema.getElement(group);
        String typeName = elementDefinition.getPropertyTypeName(propertyName);
        String className = gafferSchema.getType(typeName).getFullClassString();
        return constructType(field, className);
    }

    private Object getIdentifierAsType(String input, IdentifierType elementFieldName, String group, Schema gafferSchema){
        SchemaElementDefinition elementDefinition = gafferSchema.getElement(group);
        String typeName = elementDefinition.getIdentifierTypeName(elementFieldName);
        String className = gafferSchema.getType(typeName).getFullClassString();
        return constructType(input, className);
    }

    private Object constructType(String input, String className) {

        try {
            return typeConstructorRegistry.getTypeConstructor(className).constructType(input);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Schema getGafferSchema() {
        return gafferSchema;
    }

    public FieldMapping getFieldMapping() {
        return fieldMapping;
    }

    public TypeConstructorRegistry getTypeConstructorRegistry() {
        return typeConstructorRegistry;
    }

    public void setTypeConstructorRegistry(TypeConstructorRegistry registry){
        this.typeConstructorRegistry = registry;
    }
}

