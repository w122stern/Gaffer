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

package uk.gov.gchq.gaffer.graph.hook;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.collections.MapUtils;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.function.migration.MigrateElements;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonPropertyOrder(alphabetic = true)
public class SchemaMigration implements GraphHook {
    private Map<String, List<MigrateElements>> elementTypeToMigrationsMap = new HashMap<>();
    Map<String, List<ElementFilter>> groupToElementFiltersMap = new HashMap<>();

    public void setMigrations(final Map<String, List<MigrateElements>> elementTypeToMigrationsMap) {
        this.elementTypeToMigrationsMap = elementTypeToMigrationsMap;
    }

    public final Map<String, List<MigrateElements>> getMigrations() {
        return elementTypeToMigrationsMap;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (MapUtils.isNotEmpty(elementTypeToMigrationsMap)) {
            //populateAndAddMappingsToOpChain(opChain, context);
            for (Operation op : opChain.getOperations()) {
                if (op instanceof OperationView) {
                    updateView((OperationView) op);
                    if (op instanceof GetAllElements) {
                        ((GetAllElements) op).setMigrations(elementTypeToMigrationsMap);
                        ((GetAllElements) op).setPostFilters(groupToElementFiltersMap);
                    }
                }
            }
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    private void updateView(final OperationView op) {
        final View currentView = op.getView();
        final View.Builder newViewBuilder = new View.Builder();
        for (String elementType : elementTypeToMigrationsMap.keySet()) {
            List<MigrateElements> elementSchemaMapping = elementTypeToMigrationsMap.get(elementType);
            Set<String> selections = new HashSet<>();
            Set<String> projections = new HashSet<>();
            ElementFilter originalGroupPreAggFilter = null;
            ElementFilter newGroupPreAggFilter = null;
            if (elementType.equals("edges")) {
                for (MigrateElements mapping : elementSchemaMapping) {
                    if (currentView.getElement(mapping.getOriginalGroup()) != null) {
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPreAggregationFilters()) {
                            originalGroupPreAggFilter = currentView.getElement(mapping.getOriginalGroup()).getPreAggregationFilter();
                        }
                        List<ElementFilter> postFilters = new ArrayList<>();
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPostAggregationFilters()) {
                            postFilters.add(currentView.getElement(mapping.getOriginalGroup()).getPostAggregationFilter());
                        }
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPostTransformFilters()) {
                            postFilters.add(currentView.getElement(mapping.getOriginalGroup()).getPostTransformFilter());
                        }
                        groupToElementFiltersMap.put(mapping.getOriginalGroup(), postFilters);
                        for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                            int x = 0;
                            for (String selectionString : function.getSelection()) {
                                if (currentView.getElement(mapping.getOriginalGroup()).hasProperty(selectionString)) {
                                    selections.add(selectionString);
                                    projections.add(function.getProjection()[x]);
                                    x += 1;
                                }
                            }
                        }
                    }
                    if (currentView.getElement(mapping.getNewGroup()) != null) {
                        if (currentView.getElement(mapping.getNewGroup()).hasPreAggregationFilters()) {
                            newGroupPreAggFilter = currentView.getElement(mapping.getNewGroup()).getPreAggregationFilter();
                        }
                        List<ElementFilter> postFilters = new ArrayList<>();
                        if (currentView.getElement(mapping.getNewGroup()).hasPostAggregationFilters()) {
                            postFilters.add(currentView.getElement(mapping.getNewGroup()).getPostAggregationFilter());
                        }
                        if (currentView.getElement(mapping.getNewGroup()).hasPostTransformFilters()) {
                            postFilters.add(currentView.getElement(mapping.getNewGroup()).getPostTransformFilter());
                        }
                        groupToElementFiltersMap.put(mapping.getNewGroup(), postFilters);
                        for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                            int x = 0;
                            for (String projectionString : function.getProjection()) {
                                if (currentView.getElement(mapping.getNewGroup()).hasProperty(projectionString)) {
                                    projections.add(projectionString);
                                    selections.add(function.getSelection()[x]);
                                    x += 1;
                                }
                            }
                        }
                    }
                    newViewBuilder.edge(mapping.getOriginalGroup(), new ViewElementDefinition.Builder().properties(selections).preAggregationFilter(originalGroupPreAggFilter).build());
                    newViewBuilder.edge(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(projections).preAggregationFilter(newGroupPreAggFilter).build());
                }
            } else if (elementType.equals("entities")) {
                for (MigrateElements mapping : elementSchemaMapping) {
                    if (currentView.getElement(mapping.getOriginalGroup()) != null) {
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPreAggregationFilters()) {
                            originalGroupPreAggFilter = currentView.getElement(mapping.getOriginalGroup()).getPreAggregationFilter();
                        }
                        List<ElementFilter> postFilters = new ArrayList<>();
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPostAggregationFilters()) {
                            postFilters.add(currentView.getElement(mapping.getOriginalGroup()).getPostAggregationFilter());
                        }
                        if (currentView.getElement(mapping.getOriginalGroup()).hasPostTransformFilters()) {
                            postFilters.add(currentView.getElement(mapping.getOriginalGroup()).getPostTransformFilter());
                        }
                        groupToElementFiltersMap.put(mapping.getOriginalGroup(), postFilters);
                        for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                            int x = 0;
                            for (String selectionString : function.getSelection()) {
                                if (currentView.getElement(mapping.getOriginalGroup()).hasProperty(selectionString)) {
                                    selections.add(selectionString);
                                    projections.add(function.getProjection()[x]);
                                    x += 1;
                                }
                            }
                        }
                    }
                    if (currentView.getElement(mapping.getNewGroup()) != null) {
                        if (currentView.getElement(mapping.getNewGroup()).hasPreAggregationFilters()) {
                            newGroupPreAggFilter = currentView.getElement(mapping.getNewGroup()).getPreAggregationFilter();
                        }
                        List<ElementFilter> postFilters = new ArrayList<>();
                        if (currentView.getElement(mapping.getNewGroup()).hasPostAggregationFilters()) {
                            postFilters.add(currentView.getElement(mapping.getNewGroup()).getPostAggregationFilter());
                        }
                        if (currentView.getElement(mapping.getNewGroup()).hasPostTransformFilters()) {
                            postFilters.add(currentView.getElement(mapping.getNewGroup()).getPostTransformFilter());
                        }
                        groupToElementFiltersMap.put(mapping.getNewGroup(), postFilters);
                        for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                            int x = 0;
                            for (String projectionString : function.getProjection()) {
                                if (currentView.getElement(mapping.getNewGroup()).hasProperty(projectionString)) {
                                    projections.add(projectionString);
                                    selections.add(function.getSelection()[x]);
                                    x += 1;
                                }
                            }
                        }
                    }
                    newViewBuilder.entity(mapping.getOriginalGroup(), new ViewElementDefinition.Builder().properties(selections).preAggregationFilter(originalGroupPreAggFilter).build());
                    newViewBuilder.entity(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(projections).preAggregationFilter(newGroupPreAggFilter).build());
                }
            } else {
                throw new IllegalArgumentException("ElementType not known");
            }
        }
        op.setView(newViewBuilder.build());
    }

    private void populateAndAddMappingsToOpChain(final OperationChain<?> opChain, final Context context) {
        final uk.gov.gchq.gaffer.operation.impl.Map postExecuteSchemaMappings = new uk.gov.gchq.gaffer.operation.impl.Map();

        List<MigrateElements> migrateElementsList = new ArrayList<>();
        for (List<MigrateElements> migrateElements : elementTypeToMigrationsMap.values()) {
            migrateElementsList.addAll(migrateElements);
        }
        postExecuteSchemaMappings.setFunctions(migrateElementsList);

        AddOperationsToChain addOperationsToChain = new AddOperationsToChain();
        Map<String, List<Operation>> additionalOpsMap = new HashMap<>();
        uk.gov.gchq.gaffer.operation.impl.migration.Migration migrationOp = new uk.gov.gchq.gaffer.operation.impl.migration.Migration.Builder()
                .mappings(postExecuteSchemaMappings)
                .build();

        additionalOpsMap.put(GetElements.class.getName(), Arrays.asList(migrationOp));
        additionalOpsMap.put(GetAllElements.class.getName(), Arrays.asList(migrationOp));
        additionalOpsMap.put(GetAdjacentIds.class.getName(), Arrays.asList(migrationOp));

        addOperationsToChain.setAfter(additionalOpsMap);

        addOperationsToChain.preExecute(opChain, context);
    }
}
