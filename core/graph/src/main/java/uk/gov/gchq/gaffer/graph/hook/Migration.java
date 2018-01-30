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

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.migration.MigrateElements;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder(alphabetic = true)
public class Migration implements GraphHook {
    private Map<String, List<MigrateElements>> migrations = new HashMap<>();
    private uk.gov.gchq.gaffer.operation.impl.Map map = new uk.gov.gchq.gaffer.operation.impl.Map();

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        updateMappings();
        addPostExecuteMappingsToOpChain(opChain, context);
        for (Operation op : opChain.getOperations()) {
            if (op instanceof OperationView) {
                updateView((OperationView) op);
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

    public void setMigrations(final Map<String, List<MigrateElements>> migrations) {
        this.migrations = migrations;
    }

    public final Map<String, List<MigrateElements>> getMigrations() {
        return migrations;
    }

    public void setMap(final uk.gov.gchq.gaffer.operation.impl.Map map) {
        this.map = map;
    }

    public uk.gov.gchq.gaffer.operation.impl.Map getMap() {
        return map;
    }

    private void updateMappings() {
        //IterableFunction iterableFunction = new IterableFunction();
        Collection<List<MigrateElements>> migrationsList = migrations.values();
        List<MigrateElements> migrateElementsList = new ArrayList<>();
        for(List<MigrateElements> migrateElements : migrationsList){
            migrateElementsList.addAll(migrateElements);
        }
        //iterableFunction.setFunctions(migrateElementsList);
         map.setFunctions(migrateElementsList);
    }

    private void addPostExecuteMappingsToOpChain(final OperationChain<?> opChain, final Context context) {
        AddOperationsToChain addPostExecutionMapToChain = new AddOperationsToChain();
        Map<String, List<Operation>> additionalOpsMap = new HashMap<>();

        List<Operation> additionalOps = new ArrayList<>();
        additionalOps.add(map);

        additionalOpsMap.put(GetElements.class.getName(), additionalOps);
        additionalOpsMap.put(GetAllElements.class.getName(), additionalOps);
        additionalOpsMap.put(GetAdjacentIds.class.getName(), additionalOps);

        addPostExecutionMapToChain.setAfter(additionalOpsMap);
        addPostExecutionMapToChain.preExecute(opChain, context);
    }

    private final void updateView(final OperationView op) {
        if (null != migrations && !migrations.isEmpty()) {
            final View currentView = op.getView();
            final View.Builder newViewBuilder = new View.Builder();
            for (String elementType : migrations.keySet()) {
                List<MigrateElements> elementSchemaMapping = migrations.get(elementType);
                if (elementType.equals("edges")) {
                    for (MigrateElements mapping : elementSchemaMapping) {
                        if (currentView.getElement(mapping.getOriginalGroup()) != null) {
                            for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                                for (String selection : function.getSelection()) {
                                    newViewBuilder.edge(mapping.getOriginalGroup(), new ViewElementDefinition.Builder().properties(selection).build());
                                }
                                for (String projection : function.getProjection()) {
                                    newViewBuilder.edge(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(projection).build());
                                }
                            }
                        }
                    }
                } else if (elementType.equals("entities")) {
                    for (MigrateElements mapping : elementSchemaMapping) {
                        if (currentView.getElement(mapping.getOriginalGroup()) != null) {
                            for (TupleAdaptedFunction<String, ?, ?> function : mapping.getTransformFunctions()) {
                                for (String selection : function.getSelection()) {
                                    newViewBuilder.entity(mapping.getOriginalGroup(), new ViewElementDefinition.Builder().properties(selection).build());
                                }
                                for (String projection : function.getProjection()) {
                                    newViewBuilder.entity(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(projection).build());
                                }
                            }
                        }
                    }
                } else
                    throw new IllegalArgumentException("ElementType not known");
            }
            op.setView(newViewBuilder.build());
        }
    }
}
