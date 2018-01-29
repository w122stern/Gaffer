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
import uk.gov.gchq.gaffer.graph.migration.SchemaMapping;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonPropertyOrder(alphabetic = true)
public class Migration implements GraphHook {
    private Map<String, List<SchemaMapping>> migrations = new HashMap<>();

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
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

    public void setMigrations(final Map<String, List<SchemaMapping>> migrations) {
        this.migrations = migrations;
    }

    public final Map<String, List<SchemaMapping>> getMigrations() {
        return migrations;
    }

    private final void updateView(final OperationView op) {
        if (null != migrations && !migrations.isEmpty()) {
            final View currentView = op.getView();
            final View.Builder newViewBuilder = new View.Builder();
            for (String elementType : migrations.keySet()) {
                List<SchemaMapping> elementSchemaMapping = migrations.get(elementType);
                if (elementType.equals("edges")) {
                    for (SchemaMapping mapping : elementSchemaMapping) {
                        if (currentView.getElement(mapping.getCurrentGroup()) != null) {
                            newViewBuilder.edge(mapping.getCurrentGroup(), new ViewElementDefinition.Builder().properties(mapping.getSelection()).build());
                            newViewBuilder.edge(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(mapping.getProjection()).build());
                        }
                    }
                } else if (elementType.equals("entities")) {
                    for (SchemaMapping mapping : elementSchemaMapping) {
                        if (currentView.getElement(mapping.getCurrentGroup()) != null) {
                            newViewBuilder.entity(mapping.getCurrentGroup(), new ViewElementDefinition.Builder().properties(mapping.getSelection()).build());
                            newViewBuilder.entity(mapping.getNewGroup(), new ViewElementDefinition.Builder().properties(mapping.getProjection()).build());
                        }
                    }
                } else
                    throw new IllegalArgumentException("ElementType not known");
            }
            op.setView(newViewBuilder.build());
        }
    }
}
