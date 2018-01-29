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

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.net.URISyntaxException;

import static org.mockito.Mockito.mock;

public class SchemaMigrationTest extends GraphHookTest<Migration> {

    private static final Context CONTEXT = new Context(mock(User.class));
    private static final String SCHEMA_MIGRATION_PATH = "/schema/SchemaMigration.json";
    private final Migration hook = fromJson(SCHEMA_MIGRATION_PATH);

    public SchemaMigrationTest() {
        super(Migration.class);
    }

    @Test
    public void test() throws URISyntaxException, OperationException {

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                        .properties("property1")
                                        .build())
                                .build())
                        .build())
                .build();

        // When
        System.out.println("VIEW BEFORE: " + ((OperationView) opChain.getOperations().get(0)).getView());
        hook.preExecute(opChain, CONTEXT);
        System.out.println("VIEW AFTER: " + ((OperationView) opChain.getOperations().get(0)).getView());
    }

    @Override
    protected Migration getTestObject() {
        return fromJson(SCHEMA_MIGRATION_PATH);
    }
}
