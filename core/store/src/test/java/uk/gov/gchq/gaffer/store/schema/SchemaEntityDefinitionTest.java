/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema;

import com.google.common.collect.Sets;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.ExampleAggregateFunction;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.IsA;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class SchemaEntityDefinitionTest {
    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoProperties() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex("id.integer")
                .property("property", "property.string")
                .build();
        final Schema schema = new Schema.Builder()
                .type("property.string", String.class)
                .type("id.integer", Integer.class)
                .entity("entity", elementDef)
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(2, validator.getFunctions().size());
        assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
        assertEquals(Collections.singletonList(IdentifierType.VERTEX.name()),
                validator.getFunctions().get(0).getSelection());
        assertEquals(Collections.singletonList("property"),
                validator.getFunctions().get(1).getSelection());
    }

    @Test
    public void shouldBuildEntityDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementFilter clonedValidator = mock(ElementFilter.class);
        given(validator.clone()).willReturn(clonedValidator);

        // When
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "property.string")
                .vertex("id.integer")
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(validator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_2));

        assertEquals(1, elementDef.getIdentifiers().size());
        assertEquals("id.integer", elementDef.getIdentifierTypeName(IdentifierType.VERTEX));
        assertSame(clonedValidator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final SchemaEntityDefinition elementDef = new SchemaEntityDefinition.Builder()
                .vertex("id.integer")
                .property("property", "property.string")
                .build();
        final Schema schema = new Schema.Builder()
                .type("property.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .build())
                .type("id.integer", Integer.class)
                .entity("entity", elementDef)
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        assertEquals(1, aggregator.getFunctions().size());
        assertTrue(aggregator.getFunctions().get(0).getFunction() instanceof ExampleAggregateFunction);
        assertEquals(Collections.singletonList("property"),
                aggregator.getFunctions().get(0).getSelection());
    }

    @Test
    public void shouldMergeDifferentSchemaElementDefinitions() {
        // Given
        // When
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("id.integer")
                .property(TestPropertyNames.PROP_1, "property.integer")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_2)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_2)
                .build();

        // When
        final SchemaEntityDefinition mergedDef = new SchemaEntityDefinition.Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertEquals("id.integer", mergedDef.getVertex());
        assertEquals(2, mergedDef.getProperties().size());
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_2));

        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_2)),
                mergedDef.getGroupBy());
    }

    @Test
    public void shouldOverrideVertexWhenMerging() {
        // Given
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.integer")
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
                .vertex("vertex.string")
                .build();

        // When
        final SchemaEntityDefinition mergedDef = new SchemaEntityDefinition.Builder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertEquals("vertex.string", mergedDef.getVertex());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingProperty() {
        // Given
        // When
        final SchemaEntityDefinition elementDef1 = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "int")
                .build();

        final SchemaEntityDefinition elementDef2 = new SchemaEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "string")
                .build();

        // When / Then
        try {
            new SchemaEntityDefinition.Builder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("property"));
        }
    }
}