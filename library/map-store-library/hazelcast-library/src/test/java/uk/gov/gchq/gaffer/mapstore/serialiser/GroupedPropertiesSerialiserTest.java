/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.serialiser;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.RawIntegerSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.store.serialiser.GroupedPropertiesSerialiser;

import static org.junit.Assert.assertEquals;

public class GroupedPropertiesSerialiserTest {

    @Test
    public void shouldSerialiseAndDeserialiseProperties() throws SerialisationException {
        final Schema schema = new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .property(TestPropertyNames.PROP_1, "int")
                        .property(TestPropertyNames.PROP_2, "string")
                        .build())
                .type("int", new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .serialiser(new RawIntegerSerialiser())
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .build())
                .build();

        final GroupedProperties properties = new GroupedProperties(TestGroups.ENTITY);
        properties.put(TestPropertyNames.PROP_1, 1);
        properties.put(TestPropertyNames.PROP_2, "value");
        final GroupedPropertiesSerialiser serialiser = new GroupedPropertiesSerialiser(schema);

        // When
        final byte[] serialisedProperties = serialiser.serialise(properties);
        final GroupedProperties deserialisedProperties = serialiser.deserialise(serialisedProperties);

        // Then
        assertEquals(properties, deserialisedProperties);
    }
}
