/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.BooleanSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class EdgeSerialiser extends PropertiesSerialiser implements ToBytesSerialiser<Edge> {
    private final BooleanSerialiser booleanSerialiser = new BooleanSerialiser();
    protected ToBytesSerialiser<Object> vertexSerialiser;

    public EdgeSerialiser(final Schema schema) {
        super(schema);
    }

    @Override
    public void updateSchema(final Schema schema) {
        super.updateSchema(schema);
        if (null == schema.getVertexSerialiser()) {
            throw new IllegalArgumentException("Vertex serialiser must be defined in the schema");
        }
        if (!(schema.getVertexSerialiser() instanceof ToBytesSerialiser)) {
            throw new IllegalArgumentException("Vertex serialiser " + schema.getVertexSerialiser().getClass().getName() + " must be an instance of " + ToBytesSerialiser.class.getName());
        }
        vertexSerialiser = (ToBytesSerialiser) schema.getVertexSerialiser();
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return Edge.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final Edge edge) throws SerialisationException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(edge.getGroup());
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + edge.getGroup() + ", is this group in your schema or do your table iterators need updating?");
        }

        try {
            writeBytes(StringUtil.toBytes(edge.getGroup()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        try {
            writeBytes(vertexSerialiser.serialise(edge.getSource()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        try {
            writeBytes(vertexSerialiser.serialise(edge.getDestination()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        try {
            writeBytes(booleanSerialiser.serialise(edge.isDirected()), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise edge vertex to ByteArrayOutputStream", e);
        }

        serialiseProperties(edge.getProperties(), elementDefinition, out);
        return out.toByteArray();
    }

    @Override
    public Edge deserialise(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;

        final byte[] groupBytes = getFieldBytes(bytes, lastDelimiter);
        final String group = StringUtil.toString(groupBytes);
        lastDelimiter = getLastDelimiter(bytes, groupBytes, lastDelimiter);

        final byte[] sourceBytes = getFieldBytes(bytes, lastDelimiter);
        final Object source = ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(sourceBytes);
        lastDelimiter = getLastDelimiter(bytes, sourceBytes, lastDelimiter);

        final byte[] destBytes = getFieldBytes(bytes, lastDelimiter);
        final Object dest = ((ToBytesSerialiser) schema.getVertexSerialiser()).deserialise(destBytes);
        lastDelimiter = getLastDelimiter(bytes, destBytes, lastDelimiter);

        final byte[] directedBytes = getFieldBytes(bytes, lastDelimiter);
        final boolean directed = booleanSerialiser.deserialise(directedBytes);
        lastDelimiter = getLastDelimiter(bytes, directedBytes, lastDelimiter);

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Edge edge = new Edge(group, source, dest, directed);
        deserialiseProperties(bytes, edge.getProperties(), elementDefinition, lastDelimiter);
        return edge;
    }

    @Override
    public Edge deserialiseEmpty() throws SerialisationException {
        return null;
    }
}
