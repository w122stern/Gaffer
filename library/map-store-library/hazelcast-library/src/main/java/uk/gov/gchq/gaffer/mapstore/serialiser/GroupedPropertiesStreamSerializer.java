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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.serialiser.GroupedPropertiesSerialiser;
import java.io.IOException;

public class GroupedPropertiesStreamSerializer implements StreamSerializer<GroupedProperties> {
    private final GroupedPropertiesSerialiser serialiser;

    public GroupedPropertiesStreamSerializer(final Schema schema) {
        this.serialiser = new GroupedPropertiesSerialiser(schema);
    }

    @Override
    public int getTypeId() {
        return 3;
    }

    @Override
    public void write(final ObjectDataOutput out, final GroupedProperties properties) throws IOException {
        out.writeByteArray(serialiser.serialise(properties));
    }

    @Override
    public GroupedProperties read(final ObjectDataInput in) throws IOException {
        return serialiser.deserialise(in.readByteArray());
    }

    @Override
    public void destroy() {
    }

    public void updateSchema(final Schema schema) {
        serialiser.updateSchema(schema);
    }
}
