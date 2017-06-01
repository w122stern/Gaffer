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
}