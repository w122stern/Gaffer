package uk.gov.gchq.gaffer.mapstore.serialiser;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import java.io.IOException;

public abstract class ElementStreamSerializer<E extends Element>
        implements StreamSerializer<E> {
    private final ToBytesSerialiser<E> serialiser;

    public ElementStreamSerializer(final ToBytesSerialiser<E> serialiser) {
        this.serialiser = serialiser;
    }

    @Override
    public void write(final ObjectDataOutput out, final E entity) throws IOException {
        out.writeByteArray(serialiser.serialise(entity));
    }

    @Override
    public E read(ObjectDataInput in)
            throws IOException {
        return serialiser.deserialise(in.readByteArray());
    }

    @Override
    public void destroy() {
    }
}