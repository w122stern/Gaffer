package uk.gov.gchq.gaffer.mapstore.serialiser;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.serialiser.EdgeSerialiser;

public class EdgeStreamSerializer
        extends ElementStreamSerializer<Edge> {

    public EdgeStreamSerializer(final Schema schema) {
        super(new EdgeSerialiser(schema));
    }

    @Override
    public int getTypeId() {
        return 2;
    }
}