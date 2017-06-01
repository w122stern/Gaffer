package uk.gov.gchq.gaffer.mapstore.serialiser;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.serialiser.EntitySerialiser;

public class EntityStreamSerializer
        extends ElementStreamSerializer<Entity> {

    public EntityStreamSerializer(final Schema schema) {
        super(new EntitySerialiser(schema));
    }

    @Override
    public int getTypeId() {
        return 1;
    }
}