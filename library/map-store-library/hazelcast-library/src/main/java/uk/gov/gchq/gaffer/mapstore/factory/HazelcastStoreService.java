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
package uk.gov.gchq.gaffer.mapstore.factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Run the main method to launch a hazelcast node with the provided schema and properties.
 * <p>
 * Usage: nohup java -cp &lt;path to jar&gt; uk.gov.gchq.gaffer.mapstore.factory.HazelcastStoreService &lt;schema path&gt; &lt;store properties path&gt; &gt;&gt; /dev/null"
 * </p>
 * <p>
 * When the java process is terminated it will call a shutdown hook to shutdown the hazelcast node.s
 * </p>
 */
public class HazelcastStoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastStoreService.class);

    public static void main(final String[] args) {
        new HazelcastStoreService().start(args);
    }

    protected void start(final String[] args) {
        validateArgs(args);
        start(args[0], args[1]);
    }

    protected void start(final String schemaArg, final String propertiesPath) {
        start(Schema.fromJson(getSchemaPaths(schemaArg)),
                StoreProperties.loadStoreProperties(propertiesPath));
    }

    protected void start(final Schema schema, final StoreProperties properties) {
        new Graph.Builder()
                .addSchema(schema)
                .storeProperties(properties)
                .build();
    }

    protected void validateArgs(final String[] args) {
        if (null == args || args.length < 2 || null == args[0] || null == args[1]) {
            final String errorMsg = "Usage: nohup java -cp <path to jar> " + getClass().getName() + " <schema path> <store properties path> >> /dev/null";
            LOGGER.error(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }
    }

    protected Path[] getSchemaPaths(final String schemaArg) {
        final String[] schemaPathsArray = schemaArg.split(",");
        final Path[] paths = new Path[schemaPathsArray.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = Paths.get(schemaPathsArray[i]);
        }

        return paths;
    }
}
