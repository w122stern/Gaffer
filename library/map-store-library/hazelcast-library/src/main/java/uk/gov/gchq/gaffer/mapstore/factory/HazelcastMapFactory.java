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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.GafferToHazelcastMultiMap;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.mapstore.serialiser.EdgeStreamSerializer;
import uk.gov.gchq.gaffer.mapstore.serialiser.EntityStreamSerializer;
import uk.gov.gchq.gaffer.mapstore.serialiser.GroupedPropertiesStreamSerializer;
import uk.gov.gchq.gaffer.mapstore.util.GafferToHazelcastMap;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.io.File;
import java.io.InputStream;
import java.util.Map;

public class HazelcastMapFactory implements MapFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastMapFactory.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static HazelcastInstance hazelcast;
    private static Schema schema;

    @SuppressFBWarnings(value = {"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", "REC_CATCH_EXCEPTION"})
    @Override
    public void initialise(final Schema schema, final MapStoreProperties properties) {
        if (null == hazelcast) {
            HazelcastMapFactory.schema = schema;
            hazelcast = Hazelcast.newHazelcastInstance(loadConfig(schema, properties));
            LOGGER.info("Initialised hazelcast: {}", hazelcast.getCluster().getClusterState().name());
        } else {
            updateConfig(hazelcast.getConfig(), schema);
        }
    }

    @Override
    public <K, V> Map<K, V> getMap(final String mapName) {
        return new GafferToHazelcastMap<>(hazelcast.getMap(mapName));
    }

    @Override
    public <K, V> MultiMap<K, V> getMultiMap(final String mapName) {
        return new GafferToHazelcastMultiMap<>(hazelcast.getMultiMap(mapName));
    }

    @Override
    public <K, V> void updateValue(final Map<K, V> map, final K key, final V adaptedValue) {
        map.put(key, adaptedValue);
    }

    @Override
    public Element cloneElement(final Element element, final Schema schema) {
        // Element will already be cloned
        return element;
    }

    @Override
    public void clear() {
        if (null != hazelcast) {
            for (final DistributedObject map : hazelcast.getDistributedObjects()) {
                map.destroy();
            }
        }
    }

    private Config loadConfig(final Schema schema, final MapStoreProperties properties) {
        final String configFile = properties.getMapFactoryConfig();
        final Config config;
        if (configFile == null) {
            config = new XmlConfigBuilder().build();
        } else if (new File(configFile).exists()) {
            try {
                config = new FileSystemXmlConfig(configFile);
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not create hazelcast instance using config path: " + configFile, e);
            }
        } else {
            try (final InputStream configStream = StreamUtil.openStream(getClass(), configFile)) {
                config = new XmlConfigBuilder(configStream).build();
            } catch (final Exception e) {
                throw new IllegalArgumentException("Could not create hazelcast instance using config resource: " + configFile, e);
            }
        }

        updateConfig(config, schema);
        return config;
    }

    private void updateConfig(final Config config, final Schema schema) {
        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                        .setImplementation(new EntityStreamSerializer(schema))
                        .setTypeClass(Entity.class))
                .addSerializerConfig(new SerializerConfig()
                        .setImplementation(new EdgeStreamSerializer(schema))
                        .setTypeClass(Edge.class))
                .addSerializerConfig(new SerializerConfig()
                        .setImplementation(new GroupedPropertiesStreamSerializer(schema))
                        .setTypeClass(GroupedProperties.class));
    }
}
