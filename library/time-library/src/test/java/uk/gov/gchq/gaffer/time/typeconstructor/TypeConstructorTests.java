/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.time.typeconstructor;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromCsv;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.csv.buckets.impl.LongTimeBucket;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.user.User;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TypeConstructorTests {

    @Test
    public void typeConstructorTest(){

        String schemaPath = "src/test/resources/typeConstructorTests/schema.json";
        String graphConfigPath = "src/test/resources/typeConstructorTests/graphConfig.json";
        String storePropertiesPath = "src/test/resources/typeConstructorTests/store.properties";

        String dataPath = "src/test/resources/typeConstructorTests/testData.csv";
        String fieldMappingsPath = "src/test/resources/typeConstructorTests/field-mappings.json";

        Graph graph = null;

        try {
            graph = new Graph.Builder()
                    .addSchema(new FileInputStream(new File(schemaPath)))
                    .storeProperties(new FileInputStream(new File(storePropertiesPath)))
                    .config(new FileInputStream(new File(graphConfigPath)))
                    .build();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        User user = new User.Builder()
                .userId("test-user")
                .build();

        AddElementsFromCsv addElementsFromCsv = new AddElementsFromCsv.Builder()
                .filename(dataPath)
                .mappingsFile(fieldMappingsPath)
                .build();

        try {
            graph.execute(addElementsFromCsv, user);
        } catch (OperationException e) {
            e.printStackTrace();
        }

        GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("1"))
                .view(new View.Builder()
                        .entity("Entity", new ViewElementDefinition.Builder()
                                .groupBy()
                        .build())
                .build())
                .build();

        List<Element> result = new ArrayList<>();
        try {
            for(Element e : graph.execute(getElements, user)){
                result.add(e);
            }
        } catch (OperationException e) {
            e.printStackTrace();
        }

        Entity entity = new Entity("Entity");
        entity.setVertex("1");
        entity.putProperty("count", 3L);
        RBMBackedTimestampSet timestamps = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.SECOND);
        timestamps.add(Instant.ofEpochMilli(1254192988000L));
        timestamps.add(Instant.ofEpochMilli(1254192989000L));
        timestamps.add(Instant.ofEpochMilli(1254192990000L));
        entity.putProperty("timestamps", timestamps);
        LongTimeBucket timeBucket = new LongTimeBucket();
        timeBucket.setBucketSize("SECOND");
        entity.putProperty("timebucket", timeBucket.getBucket(1254192988000L));

        assertEquals(result.get(0), entity);

    }

}
