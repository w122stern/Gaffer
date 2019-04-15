/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2.example;

import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.analytic.AddAnalyticOperation;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;

/**
 * An {@code ExamplesFactory} creates example operations for use with Gaffer's
 * REST API.
 */
public interface ExamplesFactory {

    /**
     * Generates an example for the {@link AddAnalyticOperation} operation.
     *
     * @return the example class for AddAnalyticOperation
     */
    AddAnalyticOperation AddAnalyticOperation();

    /**
     * Generates an example for the {@link GetAdjacentIds} operation.
     *
     * @return the example class for GetAdjacentIds
     */
    GetAdjacentIds getAdjacentIds();

    /**
     * Generates an example for the {@link GetAllElements} operation.
     *
     * @return the example class for GetAllElements
     */
    GetAllElements getAllElements();

    /**
     * Generates an example for the {@link GetElements} operation.
     *
     * @return the example class for GetElements
     */
    GetElements getElements();

    /**
     * Generates an example for the {@link AddElements} operation.
     *
     * @return the example class for AddElements
     */
    AddElements addElements();

    /**
     * Generates an example for the {@link GenerateObjects} operation.
     *
     * @return the example class for GenerateObjects
     */
    GenerateObjects generateObjects();

    /**
     * Generates an example for the {@link GenerateElements} operation.
     *
     * @return the example class for GenerateElements
     */
    GenerateElements generateElements();

    /**
     * Generates an example for any {@link Operation} class.
     *
     * @param opClass the operation to create an example for
     * @return the example class
     * @throws IllegalAccessException if the operation could not be created
     * @throws InstantiationException if the operation could not be created
     */
    Operation generateExample(final Class<? extends Operation> opClass) throws IllegalAccessException, InstantiationException;

    /**
     * Generates an example for the {@link Sort} operation.
     *
     * @return the example class for Sorts
     */
    Sort sort();

    /**
     * Generates an example for the {@link Max} operation.
     *
     * @return the example class for Max
     */
    Max max();

    /**
     * Generates an example for the {@link Min} operation.
     *
     * @return the example class for Min
     */
    Min min();

    /**
     * Generates an example for the {@link ToMap} operation.
     *
     * @return the example class for ToMap
     */
    ToMap toMap();

    /**
     * Generates an example for the {@link GetWalks} operation.
     *
     * @return the example class for GetWalks
     */
    GetWalks getWalks();

    /**
     * Generates an example for the {@link AddNamedView} operation.
     *
     * @return the example class
     */
    AddNamedView addNamedView();

    /**
     * Generates an example for the {@link If} operation.
     *
     * @return the example class
     */
    If ifOperation();

    /**
     * Generates an example for the {@link While} operation.
     *
     * @return the example class
     */
    While whileOperation();
}
