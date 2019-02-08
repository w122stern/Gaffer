/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.federatedstore.operation.AddGraphWithHooks;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.FederatedAddGraphHandlerParent;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.GraphOperationValidation;

/**
 * A handler for {@link AddGraphWithHooks} operation for the FederatedStore.
 *
 * @see FederatedAddGraphHandlerParent
 * @see GraphDelegate
 */
public class FederatedAddGraphWithHooksHandler extends FederatedAddGraphHandlerParent<AddGraphWithHooks> implements GraphOperationValidation<AddGraphWithHooks> {
    @Override
    protected GraphSerialisable _makeGraph(final AddGraphWithHooks operation, final Store store) {
        return new GraphDelegate.Builder()
                .store(store)
                .graphId(operation.getGraphId())
                .schema(operation.getSchema())
                .storeProperties(operation.getStoreProperties())
                .hooks(operation.getHooks())
                .parentSchemaIds(operation.getParentSchemaIds())
                .parentStorePropertiesId(operation.getParentPropertiesId())
                .buildGraphSerialisable();
    }
}
