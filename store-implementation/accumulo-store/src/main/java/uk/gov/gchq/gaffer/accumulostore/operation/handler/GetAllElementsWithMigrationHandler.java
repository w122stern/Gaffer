/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.operation.handler;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.retriever.impl.AccumuloAllElementsRetriever;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class GetAllElementsWithMigrationHandler implements OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> {
    @Override
    public CloseableIterable<? extends Element> doOperation(final GetAllElements operation, final Context context, final Store store)
            throws OperationException {
        return doOperation(operation, context, (AccumuloStore) store);
    }

    public CloseableIterable<? extends Element> doOperation(final GetAllElements operation, final Context context, final AccumuloStore store) throws OperationException {
        final uk.gov.gchq.gaffer.operation.impl.Map postExecuteSchemaMappings = new uk.gov.gchq.gaffer.operation.impl.Map();
        List<Function> migrateElementsList = new ArrayList<>();
        operation.getMigrations().values().forEach(migrateElementsList::addAll);

        postExecuteSchemaMappings.setFunctions(migrateElementsList);
        postExecuteSchemaMappings.setInput(getElements(operation, context.getUser(), store));

        uk.gov.gchq.gaffer.operation.impl.migration.Migration migrationOp = new uk.gov.gchq.gaffer.operation.impl.migration.Migration.Builder()
                .mappings(postExecuteSchemaMappings)
                .view(operation.getView())
                .build();

        List<Element> elements = (List) store.execute(migrationOp.getMappings(), context);

        for(Element e : elements){
            if (operation.getView().getElement(e.getGroup()).hasPostAggregationFilters()) {
                operation.getView().getElement(e.getGroup()).getPostAggregationFilter().test(e);
            }
            if(operation.getView().getElement(e.getGroup()).hasPostTransformFilters()) {
                operation.getView().getElement(e.getGroup()).getPostTransformFilter().test(e);
            }
        }

        return new WrappedCloseableIterable(elements);
    }

    private CloseableIterable<? extends Element> getElements(final GetAllElements operation, final User user, final AccumuloStore store) throws OperationException {
        try {
            return new AccumuloAllElementsRetriever(store, operation, user);
        } catch (final IteratorSettingException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }
    }
}
