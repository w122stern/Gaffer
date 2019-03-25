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

package uk.gov.gchq.gaffer.store.operation.handler.analytic;


import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.analytic.AnalyticOperationDetail;
import uk.gov.gchq.gaffer.operation.analytic.GetAllAnalyticOperation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.serialisation.util.JsonSerialisationUtil;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.analytic.cache.AnalyticOperationCache;
import uk.gov.gchq.koryphe.util.IterableUtil;
import java.util.function.Function;

/**
 * Operation Handler for GetAllAnalyticOperations
 */
public class GetAllAnalyticOperationHandler implements OutputOperationHandler<GetAllAnalyticOperation, CloseableIterable<AnalyticOperationDetail>> {
    private final AnalyticOperationCache cache;

    public GetAllAnalyticOperationHandler() {
        this(new AnalyticOperationCache());
    }

    public GetAllAnalyticOperationHandler(final AnalyticOperationCache cache) {
        this.cache = cache;
    }

    /**
     * Retrieves all the Analytic Operations that a user is allowed to see. As the expected behaviour is to bring back a
     * summary of each operation, the simple flag is set to true. This means all the details regarding access roles and
     * operation chain details are not included in the output.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return an iterable of AnalyticOperations
     * @throws OperationException thrown if the cache has not been initialized in the operation declarations file
     */
    @Override
    public CloseableIterable<AnalyticOperationDetail> doOperation(final GetAllAnalyticOperation operation, final Context context, final Store store) throws OperationException {
        final CloseableIterable<AnalyticOperationDetail> ops = cache.getAllAnalyticOperations(context.getUser(), store.getProperties().getAdminAuth());
        return new WrappedCloseableIterable<>(IterableUtil.map(ops, new AddInputType()));
    }

    private static class AddInputType implements Function<AnalyticOperationDetail, AnalyticOperationDetail> {
        @Override
        public AnalyticOperationDetail apply(final AnalyticOperationDetail analyticOp) {
            if (null != analyticOp && null == analyticOp.getInputType()) {
                try {
                    final Operation op = analyticOp.getOperationWithDefaultParams();
                    if (op instanceof Operations) {
                        final Operation firstOp = (Operation) ((Operations) op).getOperations().toArray()[0];
                        if (firstOp instanceof Input) {
                            analyticOp.setInputType(JsonSerialisationUtil.getSerialisedFieldClasses(firstOp.getClass().getName()).get("input"));
                        }
                    }
                } catch (final Exception e) {
                    // ignore - just don't add the input type
                }
            }
            return analyticOp;
        }
    }
}
