package uk.gov.gchq.gaffer.store.operation.handler.analytic;

import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.analytic.AddAnalyticOperation;
import uk.gov.gchq.gaffer.operation.analytic.AnalyticOperationDetail;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.analytic.cache.AnalyticOperationCache;
import java.util.Map;

public class AddAnalyticOperationHandler implements OperationHandler<AddAnalyticOperation> {
    private final AnalyticOperationCache cache;

    public AddAnalyticOperationHandler() {
        this(new AnalyticOperationCache());
    }

    public AddAnalyticOperationHandler(final AnalyticOperationCache cache) {
        this.cache = cache;
    }

    /**
     * Adds a AnalyticOperation to a cache which must be specified in the operation declarations file. An
     * AnalyticOperationDetail is built using the fields on the AddAnalyticOperation. The operation name and operation chain
     * fields must be set and cannot be left empty, or the build() method will fail and a runtime exception will be
     * thrown. The handler then adds/overwrites the AnalyticOperation according toa an overwrite flag.
     *
     * @param operation the {@link Operation} to be executed
     * @param context   the operation chain context, containing the user who executed the operation
     * @param store     the {@link Store} the operation should be run on
     * @return null (since the output is void)
     * @throws OperationException if the operation on the cache fails
     */
    @Override
    public Void doOperation(final AddAnalyticOperation operation, final Context context, final Store store) throws OperationException {
        try {
            final AnalyticOperationDetail analyticOperationDetail = new AnalyticOperationDetail.Builder()
                    .operation(operation.getOperationAsString())
                    .operationName(operation.getOperationName())
                    .creatorId(context.getUser().getUserId())
                    .readers(operation.getReadAccessRoles())
                    .writers(operation.getWriteAccessRoles())
                    .description(operation.getDescription())
                    .parameters(operation.getParameters())
                    .score(operation.getScore())
                    .build();

            validate(analyticOperationDetail.getOperationWithDefaultParams(), analyticOperationDetail);

            cache.addAnalyticOperation(analyticOperationDetail, operation.isOverwriteFlag(), context
                    .getUser(), store.getProperties().getAdminAuth());
        } catch (final CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }

    private void validate(final Operation operation, final AnalyticOperationDetail analyticOperationDetail) throws OperationException {

        if (null != analyticOperationDetail.getParameters()) {
            String operationString = analyticOperationDetail.getOperations();
            for (final Map.Entry<String, ParameterDetail> parameterDetail : analyticOperationDetail.getParameters().entrySet()) {
                String varName = "${" + parameterDetail.getKey() + "}";
                if (!operationString.contains(varName)) {
                    throw new OperationException("Parameter specified in AnalyticOperation doesn't occur in OperationChain string for " + varName);
                }
            }
        }
    }
}
