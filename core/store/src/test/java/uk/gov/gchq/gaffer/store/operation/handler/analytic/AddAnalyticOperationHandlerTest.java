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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.ParameterDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.analytic.AddAnalyticOperation;
import uk.gov.gchq.gaffer.operation.analytic.AnalyticOperation;
import uk.gov.gchq.gaffer.operation.analytic.AnalyticOperationDetail;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.analytic.cache.AnalyticOperationCache;
import uk.gov.gchq.gaffer.user.User;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class AddAnalyticOperationHandlerTest {
    private static final String EMPTY_ADMIN_AUTH = "";
    private final AnalyticOperationCache mockCache = mock(AnalyticOperationCache.class);
    private final AddAnalyticOperationHandler handler = new AddAnalyticOperationHandler(mockCache);

    private Context context = new Context(new User.Builder()
            .userId("test user")
            .build());
    private Store store = mock(Store.class);

    private AddAnalyticOperation addAnalyticOperation = new AddAnalyticOperation.Builder()
            .overwrite(false)
            .build();
    private static final String OPERATION_NAME = "test";
    private HashMap<String, AnalyticOperationDetail> storedOperations = new HashMap<>();

    @Before
    public void before() throws CacheOperationFailedException {
        storedOperations.clear();
        addAnalyticOperation.setOperationName(OPERATION_NAME);

        doAnswer(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            storedOperations.put(((AnalyticOperationDetail) args[0]).getOperationName(), (AnalyticOperationDetail) args[0]);
            return null;
        }).when(mockCache).addAnalyticOperation(any(AnalyticOperationDetail.class), anyBoolean(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        doAnswer(invocationOnMock ->
                new WrappedCloseableIterable<>(storedOperations.values()))
                .when(mockCache).getAllAnalyticOperations(any(User.class), eq(EMPTY_ADMIN_AUTH));

        doAnswer(invocationOnMock -> {
            String name = (String) invocationOnMock.getArguments()[0];
            AnalyticOperationDetail result = storedOperations.get(name);
            if (result == null) {
                throw new CacheOperationFailedException();
            }
            return result;
        }).when(mockCache).getAnalyticOperation(anyString(), any(User.class), eq(EMPTY_ADMIN_AUTH));

        given(store.getProperties()).willReturn(new StoreProperties());
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void after() throws CacheOperationFailedException {
        addAnalyticOperation.setOperationName(null);
        addAnalyticOperation.setOperation((String) null);
        addAnalyticOperation.setDescription(null);
        addAnalyticOperation.setOverwriteFlag(false);
        mockCache.clear();
    }


    @Test
    public void shouldNotAllowForNonRecursiveAnalyticOperationsToBeNested() throws OperationException {
        OperationChain child = new OperationChain.Builder().first(new AddElements()).build();
        addAnalyticOperation.setOperation(child);
        addAnalyticOperation.setOperationName("child");
        handler.doOperation(addAnalyticOperation, context, store);

        OperationChain parent = new OperationChain.Builder()
                .first(new AnalyticOperation.Builder().name("child").build())
                .then(new GetElements())
                .build();

        addAnalyticOperation.setOperation(parent);
        addAnalyticOperation.setOperationName("parent");

        exception.expect(OperationException.class);

        handler.doOperation(addAnalyticOperation, context, store);
    }

    @Test
    public void shouldAllowForOperationChainJSONWithParameter() {
        try {
            final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.Limit\", \"resultLimit\": \"${param1}\" } ] }";

            addAnalyticOperation.setOperation(opChainJSON);
            addAnalyticOperation.setOperationName("analyticop");
            ParameterDetail param = new ParameterDetail.Builder()
                    .defaultValue(1L)
                    .description("Limit param")
                    .valueClass(Long.class)
                    .build();
            Map<String, ParameterDetail> paramMap = Maps.newHashMap();
            paramMap.put("param1", param);
            addAnalyticOperation.setParameters(paramMap);
            handler.doOperation(addAnalyticOperation, context, store);
            assert cacheContains("analyticop");

        } catch (final Exception e) {
            fail("Expected test to pass without error. Exception " + e.getMessage());
        }

    }

    @Test
    public void shouldNotAllowForOperationChainWithParameterNotInOperationString() throws OperationException {
        final String opChainJSON = "{ \"operations\": [ { \"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\" }, { \"class\":\"uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet\", \"key\": \"${param1}\" } ] }";

        addAnalyticOperation.setOperation(opChainJSON);
        addAnalyticOperation.setOperationName("analyticop");

        // Note the param is String class to get past type checking which will also catch a param
        // with an unknown name if its not a string.
        ParameterDetail param = new ParameterDetail.Builder()
                .defaultValue("setKey")
                .description("key param")
                .valueClass(String.class)
                .build();
        Map<String, ParameterDetail> paramMap = Maps.newHashMap();
        paramMap.put("param2", param);
        addAnalyticOperation.setParameters(paramMap);

        exception.expect(OperationException.class);
        handler.doOperation(addAnalyticOperation, context, store);
    }

    @Test
    public void shouldNotAllowForOperationChainJSONWithInvalidParameter() throws UnsupportedEncodingException, SerialisationException {
        String opChainJSON = "{" +
                "  \"operations\": [" +
                "      {" +
                "          \"class\": \"uk.gov.gchq.gaffer.analytic.operation.AddAnalyticOperation\"," +
                "          \"operationName\": \"testInputParam\"," +
                "          \"overwriteFlag\": true," +
                "          \"operationChain\": {" +
                "              \"operations\": [" +
                "                  {" +
                "                      \"class\": \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"" +
                "                  }," +
                "                  {" +
                "                     \"class\": \"uk.gov.gchq.gaffer.operation.impl.Limit\"," +
                "                     \"resultLimit\": \"${param1}\"" +
                "                  }" +
                "              ]" +
                "           }," +
                "           \"parameters\": {" +
                "               \"param1\" : { \"description\" : \"Test Long parameter\"," +
                "                              \"defaultValue\" : [ \"bad arg type\" ]," +
                "                              \"requiredArg\" : false," +
                "                              \"valueClass\": \"java.lang.Long\"" +
                "                          }" +
                "           }" +
                "       }" +
                "   ]" +
                "}";

        exception.expect(SerialisationException.class);
        JSONSerialiser.deserialise(opChainJSON.getBytes("UTF-8"), OperationChain.class);
    }

    @Test
    public void shouldAddAnalyticOperationWithScoreCorrectly() throws OperationException, CacheOperationFailedException {
        OperationChain opChain = new OperationChain.Builder().first(new AddElements()).build();
        addAnalyticOperation.setOperation(opChain);
        addAnalyticOperation.setScore(2);
        addAnalyticOperation.setOperationName("testOp");

        handler.doOperation(addAnalyticOperation, context, store);

        final AnalyticOperationDetail result = mockCache.getAnalyticOperation("testOp", new User(), EMPTY_ADMIN_AUTH);

        assert cacheContains("testOp");
        assertEquals(addAnalyticOperation.getScore(), result.getScore());
    }

    private boolean cacheContains(final String opName) {
        Iterable<AnalyticOperationDetail> ops = mockCache.getAllAnalyticOperations(context.getUser(), EMPTY_ADMIN_AUTH);
        for (final AnalyticOperationDetail op : ops) {
            if (op.getOperationName().equals(opName)) {
                return true;
            }
        }
        return false;

    }
}
