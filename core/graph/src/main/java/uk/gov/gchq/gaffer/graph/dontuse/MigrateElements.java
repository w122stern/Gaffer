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
 *//*


package uk.gov.gchq.gaffer.graph.dontuse;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;

import java.util.ArrayList;
import java.util.List;

public class MigrateElements extends KorypheFunction<Iterable<Element>, Iterable<Element>> {
    private String originalGroup;
    private String newGroup;
    private ElementTransformer transformFunctions;

    public MigrateElements() {
    }

    public void setOriginalGroup(String originalGroup) {
        this.originalGroup = originalGroup;
    }

    public String getOriginalGroup() {
        return originalGroup;
    }

    public void setNewGroup(String newGroup) {
        this.newGroup = newGroup;
    }

    public String getNewGroup() {
        return newGroup;
    }

    public List<TupleAdaptedFunction<String, ?, ?>> getTransformFunctions() {
        return null != transformFunctions ? transformFunctions.getComponents() : null;
    }

    public void setTransformFunctions(final List<TupleAdaptedFunction<String, ?, ?>> transformFunctions) {
        this.transformFunctions = new ElementTransformer();
        if (null != transformFunctions) {
            this.transformFunctions.getComponents().addAll(transformFunctions);
        }
    }

    @Override
    public Iterable<Element> apply(final Iterable<Element> inputElements) {
        List<Element> outputElements = new ArrayList<>();

        if (null != inputElements) {
            inputElements.forEach(element -> outputElements.add(transformFunctions.apply(element)));
        }

        for (TupleAdaptedFunction function : getTransformFunctions()) {
            for (Element e : outputElements) {
                int x = 0;
                for (Object projectionString : function.getProjection()) {
                    if (e.getProperties().containsKey(projectionString)) {
                        Object propertyValue = e.getProperty(((String) projectionString));
                        e.getProperties().remove(projectionString);
                        e.getProperties().put((String) function.getSelection()[x], propertyValue);
                        x += 1;
                    }
                }
                e.setGroup(originalGroup);
            }
        }
        return outputElements;
    }
}
*/
