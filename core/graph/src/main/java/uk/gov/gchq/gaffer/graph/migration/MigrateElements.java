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

package uk.gov.gchq.gaffer.graph.migration;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.koryphe.function.KorypheFunction;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class MigrateElements extends KorypheFunction<Iterable<Element>, Iterable<Element>> {
    private String originalGroup;
    private String newGroup;
    private List<Transform> transformFunctions;

    public MigrateElements() {

    }

    public MigrateElements(final String originalGroup, final String newGroup, final List<Transform> transformFunctions) {
        this.originalGroup = originalGroup;
        this.newGroup = newGroup;
        this.transformFunctions = transformFunctions;
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

    public void setTransformFunctions(final List<Transform> transformFunctions) {
        this.transformFunctions = transformFunctions;
    }

    public List<Transform> getTransformFunctions() {
        return transformFunctions;
    }

    @Override
    public Iterable<Element> apply(final Iterable<Element> inputElements) {
        Iterable<Element> elements = inputElements;
        for (Transform transformFunction : transformFunctions) {
            Class clazz;
            Constructor con;
            KorypheFunction migrationFunction;
            try {
                clazz = Class.forName(transformFunction.getFunction().getClass().getName());
                con = clazz.getConstructor(String.class, String.class);
            } catch (final ClassNotFoundException | NoSuchMethodException e) {
                throw new IllegalArgumentException("Class " + transformFunction.getFunction().getClass().getName() + " failed" + e);
            }
            try {
                migrationFunction = (KorypheFunction) con.newInstance(transformFunction.getSelection(), transformFunction.getProjection());
            } catch (final InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException("Class " + transformFunction.getFunction().getClass().getName() + " failed");
            }
            if (null != migrationFunction) {
                elements = (Iterable<Element>) migrationFunction.apply(elements);
            }
        }
        for (Element e : elements) {
            e.setGroup(originalGroup);
            System.out.println("AFTER GROUP RESET BACK TO ORIGINAL FROM VIEW: " + e.toString());
        }
        return elements;
    }
}
