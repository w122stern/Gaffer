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

package uk.gov.gchq.gaffer.data.type;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.data.type.impl.FreqMapTypeConstructor;
import uk.gov.gchq.gaffer.types.FreqMap;

public class TypeConstructorTest {

    @Test
    public void testTypeConstructorRegistry(){

        TypeRegistryEntry entry = new TypeRegistryEntry();
        entry.setClassName(FreqMap.class.getCanonicalName());
        entry.setTypeConstructorClassName(FreqMapTypeConstructor.class.getCanonicalName());

        String json = null;
        try {
             json = new String(JSONSerialiser.serialise(entry));
        } catch (SerialisationException e) {
            e.printStackTrace();
        }


        TypeConstructorRegistry registry = new TypeConstructorRegistry();

        try {
            System.out.println(registry.getTypeConstructor(String.class.getCanonicalName()).getClass().getCanonicalName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

        registry.registerTypeConstructor(json);


        try {
            System.out.println(registry.getTypeConstructor(FreqMap.class.getCanonicalName()).getClass().getCanonicalName());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }

    }

}
