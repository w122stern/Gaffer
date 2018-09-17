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

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.data.type.impl.*;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.util.HashMap;

public class TypeConstructorRegistry {

    private HashMap<String, TypeConstructor> registry;

    public TypeConstructorRegistry(){
        registry = new HashMap<>();
        initialiseRegistry();

    }

    public TypeConstructor getTypeConstructor(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return registry.get(className);
    }

    private void initialiseRegistry(){
        registry.put(String.class.getCanonicalName(), new StringTypeConstructor());
        registry.put(Integer.class.getCanonicalName(), new IntegerTypeConstructor());
        registry.put(Long.class.getCanonicalName(), new LongTypeConstructor());
        registry.put(Float.class.getCanonicalName(), new FloatTypeConstructor());
        registry.put(Double.class.getCanonicalName(), new DoubleTypeConstructor());
        registry.put(FreqMap.class.getCanonicalName(), new FreqMapTypeConstructor());
    }

    public void registerTypeConstructor(String className, TypeConstructor typeConstructor){
        registry.put(className, typeConstructor);
    }

    public void registerTypeConstructor(TypeRegistryEntry registryEntry){
        try {
            registry.put(registryEntry.getClassName(), (TypeConstructor) Class.forName(registryEntry.getTypeConstructorClassName()).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void registerTypeConstructor(String json){
        try {
            TypeRegistryEntry registryEntry = JSONSerialiser.deserialise(json.getBytes(), TypeRegistryEntry.class);
            registerTypeConstructor(registryEntry);
        } catch (SerialisationException e) {
            e.printStackTrace();
        }
    }

}
