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

package uk.gov.gchq.gaffer.store.operation.handler.csv;

import org.apache.commons.io.FileUtils;
import uk.gov.gchq.gaffer.data.type.*;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromCsv;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.csv.fieldmapping.FieldMapping;
import uk.gov.gchq.gaffer.store.csv.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;

public class AddElementsFromCsvHandler implements OperationHandler<AddElementsFromCsv> {

    private String fileName;
    private String mappingsFile;

    @Override
    public Object doOperation(AddElementsFromCsv addElementsFromLocalCsv, Context context, Store store) throws OperationException {

        fileName = addElementsFromLocalCsv.getFilename();
        mappingsFile = addElementsFromLocalCsv.getMappingsFile();
        Schema schema = store.getSchema();
        FieldMapping fieldMapping;

        try {
            fieldMapping = new FieldMapping(FileUtils.readFileToString(new File(mappingsFile)));
        } catch (IOException e) {
            throw new OperationException("Cannot create fieldMappings from file " + mappingsFile + " " + e.getMessage());
        }

        CsvElementGenerator generator = new CsvElementGenerator(schema, fieldMapping);
        generator.setDelimiter(addElementsFromLocalCsv.getDelimiter());
        generator.setQuotesChar(addElementsFromLocalCsv.getQuoteChar());
        generator.setIncludeQuotes(addElementsFromLocalCsv.isIncludeQuotes());

        TypeConstructorRegistry typeConstructorRegistry = new TypeConstructorRegistry();

        String typeConstructorDeclarationPaths = null;

        try {
            typeConstructorDeclarationPaths = store.getProperties().getTypeConstructorDeclarationPaths();
        }catch(NullPointerException e){

        }

        if(typeConstructorDeclarationPaths != null){
            String[] paths = typeConstructorDeclarationPaths.split(",");
            for(String p : paths){
                try {
                    String json = FileUtils.readFileToString(new File(p));
                    TypeConstructorDeclarations declarations = JSONSerialiser.deserialise(json.getBytes(), TypeConstructorDeclarations.class);
                    for(TypeRegistryEntry entry : declarations.getEntries()){
                        TypeConstructor typeConstructor = (TypeConstructor) Class.forName(entry.getTypeConstructorClassName()).newInstance();
                        if(typeConstructor.getClass().isAssignableFrom(TimeTypeConstructor.class)){
                            ((TimeTypeConstructor) typeConstructor).setTimeConfig(fieldMapping.getTimeConfig());
                            typeConstructorRegistry.registerTypeConstructor(typeConstructor.constructs(), typeConstructor);
                        }else{
                            typeConstructorRegistry.registerTypeConstructor(entry);
                        }
                    }
                } catch (IOException e) {
                    throw new OperationException("can't load typeConstructors from " + p);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        generator.setTypeConstructorRegistry(typeConstructorRegistry);


        Iterable<String> data = () -> {
            try {
                return FileUtils.lineIterator(new File(fileName));
            } catch (IOException e) {
                throw new RuntimeException(e);            }
        };

        OperationChain addOpChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<String>()
                        .generator(generator)
                        .input(data)
                        .build())
                .then(new AddElements.Builder()
                        .build())
                .build();

        store.execute(addOpChain, context);

        return null;
    }

}
