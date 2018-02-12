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

package uk.gov.gchq.gaffer.operation.impl.migration;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;

import java.io.IOException;
import java.util.Map;

public class Migration implements InputOutput<CloseableIterable<? extends Element>, CloseableIterable<? extends Element>> {
    private CloseableIterable<? extends Element> input;
    private uk.gov.gchq.gaffer.operation.impl.Map mappings = new uk.gov.gchq.gaffer.operation.impl.Map();
    private View view = new View();

    @Override
    public CloseableIterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final CloseableIterable<? extends Element> input) {
        this.input = input;
    }

    public uk.gov.gchq.gaffer.operation.impl.Map getMappings() {
        return mappings;
    }

    public void setMappings(final uk.gov.gchq.gaffer.operation.impl.Map mappings) {
        this.mappings = mappings;
    }

    public View getView() {
        return view;
    }

    public void setView(final View view) {
        this.view = view;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return null;
    }

    @Override
    public Map<String, String> getOptions() {
        return null;
    }

    @Override
    public void setOptions(final Map<String, String> options) {

    }

    @Override
    public TypeReference<CloseableIterable<? extends Element>> getOutputTypeReference() {
        return new TypeReferenceImpl.CloseableIterableElement();
    }

    public static class Builder extends Operation.BaseBuilder<Migration, Builder> {
        public Builder() {
            super(new Migration());
        }

        public Builder mappings(final uk.gov.gchq.gaffer.operation.impl.Map mappings) {
            _getOp().setMappings(mappings);
            return this;
        }

        public Builder view(final View view) {
            _getOp().setView(view);
            return this;
        }
    }
}
