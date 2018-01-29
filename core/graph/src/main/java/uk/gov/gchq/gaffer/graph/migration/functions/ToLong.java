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

package uk.gov.gchq.gaffer.graph.migration.functions;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.koryphe.function.KorypheFunction;

public class ToLong extends KorypheFunction<Iterable<Element>, Iterable<Element>> {
    private String selection;
    private String projection;


    public ToLong() {
    }

    public ToLong(final String selection, final String projection) {
        this.selection = selection;
        this.projection = projection;
    }

    @Override
    public Iterable<Element> apply(final Iterable<Element> elementList) {
        for (Element e : elementList) {
            String propertyValue = e.getProperty(selection).toString();
            e.getProperties().remove(selection);
            e.putProperty(projection, Long.parseLong(propertyValue));
            System.out.println("AFTER TO LONG: " + e.toString());
        }
        return elementList;
    }

    public String getSelection() {
        return selection;
    }

    public void setSelection(final String selection) {
        this.selection = selection;
    }

    public String getProjection() {
        return projection;
    }

    public void setProjection(final String projection) {
        this.projection = projection;
    }
}
