/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.multimap;

import java.util.Collection;
import java.util.Set;

public class GafferToHazelcastMultiMap<K, V> implements MultiMap<K, V> {
    private final com.hazelcast.core.MultiMap<K, V> multiMap;

    public GafferToHazelcastMultiMap(final com.hazelcast.core.MultiMap<K, V> multiMap) {
        this.multiMap = multiMap;
    }

    @Override
    public boolean put(final K key, final V value) {
        return multiMap.put(key, value);
    }

    @Override
    public void put(final K key, final Collection<V> values) {
        for (final V value : values) {
            multiMap.put(key, value);
        }
    }

    @Override
    public Collection<V> get(final K key) {
        return multiMap.get(key);
    }

    @Override
    public Set<K> keySet() {
        return multiMap.keySet();
    }

    @Override
    public void clear() {
        multiMap.clear();
    }
}
