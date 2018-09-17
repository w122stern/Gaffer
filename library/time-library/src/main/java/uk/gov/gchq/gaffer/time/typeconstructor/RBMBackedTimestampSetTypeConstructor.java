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

package uk.gov.gchq.gaffer.time.typeconstructor;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.data.type.DefaultTypeConstructor;
import uk.gov.gchq.gaffer.data.type.TimeTypeConstructor;
import uk.gov.gchq.gaffer.store.csv.util.CsvUtil;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.time.Instant;

public class RBMBackedTimestampSetTypeConstructor extends TimeTypeConstructor {

    public RBMBackedTimestampSetTypeConstructor() {
        super(RBMBackedTimestampSet.class.getCanonicalName());
    }

    @Override
    public Object constructType(String input) {
        RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.valueOf(getTimeConfig().getTimestampGranularity()));
        if(input.equals(CsvUtil.NONE_INDICATOR)){
            return rbmBackedTimestampSet;
        }
        Long timestamp = Long.valueOf(input);
        Instant instant = Instant.ofEpochMilli(timestamp);
        rbmBackedTimestampSet.add(instant);
        return rbmBackedTimestampSet;
    }
}

