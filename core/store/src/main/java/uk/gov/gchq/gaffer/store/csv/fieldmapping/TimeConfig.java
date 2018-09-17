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

package uk.gov.gchq.gaffer.store.csv.fieldmapping;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TimeConfig {

    private String format;
    private Long timestampOffset = 0L;
    private Boolean millisCorrection = false;
    private String timeBucket = "SECOND";
    private Long millisMultiplier = 1L;
    private String timestampGranularity = "SECOND";

    public TimeConfig(){

    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Long getTimestampOffset() {
        return timestampOffset;
    }

    public void setTimestampOffset(Long timestampOffset) {
        this.timestampOffset = timestampOffset;
    }

    public Boolean getMillisCorrection() {
        return millisCorrection;
    }

    public void setMillisCorrection(Boolean millisCorrection) {
        this.millisCorrection = millisCorrection;
        if(millisCorrection){
            millisMultiplier = 1000L;
        }
    }

    public String getTimeBucket() {
        return timeBucket;
    }

    public void setTimeBucket(String timeBucket) {
        this.timeBucket = timeBucket;
    }

    public String getTimestampGranularity() {
        return timestampGranularity;
    }

    public void setTimestampGranularity(String timestampGranularity) {
        this.timestampGranularity = timestampGranularity;
    }

    @JsonIgnore
    public Long getMillisMultiplier() {
        return millisMultiplier;
    }

    @JsonIgnore
    public void setMillisMultiplier(Long millisMultiplier) {
        this.millisMultiplier = millisMultiplier;
    }

    @Override
    public String toString() {
        return "TimeConfig{" +
                "format='" + format + '\'' +
                ", timestampOffset=" + timestampOffset +
                ", millisCorrection=" + millisCorrection +
                ", timeBucket='" + timeBucket + '\'' +
                ", millisMultiplier=" + millisMultiplier +
                ", timestampGranularity='" + timestampGranularity + '\'' +
                '}';
    }
}
