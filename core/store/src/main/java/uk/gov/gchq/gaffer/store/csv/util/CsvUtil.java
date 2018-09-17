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

package uk.gov.gchq.gaffer.store.csv.util;

import java.util.ArrayList;
import java.util.List;

public class CsvUtil {

    public static final String NONE_INDICATOR = "none";
    public static final String TYPE_CONSTRUCTOR_SUFFIX = "TypeConstructor";

    public static final String SCHEMA_CONF_KEY = "schema";
    public static final String FIELD_MAPPING_CONF_KEY = "fieldMapping";
    public static final String DELIMITER_CONF_KEY = "delimiter";
    public static final String IGNORE_QUOTES_CONF_KEY = "ignoreQuotes";
    public static final String QUOTES_STRING_CONF_KEY = "quotes";

    public static List<String> parseCSV(String s, int numFields, boolean includeQuotes){
        return parseCSV(s, numFields, ",", "\"", includeQuotes);
    }

    public static List<String> parseCSV(String s, int numFields, String delimiterString, String quoteString, boolean includeQuotes){
        /*
        Parses a delimited String but allows for the fields surrounded by a 'quote' character to contain the delimiter.
        There's also an option to retain the 'quote' characters in the resulting strings
         For example, with delimiter set to ',', quote set to '"' and includeQuotes=false, this string:
            "the, quick, \"brown, fox\", jumps, over, the, \"lazy, dog\""
         would return the array
         [
            the
            quick
            brown, fox
            jumps
            over
            the
            lazy, dog
         ]
         */

        if(delimiterString.length() > 1){
            throw new IllegalArgumentException("delimiter can only be a single character");
        }
        if(quoteString.length() > 1){
            throw new IllegalArgumentException("quoteString can only be a single character");
        }
        char delimiter = delimiterString.charAt(0);
        char quote = quoteString.charAt(0);

        List<String> results = new ArrayList<>(numFields);//a list to hold the results
        int i = 0;//the index of the character in the string
        boolean skipNextDelimiter = false;//are we inside 'quotes' and therefore skip the delimiter this time?
        int start = 0;//start index of the substring
        int end = 1;//end index of the substring
        int quoteOffset = 0;//move along an extra char if we don't want to include the quotes in the results
        String res = "";//the current result
        while(i < s.length()) {//step along the string char by char
            if(i == s.length() - 1) {//are we at the end?
                res = s.substring(end + 1 + quoteOffset, s.length() - quoteOffset);//if so, output the final substring
                results.add(res);
            }else
            if(s.charAt(i) == quote){//is this char a quote?
                skipNextDelimiter = !skipNextDelimiter;//if it is, toggle the value of skipdelimiter;
                // if it's our left enclosing quote, we should skip the next delimiter,
                // otherwise if it's a right-enclosing quote we should split on the next delimiter
                if(!includeQuotes){//are we including quotes?
                    quoteOffset = 1;//if not, we should move the start char on and move the end char back by 1
                }
            }else
            if(s.charAt(i) == delimiter){//is this a delimiter?
                if(!skipNextDelimiter){
                    end = i;//set the end of the substring here
                    res = s.substring(start + quoteOffset, end - quoteOffset);//chop out the substring we want
                    quoteOffset = 0;//if we've chopped off a string we should start again
                    results.add(res);//add the substring to the results
                    start = end + 1;//start our new substring from the next char
                }
            }
            i++;//move on to the next char in the string
        }
        return results;
    }

}
