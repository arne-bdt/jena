/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.ext.xerces.impl.dv.xs;

import org.apache.jena.ext.xerces.impl.dv.InvalidDatatypeValueException;

/**
 * Validator for &lt;gDay&gt; datatype (W3C Schema datatypes)
 *
 * {@literal @xerces.internal} 
 *
 * @author Elena Litani
 * @author Gopal Sharma, SUN Microsystem Inc.
 * @version $Id: DayDV.java 937741 2010-04-25 04:25:46Z mrglavas $
 */
public class DayDV extends AbstractDateTimeDV {

    //size without time zone: ---09
    private final static int DAY_SIZE=5;

    @Override
    public Object getActualValue(String content) throws InvalidDatatypeValueException {
        try{
            return parse(content);
        } catch(Exception ex){
            throw new InvalidDatatypeValueException("cvc-datatype-valid.1.2.1", new Object[]{content, "gDay"});
        }
    }

    /**
     * Parses, validates and computes normalized version of gDay object
     *
     * @param str    The lexical representation of gDay object ---DD
     *               with possible time zone Z or (-),(+)hh:mm
     *               Pattern: ---(\\d\\d)(Z|(([-+])(\\d\\d)(:(\\d\\d))?
     * @return normalized date representation
     * @exception SchemaDateTimeException Invalid lexical representation
     */
    protected DateTimeData parse(String str) throws SchemaDateTimeException {
        DateTimeData date = new DateTimeData(str, this);
        int len = str.length();

        if (str.charAt(0)!='-' || str.charAt(1)!='-' || str.charAt(2)!='-') {
            throw new SchemaDateTimeException ("Error in day parsing");
        }

        //initialize values
        date.year=YEAR;
        date.month=MONTH;

        date.day=parseInt(str, 3,5);

        if ( DAY_SIZE<len ) {
            if (!isNextCharUTCSign(str, DAY_SIZE, len)) {
                throw new SchemaDateTimeException ("Error in day parsing");
            }
            else {
                getTimeZone(str, date, DAY_SIZE, len);
            }
        }

       //validate and normalize
        validateDateTime(date);

        //save unnormalized values
        saveUnnormalized(date);
        
        if ( date.utc!=0 && date.utc!='Z' ) {
            normalize(date);
        }
        date.position = 2;
        return date;
    }

    /**
     * Converts gDay object representation to String
     *
     * @param date   gDay object
     * @return lexical representation of gDay: ---DD with an optional time zone sign
     */
    @Override
    protected String dateToString(DateTimeData date) {
        StringBuilder message = new StringBuilder(6);
        message.append('-');
        message.append('-');
        message.append('-');
        append(message, date.day, 2);
        append(message, (char)date.utc, 0);
        return message.toString();
    }
}

