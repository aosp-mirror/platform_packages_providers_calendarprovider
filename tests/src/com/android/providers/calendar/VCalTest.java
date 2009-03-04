/* //device/content/providers/pim/VCalTest.java
**
** Copyright 2006, The Android Open Source Project
**
** Licensed under the Apache License, Version 2.0 (the "License"); 
** you may not use this file except in compliance with the License. 
** You may obtain a copy of the License at 
**
**     http://www.apache.org/licenses/LICENSE-2.0 
**
** Unless required by applicable law or agreed to in writing, software 
** distributed under the License is distributed on an "AS IS" BASIS, 
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
** See the License for the specific language governing permissions and 
** limitations under the License.
*/

package com.android.providers.calendar;

import junit.framework.TestCase;
import android.test.suitebuilder.annotation.SmallTest;

public class VCalTest extends TestCase {

    @SmallTest
    public void testParse1() throws Exception {
        String str =
                "DTSTART;TZID=\"America/Los_Angeles\":20060908T170000\r\n"
                        + "DURATION;X-TEST=joe;X-Test=\"http://joe;\":PT3600S\r\n"
                        + "RRULE:FREQ=WEEKLY;BYDAY=FR;WKST=SU\r\n"
                        + "BEGIN:VTIMEZONE\r\n"
                        + "TZID:America/Los_Angeles\r\n"
                        + "X-LIC-LOCATION:America/Los_Angeles\r\n"
                        + "BEGIN:STANDARD\r\n"
                        + "TZOFFSETFROM:-0700\r\n"
                        + "TZOFFSETTO:-0800\r\n"
                        + "TZNAME:PST\r\n"
                        + "DTSTART:19701025T020000\r\n"
                        + "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU\r\n"
                        + "END:STANDARD\r\n"
                        + "BEGIN:DAYLIGHT\r\n"
                        + "TZOFFSETFROM:-0800\r\n"
                        + "TZOFFSETTO:-0700\r\n"
                        + "TZNAME:PDT\r\n"
                        + "DTSTART:19700405T020000\r\n"
                        + "RRULE:\r\n"
                        + " FREQ=YEARLY;BYMONTH=4;BYDAY=1SU\r\n"
                        + "END:DAYLIGHT\r\n"
                        + "END:VTIMEZONE\r\n";
        VCal.parse(str);
    }
}


