/* //device/apps/Calendar/MonthView.java
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

import android.pim.DateException;
import android.text.format.DateUtils;
import android.util.Log;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

public class VCal
{
    public static final Pattern LINE = Pattern.compile(
            "([^:;]+)([^:]*):(.*)");

    public ArrayList<Property> properties = new ArrayList<Property>();
    public String dtstart;
    public String tzid;
    public String duration;
    public String rrule;
    public boolean allDay;

    public void dump()
    {
        System.out.println("-----------------------");
        dump(properties, "");
        System.out.println("dtstart='" + this.dtstart + "'");
        System.out.println("tzid='" + this.tzid + "'");
        System.out.println("duration='" + this.duration + "'");
        System.out.println("rrule='" + this.rrule + "'");
        System.out.println("-----------------------");
    }

    public static void dump(ArrayList<Property> props, String prefix)
    {
        int count = props.size();
        for (int i=0; i<count; i++) {
            Property prop = props.get(i);
            System.out.println(prefix + prop.name);
            if (prop instanceof Begin) {
                Begin b = (Begin)prop;
                dump(b.properties, prefix + "  ");
            }
        }
   }

    public static class Parameter
    {
        public String name;
        public String value;
    }

    public static class Property
    {
        public String name;
        public Parameter[] parameters;
        public String value;
        public String[] values;
    }

    public static class Begin extends Property
    {
        public Begin parent;
        public ArrayList<Property> properties = new ArrayList<Property>();
    }


    public static Property make(String name)
    {
        Property p;
        if (name.equals("BEGIN")) {
            p = new Begin();
        }
        else {
            p = new Property();
        }
        p.name = name;
        return p;
    }

    public static VCal parse(String str)
    {
        VCal vc = new VCal();

        int i, j, start;
        int N, M;

        // first we deal with line folding, by replacing all "\r\n " strings
        // with nothing
        str = str.replaceAll("\r\n ", "");

        // it's supposed to be \r\n, but not everyone does that
        str = str.replaceAll("\r\n", "\n");
        str = str.replaceAll("\r", "\n");

        ArrayList<Parameter> params = new ArrayList<Parameter>();
        ArrayList<Property> props = vc.properties;

        // then we split into lines
        String[] lines = str.split("\n");

        Begin begin = null;
        //System.out.println("lines.length=" + lines);
        N = lines.length;
        for (j=0; j<N; j++) {
            //System.out.println("===[" + lines[j] + "]===");
            String line = lines[j];
            int len = line.length();
            if (len > 0) {
                i = 0;
                char c;
                do {
                    c = line.charAt(i);
                    i++;
                } while (c != ';' && c != ':');

                String n = line.substring(0, i-1);
                Property prop = make(n);
                props.add(prop);
                if (n.equals("BEGIN")) {
                    Begin b = (Begin)prop;
                    b.parent = begin;
                    begin = b;
                    props = begin.properties;
                }
                else if (n.equals("END")) {
                    begin = begin.parent;
                    if (begin != null) {
                        props = begin.properties;
                    } else {
                        props = vc.properties;
                    }
                }

                //System.out.println("name=[" + prop.name + "]");
                params.clear();
                while (c == ';') {
                    Parameter param = new Parameter();
                    start = i;
                    i++;
                    // param name
                    do {
                        c = line.charAt(i);
                        i++;
                    } while (c != '=');
                    param.name = line.substring(start, i-1);
                    //System.out.println(" param.name=[" + param.name + "]");
                    start = i;
                    if (line.charAt(start) == '"') {
                        i++;
                        start++;
                        do {
                            c = line.charAt(i);
                            i++;
                        } while (c != '"');
                        param.value = line.substring(start, i-1);
                        c = line.charAt(i);
                        i++;
                        //System.out.println(" param.valueA=[" + param.value
                        //                              + "]");
                    } else {
                        do {
                            c = line.charAt(i);
                            i++;
                        } while (c != ';' && c != ':');
                        param.value = line.substring(start, i-1);
                        //System.out.println(" param.valueB=["
                        //                              + param.value + "]");
                    }
                    params.add(param);
                }
                Object[] array = params.toArray();
                prop.parameters = new Parameter[array.length];
                System.arraycopy(array, 0, prop.parameters, 0, array.length);
                if (c != ':') {
                    throw new RuntimeException("error finding ':' c=" + c);
                }
                prop.value = line.substring(i);
                prop.values = line.split(",");
            }
        }

        N = vc.properties.size();
        Calendar calStart = null;
        for (i=0; i<N; i++) {
            Property prop = vc.properties.get(i);
            String n = prop.name;
            if (n.equals("DTSTART")) {
                try {
                    calStart = parseDateTime(prop, vc);
                    vc.dtstart = prop.value;
                } catch (DateException de) {
                    Log.w("CalendarProvider", "Unable to parse DTSTART=" + n, de);
                    return null;
                }
            } else if (n.equals("DTEND")) {
                // TODO: store the dtend, compute when expanding instances?
                // will we ever need to deal with seeing the DTEND before the
                // DTSTART?
                try {
                    if (calStart == null) {
                        vc.duration = "+P0S";
                    } else {
                        Calendar calEnd =
                                parseDateTime(prop, vc);
                        long durationMillis =
                                calEnd.getTimeInMillis() -
                                        calStart.getTimeInMillis();
                        long durationSeconds = (durationMillis / 1000);
                        vc.duration = "+P" + durationSeconds + "S";
                    }
                } catch (DateException de) {
                    Log.w("CalendarProvider", "Unable to parse DTEND=" + n, de);
                    return null;
                }
            } else if (n.equals("DURATION")) {
                vc.duration = prop.value;
            } else if (n.equals("RRULE")) {
                vc.rrule = prop.value;
            }
        }
        return vc;
    }

    private static Calendar parseDateTime(Property prop, VCal vc) throws DateException {
        int M;
        int j;
        String dt = prop.value;
        M = prop.parameters.length;
        for (j=0; j<M; j++) {
            Parameter param = prop.parameters[j];
            if (param.name.equals("TZID")) {
                vc.tzid = param.value;
            }
        }

        TimeZone tz = TimeZone.getTimeZone(vc.tzid);
        if (tz == null) {
            tz = TimeZone.getTimeZone("UTC");
        }
        GregorianCalendar somewhere = new GregorianCalendar(tz);
        DateUtils.parseDateTime(dt, somewhere);
        if (dt.length() == 8) {
            // this seems to work.
            vc.allDay = true;
        }
        return somewhere;
        /*GregorianCalendar zulu = new GregorianCalendar(
                                       TimeZone.getTimeZone("GMT"));
        zulu.setTimeInMillis(somewhere.getTimeInMillis());
        return zulu;*/
        // System.out.println("DTSTART=" + dtstart
        //         + " somewhere=" + somewhere
        //         + " vc.dtstart=" + vc.dtstart);
    }
}
