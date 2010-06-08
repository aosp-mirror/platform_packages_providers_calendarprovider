/*
**
** Copyright 2010, The Android Open Source Project
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

import com.android.providers.calendar.CalendarAppWidgetService.CalendarAppWidgetModel;
import com.android.providers.calendar.CalendarAppWidgetService.MarkedEvents;

import android.database.MatrixCursor;
import android.test.AndroidTestCase;
import android.test.suitebuilder.annotation.SmallTest;
import android.text.format.DateUtils;
import android.view.View;

// adb shell am instrument -w -e class com.android.providers.calendar.CalendarAppWidgetServiceTest
//   com.android.providers.calendar.tests/android.test.InstrumentationTestRunner

public class CalendarAppWidgetServiceTest extends AndroidTestCase {
    private static final String TAG = "CalendarAppWidgetService";

    final long now = 1262340000000L; // Fri Jan 01 2010 02:00:00 GMT-0800 (PST)
    final long ONE_HOUR = 3600000;
    final long TWO_HOURS = 7200000;
    final String title = "Title";
    final String location = "Location";

//    Disabled test since this CalendarAppWidgetModel is not used for the no event case
//
//    @SmallTest
//    public void testGetAppWidgetModel_noEvents() throws Exception {
//        // Input
//        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
//
//        // Expected Output
//        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();
//        expected.visibNoEvents = View.VISIBLE;
//
//        // Test
//        long now = 1270000000000L;
//        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
//        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
//                getTestContext(), cursor, events, now);
//
//        assertEquals(expected, actual);
//    }

    @SmallTest
    public void testGetAppWidgetModel_1Event() throws Exception {
        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();

        // Input
        // allDay, begin, end, title, location, eventId
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title, location, 0));

        // Expected Output
        expected.dayOfMonth = "1";
        expected.dayOfWeek = "FRI";
        expected.visibNoEvents = View.GONE;
        expected.eventInfos[0].visibWhen = View.VISIBLE;
        expected.eventInfos[0].visibWhere = View.VISIBLE;
        expected.eventInfos[0].visibTitle = View.VISIBLE;
        expected.eventInfos[0].when = "3am";
        expected.eventInfos[0].where = location;
        expected.eventInfos[0].title = title;

        // Test
        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
                getTestContext(), cursor, events, now);

        assertEquals(expected, actual);
    }

    @SmallTest
    public void testGetAppWidgetModel_2StaggeredEvents() throws Exception {
        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();

        int i = 0;
        long tomorrow = now + DateUtils.DAY_IN_MILLIS;
        long sunday = tomorrow + DateUtils.DAY_IN_MILLIS;

        // Expected Output
        expected.dayOfMonth = "1";
        expected.dayOfWeek = "FRI";
        expected.visibNoEvents = View.GONE;
        expected.eventInfos[0].visibWhen = View.VISIBLE;
        expected.eventInfos[0].visibWhere = View.VISIBLE;
        expected.eventInfos[0].visibTitle = View.VISIBLE;
        expected.eventInfos[0].when = "2am, Tomorrow";
        expected.eventInfos[0].where = location + i;
        expected.eventInfos[0].title = title + i;
        ++i;
        expected.eventInfos[1].visibWhen = View.VISIBLE;
        expected.eventInfos[1].visibWhere = View.VISIBLE;
        expected.eventInfos[1].visibTitle = View.VISIBLE;
        expected.eventInfos[1].when = "2am, Sun";
        expected.eventInfos[1].where = location + i;
        expected.eventInfos[1].title = title + i;

        // Input
        // allDay, begin, end, title, location, eventId
        i = 0;
        cursor.addRow(getRow(0, tomorrow, tomorrow + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, sunday, sunday + TWO_HOURS, title + i, location + i, 0));
        ++i;

        // Test
        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
                getTestContext(), cursor, events, now);

        assertEquals(expected, actual);

        // Secondary test - Add two more afterwards
        cursor.addRow(getRow(0, sunday + ONE_HOUR, sunday + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, sunday + ONE_HOUR, sunday + TWO_HOURS, title + i, location + i, 0));

        // Test again
        events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        actual = CalendarAppWidgetService.getAppWidgetModel(getTestContext(), cursor, events, now);

        assertEquals(expected, actual);
    }

    @SmallTest
    public void testGetAppWidgetModel_2SameStartTimeEvents() throws Exception {
        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();

        int i = 0;
        // Expected Output
        expected.dayOfMonth = "1";
        expected.dayOfWeek = "FRI";
        expected.visibNoEvents = View.GONE;
        expected.eventInfos[0].visibWhen = View.VISIBLE;
        expected.eventInfos[0].visibWhere = View.VISIBLE;
        expected.eventInfos[0].visibTitle = View.VISIBLE;
        expected.eventInfos[0].when = "3am";
        expected.eventInfos[0].where = location + i;
        expected.eventInfos[0].title = title + i;
        ++i;
        expected.eventInfos[1].visibWhen = View.VISIBLE;
        expected.eventInfos[1].visibWhere = View.VISIBLE;
        expected.eventInfos[1].visibTitle = View.VISIBLE;
        expected.eventInfos[1].when = "3am";
        expected.eventInfos[1].where = location + i;
        expected.eventInfos[1].title = title + i;

        expected.conflictPortrait = null;
        expected.conflictLandscape = "1 more event";
        expected.visibConflictLandscape = View.VISIBLE;

        // Input
        // allDay, begin, end, title, location, eventId
        i = 0;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;

        // Test
        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
                getTestContext(), cursor, events, now);

        assertEquals(expected, actual);

        // Secondary test - Add two more afterwards
        cursor.addRow(getRow(0, now + TWO_HOURS, now + TWO_HOURS + 1, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + TWO_HOURS, now + TWO_HOURS + 1, title + i, location + i, 0));

        // Test again
        events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        actual = CalendarAppWidgetService.getAppWidgetModel(getTestContext(), cursor, events, now);

        assertEquals(expected, actual);
    }

    @SmallTest
    public void testGetAppWidgetModel_1EventThen2SameStartTimeEvents() throws Exception {
        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();

        // Input
        int i = 0;
        // allDay, begin, end, title, location, eventId
        cursor.addRow(getRow(0, now, now + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));

        // Expected Output
        expected.dayOfMonth = "1";
        expected.dayOfWeek = "FRI";
        i = 0;
        expected.visibNoEvents = View.GONE;
        expected.eventInfos[0].visibWhen = View.VISIBLE;
        expected.eventInfos[0].visibWhere = View.VISIBLE;
        expected.eventInfos[0].visibTitle = View.VISIBLE;
        expected.eventInfos[0].when = "2am";
        expected.eventInfos[0].where = location + i;
        expected.eventInfos[0].title = title + i;

        expected.eventInfos[1].visibWhen = View.VISIBLE;
        expected.eventInfos[1].when = "3am";

        expected.visibConflictPortrait = View.VISIBLE;
        expected.conflictPortrait = "2 more events";
        expected.conflictLandscape = null;

        // Test
        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
                getTestContext(), cursor, events, now);

        assertEquals(expected, actual);
    }

    @SmallTest
    public void testGetAppWidgetModel_3SameStartTimeEvents() throws Exception {
        final long now = 1262340000000L; // Fri Jan 01 2010 01:00:00 GMT-0700 (PDT)
        MatrixCursor cursor = new MatrixCursor(CalendarAppWidgetService.EVENT_PROJECTION, 0);
        CalendarAppWidgetModel expected = new CalendarAppWidgetModel();

        int i = 0;

        // Expected Output
        expected.dayOfMonth = "1";
        expected.dayOfWeek = "FRI";
        expected.visibNoEvents = View.GONE;
        expected.eventInfos[0].visibWhen = View.VISIBLE;
        expected.eventInfos[0].visibWhere = View.VISIBLE;
        expected.eventInfos[0].visibTitle = View.VISIBLE;
        expected.eventInfos[0].when = "3am";
        expected.eventInfos[0].where = location + i;
        expected.eventInfos[0].title = title + i;

        expected.visibConflictPortrait = View.VISIBLE;
        expected.visibConflictLandscape = View.VISIBLE;
        expected.conflictPortrait = "2 more events";
        expected.conflictLandscape = "2 more events";

        // Input
        // allDay, begin, end, title, location, eventId
        i = 0;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + ONE_HOUR, now + TWO_HOURS, title + i, location + i, 0));
        ++i;

        // Test
        MarkedEvents events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        CalendarAppWidgetModel actual = CalendarAppWidgetService.getAppWidgetModel(
                getTestContext(), cursor, events, now);

        assertEquals(expected, actual);

        // Secondary test - Add two more afterwards
        cursor.addRow(getRow(0, now + TWO_HOURS, now + TWO_HOURS + 1, title + i, location + i, 0));
        ++i;
        cursor.addRow(getRow(0, now + TWO_HOURS, now + TWO_HOURS + 1, title + i, location + i, 0));

        // Test again
        events = CalendarAppWidgetService.buildMarkedEvents(cursor, null, now);
        actual = CalendarAppWidgetService.getAppWidgetModel(getTestContext(), cursor, events, now);

        assertEquals(expected, actual);
    }

    private Object[] getRow(int allDay, long begin, long end, String title, String location,
            long eventId) {
        Object[] row = new Object[CalendarAppWidgetService.EVENT_PROJECTION.length];
        row[CalendarAppWidgetService.INDEX_ALL_DAY] = new Integer(allDay);
        row[CalendarAppWidgetService.INDEX_BEGIN] = new Long(begin);
        row[CalendarAppWidgetService.INDEX_END] = new Long(end);
        row[CalendarAppWidgetService.INDEX_TITLE] = new String(title);
        row[CalendarAppWidgetService.INDEX_EVENT_LOCATION] = new String(location);
        row[CalendarAppWidgetService.INDEX_EVENT_ID] = new Long(eventId);
        return row;
    }
}
