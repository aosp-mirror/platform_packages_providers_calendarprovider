/*
 * Copyright (C) 2009 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.providers.calendar;

import android.database.Cursor;
import android.net.Uri;
import android.text.format.Time;
import android.test.suitebuilder.annotation.LargeTest;

import android.util.Log;

/**
 * Calendar Sync instrumentation tests. Testing creation of new events, deleting events,
 * editing events.
 */
public class SyncCalendarTest extends CalendarSyncTestingBase {
    private EventInfo normalEvent =
            new EventInfo("normal0", "2009-05-29T08:00:00", "2009-05-29T08:30:00", false);
    private EventInfo dailyRecurringEvent = new EventInfo("dailyEvent",
            "daily from 5/1/2009 12am to 1am", "2009-10-01T00:00:00", "2009-10-01T01:00:00",
            "FREQ=DAILY;WKST=SU", false);

    private static final long ONE_HOUR_IN_MILLIS = 3600000;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @LargeTest
    public void testCreateNewEvent() throws Exception {
        int countBeforeNewEvent = getEventsCount();
        insertEvent(normalEvent);
        assertTrue("No new event was added. ", getEventsCount() > countBeforeNewEvent);
    }

    @LargeTest
    public void testCreateAndDeleteNewRecurringEvent() throws Exception {
        int countBeforeNewEvent = getEventsCount();
        Uri insertUri = insertEvent(dailyRecurringEvent);

        assertTrue("A daily recurring event should have been created.",
                   getEventsCount() > countBeforeNewEvent);
        deleteEvent(insertUri);
        assertEquals("Daily recurring event should have been deleted.",
                     countBeforeNewEvent, getEventsCount());
    }

    @LargeTest
    public void testCreateAllDayEvent() throws Exception {
        Time time = new Time();
        time.setToNow();
        long dtStart = time.toMillis(false);
        long dtEnd = time.toMillis(false) + ONE_HOUR_IN_MILLIS;
        EventInfo allDayEvent = new EventInfo("allday0", dtStart, dtEnd, true);

        Log.v("FOO", "alpha");
        int countBeforeNewEvent = getEventsCount();
        Log.v("FOO", "beta");
        insertEvent(allDayEvent);
        Log.v("FOO", "gamma");

        assertTrue("An all-day event should have been created.",
                   getEventsCount() > countBeforeNewEvent);
        Log.v("FOO", "delta");
    }

    @LargeTest
    public void testEditEventTitle() throws Exception {
        Cursor cursor;
        cursor = mResolver.query(mEventsUri, null, null, null, null);

        int countBeforeNewEvent = cursor.getCount();
        cursor.moveToNext();
        Time time = new Time();
        time.setToNow();
        String newTitle = cursor.getString(cursor.getColumnIndex("title")) + time.toString();
        long dtStart = cursor.getLong(cursor.getColumnIndex("dtstart"));
        long dtEnd = cursor.getLong(cursor.getColumnIndex("dtend"));

        EventInfo event = new EventInfo(newTitle, dtStart, dtEnd, false);
        long eventId = cursor.getLong(cursor.getColumnIndex("_id"));

        editEvent(eventId, event);
        cursor = mResolver.query(mEventsUri, null, null, null, null);        
        assertTrue("Events count should remain same.", getEventsCount() == countBeforeNewEvent);

        while (cursor.moveToNext()) {
            if (cursor.getLong(cursor.getColumnIndex("_id")) == eventId) {
                assertEquals(cursor.getString(cursor.getColumnIndex("title")), newTitle);
                break;
            }
        }
        cursor.close();
    }

    @LargeTest
    public void testEditEventDate() throws Exception {
        Cursor cursor;
        cursor = mResolver.query(mEventsUri, null, null, null, null);

        int countBeforeNewEvent = cursor.getCount();
        cursor.moveToNext();
        Time time = new Time();
        String title = cursor.getString(cursor.getColumnIndex("title"));
        long dtStart = cursor.getLong(cursor.getColumnIndex("dtstart"));
        time.set(dtStart + 2 * ONE_HOUR_IN_MILLIS);
        long newDtStart = time.toMillis(false);
        time.set(dtStart + 3 * ONE_HOUR_IN_MILLIS);
        long newDtEnd = time.toMillis(false);

        EventInfo event = new EventInfo(title, newDtStart, newDtEnd, false);
        long eventId = cursor.getLong(cursor.getColumnIndex("_id"));

        editEvent(eventId,  event);

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        int countAfterNewEvent = cursor.getCount();
        assertTrue("Events count should remain same.", countAfterNewEvent == countBeforeNewEvent);

        while (cursor.moveToNext()){
          if (cursor.getLong(cursor.getColumnIndex("_id")) == eventId) {
            System.err.println("startindex: " + cursor.getColumnIndex("dtstart"));
            System.err.println("  endindex: " + cursor.getColumnIndex("dtend"));
            assertEquals(cursor.getLong(cursor.getColumnIndex("dtstart")), newDtStart);
            assertEquals(cursor.getLong(cursor.getColumnIndex("dtend")), newDtEnd);
            break;
          }
        }
        cursor.close();
    }

    @LargeTest
    public void testEditEventDescription() throws Exception {
        Cursor cursor;
        cursor = mResolver.query(mEventsUri, null, null, null, null);
        int countBeforeNewEvent = cursor.getCount();
        cursor.moveToNext();

        String title = cursor.getString(cursor.getColumnIndex("title"));
        long dtStart = cursor.getLong(cursor.getColumnIndex("dtstart"));
        long dtEnd = cursor.getLong(cursor.getColumnIndex("dtend"));

        String newDescription = "NEW Descrption";
        EventInfo event = new EventInfo(title, dtStart, dtEnd, false, newDescription);

        long eventId = cursor.getLong(cursor.getColumnIndex("_id"));
        editEvent(eventId,  event);

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        int countAfterNewEvent = cursor.getCount();
        assertTrue("Events count should remain same.", countAfterNewEvent == countBeforeNewEvent);

        while (cursor.moveToNext()){
          if (cursor.getLong(cursor.getColumnIndex("_id")) == eventId) {
            assertEquals(cursor.getString(cursor.getColumnIndex("description")), newDescription);
            break;
          }
        }
        cursor.close();
    }
}
