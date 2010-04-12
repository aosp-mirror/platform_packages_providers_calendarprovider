/*
 * Copyright (C) 2010 The Android Open Source Project
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
 * limitations under the License
 */
package com.android.providers.calendar;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteDatabase.CursorFactory;
import android.provider.Calendar;
import android.test.suitebuilder.annotation.MediumTest;
import android.text.TextUtils;
import android.util.Log;

import junit.framework.TestCase;

public class CalendarDatabaseHelperTest extends TestCase {

    private SQLiteDatabase mBadDb;
    private SQLiteDatabase mGoodDb;
    private DatabaseUtils.InsertHelper mBadEventsInserter;
    private DatabaseUtils.InsertHelper mGoodEventsInserter;

    @Override
    public void setUp() {
        mBadDb = SQLiteDatabase.create(null);
        assertNotNull(mBadDb);
        mGoodDb = SQLiteDatabase.create(null);
        assertNotNull(mGoodDb);
    }

    private void createVersion67EventsTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE Events (" +
                "_id INTEGER PRIMARY KEY," +
                "_sync_account TEXT," +
                "_sync_account_type TEXT," +
                "_sync_id TEXT," +
                "_sync_version TEXT," +
                "_sync_time TEXT," +            // UTC
                "_sync_local_id INTEGER," +
                "_sync_dirty INTEGER," +
                "_sync_mark INTEGER," + // To filter out new rows
                "calendar_id INTEGER NOT NULL," +
                "htmlUri TEXT," +
                "title TEXT," +
                "eventLocation TEXT," +
                "description TEXT," +
                "eventStatus INTEGER," +
                "selfAttendeeStatus INTEGER NOT NULL DEFAULT 0," +
                "commentsUri TEXT," +
                "dtstart INTEGER," +               // millis since epoch
                "dtend INTEGER," +                 // millis since epoch
                "eventTimezone TEXT," +         // timezone for event
                "duration TEXT," +
                "allDay INTEGER NOT NULL DEFAULT 0," +
                "visibility INTEGER NOT NULL DEFAULT 0," +
                "transparency INTEGER NOT NULL DEFAULT 0," +
                "hasAlarm INTEGER NOT NULL DEFAULT 0," +
                "hasExtendedProperties INTEGER NOT NULL DEFAULT 0," +
                "rrule TEXT," +
                "rdate TEXT," +
                "exrule TEXT," +
                "exdate TEXT," +
                "originalEvent TEXT," +  // _sync_id of recurring event
                "originalInstanceTime INTEGER," +  // millis since epoch
                "originalAllDay INTEGER," +
                "lastDate INTEGER," +               // millis since epoch
                "hasAttendeeData INTEGER NOT NULL DEFAULT 0," +
                "guestsCanModify INTEGER NOT NULL DEFAULT 0," +
                "guestsCanInviteOthers INTEGER NOT NULL DEFAULT 1," +
                "guestsCanSeeGuests INTEGER NOT NULL DEFAULT 1," +
                "organizer STRING," +
                "deleted INTEGER NOT NULL DEFAULT 0," +
                "dtstart2 INTEGER," + //millis since epoch, allDay events in local timezone
                "dtend2 INTEGER," + //millis since epoch, allDay events in local timezone
                "eventTimezone2 TEXT," + //timezone for event with allDay events in local timezone
                "syncAdapterData TEXT" + //available for use by sync adapters
                ");");
    }

    private void addVersion67Events() {
        // April 5th 1:01:01 AM to April 6th 1:01:01
        mBadDb.execSQL("INSERT INTO Events (_id,dtstart,dtend,duration,dtstart2,dtend2," +
                "eventTimezone,eventTimezone2,allDay,calendar_id) " +
                "VALUES (1,1270454471000,1270540872000,'P10S'," +
                "1270454460000,1270540861000,'America/Los_Angeles','America/Los_Angeles',1,1);");

        // April 5th midnight to April 6th midnight, duration cleared
        mGoodDb.execSQL("INSERT INTO Events (_id,dtstart,dtend,duration,dtstart2,dtend2," +
                "eventTimezone,eventTimezone2,allDay,calendar_id) " +
                "VALUES (1,1270425600000,1270512000000,null," +
                "1270450800000,1270537200000,'UTC','America/Los_Angeles',1,1);");

        // April 5th 1:01:01 AM to April 6th 1:01:01, recurring weekly (We only check for the
        // existence of an rrule so it doesn't matter if the day is correct)
        mBadDb.execSQL("INSERT INTO Events (_id,dtstart,dtend,duration,dtstart2,dtend2," +
                "eventTimezone,eventTimezone2,allDay,rrule,calendar_id) " +
                "VALUES (2,1270454462000,1270540863000," +
                "'P10S',1270454461000,1270540861000,'America/Los_Angeles','America/Los_Angeles',1," +
                "'WEEKLY:MON',1);");

        // April 5th midnight with 1 day duration, if only dtend was wrong we wouldn't fix it, but
        // if anything else is wrong we clear dtend to be sure.
        mGoodDb.execSQL("INSERT INTO Events (" +
                "_id,dtstart,dtend,duration,dtstart2,dtend2," +
                "eventTimezone,eventTimezone2,allDay,rrule,calendar_id)" +
                "VALUES (2,1270425600000,null,'P1D',1270450800000,null," +
                "'UTC','America/Los_Angeles',1," +
                "'WEEKLY:MON',1);");

        assertEquals(mBadDb.rawQuery("SELECT _id FROM Events;", null).getCount(), 2);
        assertEquals(mGoodDb.rawQuery("SELECT _id FROM Events;", null).getCount(), 2);
    }

    @MediumTest
    public void testUpgradeToVersion69() {
        // Create event tables
        createVersion67EventsTable(mBadDb);
        createVersion67EventsTable(mGoodDb);
        // Fill in good and bad events
        addVersion67Events();
        // Run the upgrade on the bad events
        CalendarDatabaseHelper.upgradeToVersion69(mBadDb);
        Cursor badCursor = null;
        Cursor goodCursor = null;
        try {
            badCursor = mBadDb.rawQuery("SELECT _id,dtstart,dtend,duration,dtstart2,dtend2," +
                    "eventTimezone,eventTimezone2,rrule FROM Events WHERE allDay=?",
                    new String[] {"1"});
            goodCursor = mGoodDb.rawQuery("SELECT _id,dtstart,dtend,duration,dtstart2,dtend2," +
                    "eventTimezone,eventTimezone2,rrule FROM Events WHERE allDay=?",
                    new String[] {"1"});
            // Check that we get the correct results back
            assertTrue(compareCursors(badCursor, goodCursor));
        } finally {
            if (badCursor != null) {
                badCursor.close();
            }
            if (goodCursor != null) {
                goodCursor.close();
            }
        }
    }

    /**
     * Compares two cursors to see if they contain the same data.
     *
     * @return Returns true of the cursors contain the same data and are not null, false
     * otherwise
     */
    private static boolean compareCursors(Cursor c1, Cursor c2) {
        if(c1 == null || c2 == null) {
            Log.d("CDBT","c1 is " + c1 + " and c2 is " + c2);
            return false;
        }

        int numColumns = c1.getColumnCount();
        if (numColumns != c2.getColumnCount()) {
            Log.d("CDBT","c1 has " + numColumns + " columns and c2 has " + c2.getColumnCount());
            return false;
        }

        if (c1.getCount() != c2.getCount()) {
            Log.d("CDBT","c1 has " + c1.getCount() + " rows and c2 has " + c2.getCount());
            return false;
        }

        c1.moveToPosition(-1);
        c2.moveToPosition(-1);
        while(c1.moveToNext() && c2.moveToNext()) {
            for(int i = 0; i < numColumns; i++) {
                if(!TextUtils.equals(c1.getString(i),c2.getString(i))) {
                    Log.d("CDBT", c1.getString(i) + "\n" + c2.getString(i));
                    return false;
                }
            }
        }

        return true;
    }
}
