/*
 * Copyright (C) 2011 The Android Open Source Project
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

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.IContentProvider;
import android.database.Cursor;
import android.net.Uri;
import android.provider.Calendar;
import android.provider.Calendar.Calendars;
import android.test.InstrumentationTestCase;
import android.test.suitebuilder.annotation.*;
import android.util.Log;

public class CalendarCts extends InstrumentationTestCase {


    /**
     *
     */
    private static final String TAG = "CalCTS";
    private ContentResolver mContentResolver;

    private static class CalendarHelper {
        private static final String CTS_TEST_TYPE = "LOCAL";
        private static final String[] TIME_ZONES = new String[] {
                "UTC", "America/Los_Angeles", "Asia/Beirut", "Pacific/Auckland", };
        public static final String[] CALENDARS_PROJECTION = new String[] {
                Calendars._ID,
                Calendars._SYNC_ACCOUNT,
                Calendars._SYNC_ACCOUNT_TYPE,
                Calendars._SYNC_ID,
                Calendars._SYNC_VERSION,
                Calendars._SYNC_TIME,
                Calendars._SYNC_DIRTY,
                Calendars._SYNC_MARK,
                Calendars.NAME,
                Calendars.DISPLAY_NAME,
                Calendars.COLOR,
                Calendars.ACCESS_LEVEL,
                Calendars.SELECTED,
                Calendars.SYNC_EVENTS,
                Calendars.LOCATION,
                Calendars.TIMEZONE,
                Calendars.OWNER_ACCOUNT,
                Calendars.ORGANIZER_CAN_RESPOND,
                Calendars.DELETED,
                Calendars.SYNC1,
                Calendars.SYNC2,
                Calendars.SYNC3,
                Calendars.SYNC4,
                Calendars.SYNC5, };

        /**
         * Creates a new set of values for creating a single calendar with every
         * field.
         *
         * @param account The account name to create this calendar with
         * @param seed A number used to generate the values
         * @return A complete set of values for the calendar
         */
        public static ContentValues getNewCalendarValues(
                String account, int seed) {
            String seedString = Long.toString(seed);
            ContentValues values = new ContentValues();
            values.put(Calendars._SYNC_ACCOUNT_TYPE, CTS_TEST_TYPE);

            values.put(Calendars._SYNC_ACCOUNT, account);
            values.put(Calendars._SYNC_ID, "SYNC_ID:" + seedString);
            values.put(Calendars._SYNC_VERSION, "SYNC_V:" + seedString);
            values.put(Calendars._SYNC_TIME, "SYNC_TIME:" + seedString);
            values.put(Calendars._SYNC_DIRTY, 0);
            values.put(Calendars._SYNC_MARK, 0);
            values.put(Calendars.OWNER_ACCOUNT, "OWNER_" + account);

            values.put(Calendars.NAME, seedString);
            values.put(Calendars.DISPLAY_NAME, "DISPLAY_" + seedString);

            values.put(Calendars.ACCESS_LEVEL, (seed % 8) * 100);

            values.put(Calendars.COLOR, 0xff000000 + seed);
            values.put(Calendars.SELECTED, seed % 2);
            values.put(Calendars.SYNC_EVENTS, seed % 2);
            values.put(Calendars.LOCATION, "LOCATION:" + seedString);
            values.put(Calendars.TIMEZONE, TIME_ZONES[seed % TIME_ZONES.length]);
            values.put(Calendars.ORGANIZER_CAN_RESPOND, seed % 2);
            values.put(Calendars.SYNC1, "SYNC1:" + seedString);
            values.put(Calendars.SYNC2, "SYNC2:" + seedString);
            values.put(Calendars.SYNC3, "SYNC3:" + seedString);
            values.put(Calendars.SYNC4, "SYNC4:" + seedString);
            values.put(Calendars.SYNC5, "SYNC5:" + seedString);

            return values;
        }

        /**
         * Creates a set of values with just the updates and modifies the
         * original values to the expected values
         */
        public static ContentValues getUpdateCalendarValuesWithOriginal(
                ContentValues original, int seed) {
            ContentValues values = new ContentValues();
            String seedString = Long.toString(seed);

            values.put(Calendars.DISPLAY_NAME, "DISPLAY_" + seedString);
            values.put(Calendars.COLOR, 0xff000000 + seed);
            values.put(Calendars.SELECTED, seed % 2);
            values.put(Calendars.SYNC_EVENTS, seed % 2);

            original.putAll(values);
            original.put(Calendars._SYNC_DIRTY, 1);

            return values;
        }

        public static int deleteCalendarById(ContentResolver resolver, long id) {
            return resolver.delete(Calendars.CONTENT_URI, Calendars._ID + "=?",
                    new String[] { Long.toString(id) });
        }

        public static int deleteCalendarByAccount(ContentResolver resolver, String account) {
            return resolver.delete(Calendars.CONTENT_URI, Calendars._SYNC_ACCOUNT + "=?",
                    new String[] { account });
        }

        public static Cursor getCalendarsByAccount(ContentResolver resolver, String account) {
            String selection = Calendars._SYNC_ACCOUNT_TYPE + "=?";
            String[] selectionArgs;
            if (account != null) {
                selection += " AND " + Calendars._SYNC_ACCOUNT + "=?";
                selectionArgs = new String[2];
                selectionArgs[1] = account;
            } else {
                selectionArgs = new String[1];
            }
            selectionArgs[0] = CTS_TEST_TYPE;

            return resolver.query(
                    Calendars.CONTENT_URI, CALENDARS_PROJECTION, selection, selectionArgs, null);
        }
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mContentResolver = getInstrumentation().getTargetContext().getContentResolver();
        IContentProvider provider = mContentResolver.acquireProvider(Calendar.AUTHORITY);
        // mBuilder = new ContactsContract_TestDataBuilder(provider);
    }

    @MediumTest
    public void testCalendarCreationAndDeletion() {
        String account = "cc1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create a calendar
        ContentValues values = CalendarHelper.getNewCalendarValues(account, seed++);
        Uri uri = mContentResolver.insert(Calendars.CONTENT_URI, values);
        Log.d(TAG, "uri:" + uri.toString());
        long id = ContentUris.parseId(uri);
        assertTrue(id >= 0);

        verifyCalendar(account, values, id);

        // Delete
        assertEquals(1, CalendarHelper.deleteCalendarById(mContentResolver, id));

        // Verify
        Cursor c = CalendarHelper.getCalendarsByAccount(mContentResolver, account);
        assertEquals(0, c.getCount());
        c.close();
    }

    @MediumTest
    public void testCalendarUpdate() {
        String account = "cu1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create a calendar
        ContentValues values = CalendarHelper.getNewCalendarValues(account, seed++);
        Uri uri = mContentResolver.insert(Calendars.CONTENT_URI, values);
        Log.d(TAG, "uri:" + uri.toString());
        long id = ContentUris.parseId(uri);
        assertTrue(id >= 0);

        // Update the calendar using the direct Uri
        ContentValues updateValues = CalendarHelper.getUpdateCalendarValuesWithOriginal(
                values, seed++);
        assertEquals(1, mContentResolver.update(uri, updateValues, null, null));

        verifyCalendar(account, values, id);

        // Update the calendar using selection + args
        String selection = Calendars._ID + "=?";
        String[] selectionArgs = new String[] { Long.toString(id) };
        Log.d(TAG, "args:" + selectionArgs.toString());

        updateValues = CalendarHelper.getUpdateCalendarValuesWithOriginal(values, seed++);

        assertEquals(1, mContentResolver.update(
                Calendars.CONTENT_URI, updateValues, selection, selectionArgs));

        verifyCalendar(account, values, id);

        // Delete
        assertEquals(1, CalendarHelper.deleteCalendarById(mContentResolver, id));

        // Verify
        Cursor c = CalendarHelper.getCalendarsByAccount(mContentResolver, account);
        assertEquals(0, c.getCount());
        c.close();
    }

    /**
     * @param account
     * @param values
     * @param id
     */
    private void verifyCalendar(String account, ContentValues values, long id) {
        // Verify
        Cursor c = CalendarHelper.getCalendarsByAccount(mContentResolver, account);
        assertEquals(1, c.getCount());
        assertTrue(c.moveToFirst());
        assertEquals(id, c.getLong(0));
        for (String key : values.keySet()) {
            int index = c.getColumnIndex(key);
            assertEquals(values.getAsString(key), c.getString(index));
        }
        c.close();
    }

}
