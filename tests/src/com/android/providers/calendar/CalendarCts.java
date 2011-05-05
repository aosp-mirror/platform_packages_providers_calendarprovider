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
import android.provider.Calendar.Events;
import android.test.InstrumentationTestCase;
import android.test.suitebuilder.annotation.*;
import android.text.format.DateUtils;
import android.util.Log;

public class CalendarCts extends InstrumentationTestCase {

    private static final String TAG = "CalCTS";
    private static final String CTS_TEST_TYPE = "LOCAL";
    // @formatter:off
    private static final String[] TIME_ZONES = new String[] {
            "UTC",
            "America/Los_Angeles",
            "Asia/Beirut",
            "Pacific/Auckland", };
    // @formatter:on

    private ContentResolver mContentResolver;

    private static class CalendarHelper {

        // @formatter:off
        public static final String[] CALENDARS_SYNC_PROJECTION = new String[] {
                Calendars._ID,
                Calendars.ACCOUNT_NAME,
                Calendars.ACCOUNT_TYPE,
                Calendars._SYNC_ID,
                Calendars._SYNC_VERSION,
                Calendars._SYNC_TIME,
                Calendars.DIRTY,
                Calendars.NAME,
                Calendars.DISPLAY_NAME,
                Calendars.CALENDAR_COLOR,
                Calendars.ACCESS_LEVEL,
                Calendars.VISIBLE,
                Calendars.SYNC_EVENTS,
                Calendars.CALENDAR_LOCATION,
                Calendars.CALENDAR_TIMEZONE,
                Calendars.OWNER_ACCOUNT,
                Calendars.CAN_ORGANIZER_RESPOND,
                Calendars.CAN_MODIFY_TIME_ZONE,
                Calendars.MAX_REMINDERS,
                Calendars.DELETED,
                Calendars.SYNC1,
                Calendars.SYNC2,
                Calendars.SYNC3,
                Calendars.SYNC4,
                Calendars.SYNC5,
                Calendars.SYNC6,
                };
        // @formatter:on

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
            values.put(Calendars.ACCOUNT_TYPE, CTS_TEST_TYPE);

            values.put(Calendars.ACCOUNT_NAME, account);
            values.put(Calendars._SYNC_ID, "SYNC_ID:" + seedString);
            values.put(Calendars._SYNC_VERSION, "SYNC_V:" + seedString);
            values.put(Calendars._SYNC_TIME, "SYNC_TIME:" + seedString);
            values.put(Calendars.DIRTY, 0);
            values.put(Calendars.OWNER_ACCOUNT, "OWNER_" + account);

            values.put(Calendars.NAME, seedString);
            values.put(Calendars.DISPLAY_NAME, "DISPLAY_" + seedString);

            values.put(Calendars.ACCESS_LEVEL, (seed % 8) * 100);

            values.put(Calendars.CALENDAR_COLOR, 0xff000000 + seed);
            values.put(Calendars.VISIBLE, seed % 2);
            values.put(Calendars.SYNC_EVENTS, seed % 2);
            values.put(Calendars.CALENDAR_LOCATION, "LOCATION:" + seedString);
            values.put(Calendars.CALENDAR_TIMEZONE, TIME_ZONES[seed % TIME_ZONES.length]);
            values.put(Calendars.CAN_ORGANIZER_RESPOND, seed % 2);
            values.put(Calendars.CAN_MODIFY_TIME_ZONE, seed % 2);
            values.put(Calendars.MAX_REMINDERS, seed);
            values.put(Calendars.SYNC1, "SYNC1:" + seedString);
            values.put(Calendars.SYNC2, "SYNC2:" + seedString);
            values.put(Calendars.SYNC3, "SYNC3:" + seedString);
            values.put(Calendars.SYNC4, "SYNC4:" + seedString);
            values.put(Calendars.SYNC5, "SYNC5:" + seedString);
            values.put(Calendars.SYNC6, "SYNC6:" + seedString);

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
            values.put(Calendars.CALENDAR_COLOR, 0xff000000 + seed);
            values.put(Calendars.VISIBLE, seed % 2);
            values.put(Calendars.SYNC_EVENTS, seed % 2);

            original.putAll(values);
            original.put(Calendars.DIRTY, 1);

            return values;
        }

        public static int deleteCalendarById(ContentResolver resolver, long id) {
            return resolver.delete(Calendars.CONTENT_URI, Calendars._ID + "=?",
                    new String[] { Long.toString(id) });
        }

        public static int deleteCalendarByAccount(ContentResolver resolver, String account) {
            return resolver.delete(Calendars.CONTENT_URI, Calendars.ACCOUNT_NAME + "=?",
                    new String[] { account });
        }

        public static Cursor getCalendarsByAccount(ContentResolver resolver, String account) {
            String selection = Calendars.ACCOUNT_TYPE + "=?";
            String[] selectionArgs;
            if (account != null) {
                selection += " AND " + Calendars.ACCOUNT_NAME + "=?";
                selectionArgs = new String[2];
                selectionArgs[1] = account;
            } else {
                selectionArgs = new String[1];
            }
            selectionArgs[0] = CTS_TEST_TYPE;

            return resolver.query(Calendars.CONTENT_URI, CALENDARS_SYNC_PROJECTION, selection,
                    selectionArgs, null);
        }
    }

    // @formatter:off
    private static class EventHelper {
        public static final String[] EVENTS_PROJECTION = new String[] {
            Events._ID,
            Events.ACCOUNT_NAME,
            Events.ACCOUNT_TYPE,
            Events.OWNER_ACCOUNT,
            // Events.ORGANIZER_CAN_RESPOND, from Calendars
            // Events.CAN_CHANGE_TZ, from Calendars
            // Events.MAX_REMINDERS, from Calendars
            Events.CALENDAR_ID,
            // Events.CALENDAR_DISPLAY_NAME, from Calendars
            // Events.CALENDAR_COLOR, from Calendars
            // Events.CALENDAR_ACL, from Calendars
            // Events.CALENDAR_VISIBLE, from Calendars
            Events.HTML_URI,
            Events.COMMENTS_URI,
            Events.TITLE,
            Events.EVENT_LOCATION,
            Events.DESCRIPTION,
            Events.STATUS,
            Events.SELF_ATTENDEE_STATUS,
            Events.DTSTART,
            Events.DTEND,
            Events.EVENT_TIMEZONE,
            Events.EVENT_END_TIMEZONE,
            Events.DURATION,
            Events.ALL_DAY,
            Events.ACCESS_LEVEL,
            Events.AVAILABILITY,
            Events.HAS_ALARM,
            Events.HAS_EXTENDED_PROPERTIES,
            Events.RRULE,
            Events.RDATE,
            Events.EXRULE,
            Events.EXDATE,
            // Events.ORIGINAL_ID
            Events.ORIGINAL_SYNC_ID, // rename ORIGINAL_SYNC_ID
            Events.ORIGINAL_INSTANCE_TIME,
            Events.ORIGINAL_ALL_DAY,
            Events.LAST_DATE,
            Events.HAS_ATTENDEE_DATA,
            Events.GUESTS_CAN_MODIFY,
            Events.GUESTS_CAN_INVITE_OTHERS,
            Events.GUESTS_CAN_SEE_GUESTS,
            Events.ORGANIZER,
            Events.DELETED,
            Events._SYNC_ID,
            Events._SYNC_VERSION,
            Events._SYNC_TIME,
            Events.DIRTY,
            Events._SYNC_MARK,
            Events._SYNC_DATA, // Events.SYNC_DATA1
            // Events.SYNC_DATA2
            // Events.SYNC_DATA3
            // Events.SYNC_DATA4
        };
        // @formatter:on

        public static ContentValues getNewEventValues(
                String account, int seed, long calendarId, boolean asSyncAdapter) {
            String seedString = Long.toString(seed);
            ContentValues values = new ContentValues();
            values.put(Events.ORGANIZER, "ORGANIZER:" + seedString);

            values.put(Events.TITLE, "TITLE:" + seedString);
            values.put(Events.EVENT_LOCATION, "LOCATION_" + seedString);

            values.put(Events.CALENDAR_ID, calendarId);

            values.put(Events.DESCRIPTION, "DESCRIPTION:" + seedString);
            values.put(Events.STATUS, seed % 3);

            values.put(Events.DTSTART, seed);
            values.put(Events.DTEND, seed + DateUtils.HOUR_IN_MILLIS);
            values.put(Events.EVENT_TIMEZONE, TIME_ZONES[seed % TIME_ZONES.length]);
            // values.put(Events.EVENT_TIMEZONE2, TIME_ZONES[(seed +1) %
            // TIME_ZONES.length]);
            values.put(Events.ALL_DAY, 0); // must be 0 or dtstart/dtend will
                                           // get adjusted
            values.put(Events.ACCESS_LEVEL, seed % 4);
            values.put(Events.AVAILABILITY, seed % 2);
            values.put(Events.HAS_ALARM, seed % 2);
            values.put(Events.HAS_EXTENDED_PROPERTIES, seed % 2);
            values.put(Events.HAS_ATTENDEE_DATA, seed % 2);
            values.put(Events.GUESTS_CAN_MODIFY, seed % 2);
            values.put(Events.GUESTS_CAN_INVITE_OTHERS, seed % 2);
            values.put(Events.GUESTS_CAN_SEE_GUESTS, seed % 2);

            if (asSyncAdapter) {
                values.put(Events._SYNC_ID, "SYNC_ID:" + seedString);
                values.put(Events._SYNC_VERSION, "SYNC_V:" + seedString);
                values.put(Events._SYNC_TIME, "SYNC_TIME:" + seedString);
                values.put(Events.HTML_URI, "HTML:" + seedString);
                values.put(Events.COMMENTS_URI, "COMMENTS:" + seedString);
                values.put(Events.DIRTY, 0);
                values.put(Events._SYNC_MARK, 0);
            }
            // values.put(Events.SYNC1, "SYNC1:" + seedString);
            // values.put(Events.SYNC2, "SYNC2:" + seedString);
            // values.put(Events.SYNC3, "SYNC3:" + seedString);
            // values.put(Events.SYNC4, "SYNC4:" + seedString);
            // values.put(Events.SYNC5, "SYNC5:" + seedString);
//            Events.RRULE,
//            Events.RDATE,
//            Events.EXRULE,
//            Events.EXDATE,
//            // Events.ORIGINAL_ID
//            Events.ORIGINAL_EVENT, // rename ORIGINAL_SYNC_ID
//            Events.ORIGINAL_INSTANCE_TIME,
//            Events.ORIGINAL_ALL_DAY,

            return values;
        }

        public static ContentValues getUpdateEventValuesWithOriginal(ContentValues original,
                int seed, boolean asSyncAdapter) {
            String seedString = Long.toString(seed);
            ContentValues values = new ContentValues();

            values.put(Events.TITLE, "TITLE:" + seedString);
            values.put(Events.EVENT_LOCATION, "LOCATION_" + seedString);
            values.put(Events.DESCRIPTION, "DESCRIPTION:" + seedString);
            values.put(Events.STATUS, seed % 3);

            values.put(Events.DTSTART, seed);
            values.put(Events.DTEND, seed + DateUtils.HOUR_IN_MILLIS);
            values.put(Events.EVENT_TIMEZONE, TIME_ZONES[seed % TIME_ZONES.length]);
            // values.put(Events.EVENT_TIMEZONE2, TIME_ZONES[(seed +1) %
            // TIME_ZONES.length]);
            values.put(Events.ACCESS_LEVEL, seed % 4);
            values.put(Events.AVAILABILITY, seed % 2);
            values.put(Events.HAS_ALARM, seed % 2);
            values.put(Events.HAS_EXTENDED_PROPERTIES, seed % 2);
            values.put(Events.HAS_ATTENDEE_DATA, seed % 2);
            values.put(Events.GUESTS_CAN_MODIFY, seed % 2);
            values.put(Events.GUESTS_CAN_INVITE_OTHERS, seed % 2);
            values.put(Events.GUESTS_CAN_SEE_GUESTS, seed % 2);
            if (asSyncAdapter) {
                values.put(Events._SYNC_ID, "SYNC_ID:" + seedString);
                values.put(Events._SYNC_VERSION, "SYNC_V:" + seedString);
                values.put(Events._SYNC_TIME, "SYNC_TIME:" + seedString);
            }
            original.putAll(values);
            original.put(Events.DIRTY, asSyncAdapter ? 0 : 1);
            return values;
        }

        public static void addDefaultReadOnlyValues(ContentValues values, String account,
                boolean asSyncAdapter) {
            values.put(Events.SELF_ATTENDEE_STATUS, Events.STATUS_TENTATIVE);
            values.put(Events.DELETED, 0);
            values.put(Events.DIRTY, asSyncAdapter ? 0 : 1);
            values.put(Events.OWNER_ACCOUNT, "OWNER_" + account);
            values.put(Events.ACCOUNT_TYPE, CTS_TEST_TYPE);
            values.put(Events.ACCOUNT_NAME, account);
        }

        public static int deleteEvent(ContentResolver resolver, Uri uri, ContentValues values) {
            values.put(Events.DELETED, 1);
            values.put(Events.DIRTY, 1);
            return resolver.delete(uri, null, null);
        }

        public static int deleteEventAsSyncAdapter(ContentResolver resolver, Uri uri,
                String account) {
            Uri syncUri = asSyncAdapter(uri, account, CTS_TEST_TYPE);
            return resolver.delete(syncUri, null, null);
        }

        public static Cursor getEventsByAccount(ContentResolver resolver, String account) {
            String selection = Calendars.ACCOUNT_TYPE + "=?";
            String[] selectionArgs;
            if (account != null) {
                selection += " AND " + Calendars.ACCOUNT_NAME + "=?";
                selectionArgs = new String[2];
                selectionArgs[1] = account;
            } else {
                selectionArgs = new String[1];
            }
            selectionArgs[0] = CTS_TEST_TYPE;
            return resolver.query(Events.CONTENT_URI, EVENTS_PROJECTION, selection, selectionArgs,
                    null);
        }

        public static Cursor getEventByUri(ContentResolver resolver, Uri uri) {
            return resolver.query(uri, EVENTS_PROJECTION, null, null, null);
        }
    }

    static Uri asSyncAdapter(Uri uri, String account, String accountType) {
        return uri.buildUpon()
                .appendQueryParameter(android.provider.Calendar.CALLER_IS_SYNCADAPTER, "true")
                .appendQueryParameter(Calendars.ACCOUNT_NAME, account)
                .appendQueryParameter(Calendars.ACCOUNT_TYPE, accountType).build();
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
        long id = createAndVerifyCalendar(account, seed++, null);

        removeAndVerifyCalendar(account, id);
    }

    @MediumTest
    public void testCalendarUpdateAsApp() {
        String account = "cu1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create a calendar
        ContentValues values = CalendarHelper.getNewCalendarValues(account, seed);
        long id = createAndVerifyCalendar(account, seed++, values);

        Uri uri = ContentUris.withAppendedId(Calendars.CONTENT_URI, id);

        // Update the calendar using the direct Uri
        ContentValues updateValues = CalendarHelper.getUpdateCalendarValuesWithOriginal(
                values, seed++);
        assertEquals(1, mContentResolver.update(uri, updateValues, null, null));

        verifyCalendar(account, values, id);

        // Update the calendar using selection + args
        String selection = Calendars._ID + "=?";
        String[] selectionArgs = new String[] { Long.toString(id) };

        updateValues = CalendarHelper.getUpdateCalendarValuesWithOriginal(values, seed++);

        assertEquals(1, mContentResolver.update(
                Calendars.CONTENT_URI, updateValues, selection, selectionArgs));

        verifyCalendar(account, values, id);

        removeAndVerifyCalendar(account, id);
    }

    // TODO test calendar updates as sync adapter

    private void verifyEvent(ContentValues values, long eventId) {
        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);
        // Verify
        Cursor c = mContentResolver
                .query(eventUri, EventHelper.EVENTS_PROJECTION, null, null, null);
        assertEquals(1, c.getCount());
        assertTrue(c.moveToFirst());
        assertEquals(eventId, c.getLong(0));
        for (String key : values.keySet()) {
            int index = c.getColumnIndex(key);
            assertEquals(key, values.getAsString(key), c.getString(index));
        }
        c.close();
    }

    @MediumTest
    public void testEventCreationAndDeletion() {
        String account = "ec1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar and event
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        ContentValues eventValues = EventHelper
                .getNewEventValues(account, seed++, calendarId, true);
        long eventId = createAndVerifyEvent(account, seed, calendarId, eventValues);

        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);

        removeAndVerifyEvent(eventUri, eventValues, account);

        removeAndVerifyCalendar(account, calendarId);
    }

    @MediumTest
    public void testEventUpdateAsApp() {

        String account = "em1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar and event
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        ContentValues eventValues = EventHelper
                .getNewEventValues(account, seed++, calendarId, true);
        long eventId = createAndVerifyEvent(account, seed, calendarId, eventValues);

        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);

        ContentValues updateVals = EventHelper.getUpdateEventValuesWithOriginal(eventValues,
                seed++, false);
        assertEquals(1, mContentResolver.update(eventUri, updateVals, null, null));
        verifyEvent(eventValues, eventId);

        removeAndVerifyEvent(eventUri, eventValues, account);

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    // TODO test modifying an event as sync adater

    @MediumTest
    public void testSyncOnlyInsertEnforcement() {
        String account = "so1_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar and event
        long calendarId = createAndVerifyCalendar(account, seed++, null);
        CalendarHelper.deleteCalendarById(mContentResolver, calendarId);
        ContentValues vals = new ContentValues();
        for (int i = 0; i < Calendars.SYNC_WRITABLE_COLUMNS.length; i++) {
            boolean threw = false;
            try {
                vals.clear();
                vals.put(Calendars.SYNC_WRITABLE_COLUMNS[i], "1");
                mContentResolver.insert(Calendars.CONTENT_URI, vals);
            } catch (IllegalArgumentException e) {
                threw = true;
            }
            assertTrue("Only sync adapter should be allowed to insert "
                    + Calendars.SYNC_WRITABLE_COLUMNS[i], threw);
        }
    }

    /**
     * Deletes an event as app and sync adapter which removes it from the db and
     * verifies after each.
     *
     * @param eventUri The uri for the event to delete
     * @param accountName TODO
     */
    private void removeAndVerifyEvent(Uri eventUri, ContentValues eventValues, String accountName) {
        // Delete event
        EventHelper.deleteEvent(mContentResolver, eventUri, eventValues);
        // Verify
        verifyEvent(eventValues, ContentUris.parseId(eventUri));
        // Delete as sync adapter
        assertEquals(1,
                EventHelper.deleteEventAsSyncAdapter(mContentResolver, eventUri, accountName));
        // Verify
        Cursor c = EventHelper.getEventByUri(mContentResolver, eventUri);
        assertEquals(0, c.getCount());
        c.close();
    }

    /**
     * Creates an event on the given calendar and verifies it.
     *
     * @param account
     * @param seed
     * @param calendarId
     * @param values optional pre created set of values
     * @return the _id for the new event
     */
    private long createAndVerifyEvent(String account, int seed, long calendarId,
            ContentValues values) {
        // Create an event
        if (values == null) {
            values = EventHelper.getNewEventValues(account, seed, calendarId, true /* as sync*/);
        }
        Uri syncUri = asSyncAdapter(Events.CONTENT_URI, account, CTS_TEST_TYPE);
        Uri uri = mContentResolver.insert(syncUri, values);

        // Verify
        EventHelper.addDefaultReadOnlyValues(values, account, true /* as sync */);
        long eventId = ContentUris.parseId(uri);
        assertTrue(eventId >= 0);

        verifyEvent(values, eventId);
        return eventId;
    }

    /**
     * Inserts a new calendar with the given account and seed and verifies it.
     *
     * @param account The account to add the calendar to
     * @param seed A number to use to generate the values
     * @return the created calendar's id
     */
    private long createAndVerifyCalendar(String account, int seed, ContentValues values) {
        // Create a calendar
        if (values == null) {
            values = CalendarHelper.getNewCalendarValues(account, seed);
        }
        Uri syncUri = asSyncAdapter(Calendars.CONTENT_URI, account, CTS_TEST_TYPE);
        Uri uri = mContentResolver.insert(syncUri, values);
        long calendarId = ContentUris.parseId(uri);
        assertTrue(calendarId >= 0);

        verifyCalendar(account, values, calendarId);
        return calendarId;
    }

    /**
     * Deletes a given calendar and verifies no calendars remain on that
     * account.
     *
     * @param account
     * @param id
     */
    private void removeAndVerifyCalendar(String account, long id) {
        // TODO Add code to delete as app and sync adapter and test both

        // Delete
        assertEquals(1, CalendarHelper.deleteCalendarById(mContentResolver, id));

        // Verify
        Cursor c = CalendarHelper.getCalendarsByAccount(mContentResolver, account);
        assertEquals(0, c.getCount());
        c.close();
    }

    /**
     * Check all the fields of a calendar contained in values + id. This assumes
     * a single calendar has been created on the given account.
     *
     * @param account the account of the calendar
     * @param values the values to check against the db
     * @param id the _id of the calendar
     */
    private void verifyCalendar(String account, ContentValues values, long id) {
        // Verify
        Cursor c = CalendarHelper.getCalendarsByAccount(mContentResolver, account);
        assertEquals(1, c.getCount());
        assertTrue(c.moveToFirst());
        assertEquals(id, c.getLong(0));
        for (String key : values.keySet()) {
            int index = c.getColumnIndex(key);
            assertEquals(key, values.getAsString(key), c.getString(index));
        }
        c.close();
    }

}
