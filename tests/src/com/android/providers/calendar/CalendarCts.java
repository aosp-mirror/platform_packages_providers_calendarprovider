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
import android.database.Cursor;
import android.net.Uri;
import android.provider.CalendarContract;
import android.provider.CalendarContract.Attendees;
import android.provider.CalendarContract.Calendars;
import android.provider.CalendarContract.Events;
import android.provider.CalendarContract.Instances;
import android.provider.CalendarContract.Reminders;
import android.test.InstrumentationTestCase;
import android.test.suitebuilder.annotation.*;
import android.text.format.DateUtils;
import android.text.format.Time;
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

    private static final String SQL_WHERE_ID = Events._ID + "=?";
    private static final String SQL_WHERE_CALENDAR_ID = Events.CALENDAR_ID + "=?";

    private ContentResolver mContentResolver;

    /** If set, log verbose instance info when running recurrence tests. */
    private static final boolean DEBUG_RECURRENCE = false;

    private static class CalendarHelper {

        // @formatter:off
        public static final String[] CALENDARS_SYNC_PROJECTION = new String[] {
                Calendars._ID,
                Calendars.ACCOUNT_NAME,
                Calendars.ACCOUNT_TYPE,
                Calendars._SYNC_ID,
                Calendars.CAL_SYNC7,
                Calendars.CAL_SYNC8,
                Calendars.DIRTY,
                Calendars.NAME,
                Calendars.CALENDAR_DISPLAY_NAME,
                Calendars.CALENDAR_COLOR,
                Calendars.CALENDAR_ACCESS_LEVEL,
                Calendars.VISIBLE,
                Calendars.SYNC_EVENTS,
                Calendars.CALENDAR_LOCATION,
                Calendars.CALENDAR_TIME_ZONE,
                Calendars.OWNER_ACCOUNT,
                Calendars.CAN_ORGANIZER_RESPOND,
                Calendars.CAN_MODIFY_TIME_ZONE,
                Calendars.MAX_REMINDERS,
                Calendars.DELETED,
                Calendars.CAL_SYNC1,
                Calendars.CAL_SYNC2,
                Calendars.CAL_SYNC3,
                Calendars.CAL_SYNC4,
                Calendars.CAL_SYNC5,
                Calendars.CAL_SYNC6,
                };
        // @formatter:on

        private CalendarHelper() {}     // do not instantiate this class

        static String generateCalendarOwnerEmail(String account) {
            return "OWNER_" + account + "@example.com";
        }

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
            values.put(Calendars.CAL_SYNC7, "SYNC_V:" + seedString);
            values.put(Calendars.CAL_SYNC8, "SYNC_TIME:" + seedString);
            values.put(Calendars.DIRTY, 0);
            values.put(Calendars.OWNER_ACCOUNT, generateCalendarOwnerEmail(account));

            values.put(Calendars.NAME, seedString);
            values.put(Calendars.CALENDAR_DISPLAY_NAME, "DISPLAY_" + seedString);

            values.put(Calendars.CALENDAR_ACCESS_LEVEL, (seed % 8) * 100);

            values.put(Calendars.CALENDAR_COLOR, 0xff000000 + seed);
            values.put(Calendars.VISIBLE, seed % 2);
            values.put(Calendars.SYNC_EVENTS, 1);   // must be 1 for recurrence expansion
            values.put(Calendars.CALENDAR_LOCATION, "LOCATION:" + seedString);
            values.put(Calendars.CALENDAR_TIME_ZONE, TIME_ZONES[seed % TIME_ZONES.length]);
            values.put(Calendars.CAN_ORGANIZER_RESPOND, seed % 2);
            values.put(Calendars.CAN_MODIFY_TIME_ZONE, seed % 2);
            values.put(Calendars.MAX_REMINDERS, seed);
            values.put(Calendars.CAL_SYNC1, "SYNC1:" + seedString);
            values.put(Calendars.CAL_SYNC2, "SYNC2:" + seedString);
            values.put(Calendars.CAL_SYNC3, "SYNC3:" + seedString);
            values.put(Calendars.CAL_SYNC4, "SYNC4:" + seedString);
            values.put(Calendars.CAL_SYNC5, "SYNC5:" + seedString);
            values.put(Calendars.CAL_SYNC6, "SYNC6:" + seedString);

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

            values.put(Calendars.CALENDAR_DISPLAY_NAME, "DISPLAY_" + seedString);
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
            Events.SYNC_DATA3,
            Events.SYNC_DATA6,
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
            Events.ORIGINAL_ID,
            Events.ORIGINAL_SYNC_ID,
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
            Events.SYNC_DATA4,
            Events.SYNC_DATA5,
            Events.DIRTY,
            Events.SYNC_DATA8,
            Events.SYNC_DATA2, // Events.SYNC_DATA1
            // Events.SYNC_DATA2
            // Events.SYNC_DATA3
            // Events.SYNC_DATA4
        };
        // @formatter:on

        private EventHelper() {}    // do not instantiate this class

        /**
         * Constructs a set of name/value pairs that can be used to create a Calendar event.
         * Various fields are generated from the seed value.
         */
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
            if ((seed % 2) == 0) {
                // Either set to zero, or leave unset to get default zero.
                // Must be 0 or dtstart/dtend will get adjusted.
                values.put(Events.ALL_DAY, 0);
            }
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
                values.put(Events.SYNC_DATA4, "SYNC_V:" + seedString);
                values.put(Events.SYNC_DATA5, "SYNC_TIME:" + seedString);
                values.put(Events.SYNC_DATA3, "HTML:" + seedString);
                values.put(Events.SYNC_DATA6, "COMMENTS:" + seedString);
                values.put(Events.DIRTY, 0);
                values.put(Events.SYNC_DATA8, "0");
            } else {
                // only the sync adapter can set the DIRTY flag
                //values.put(Events.DIRTY, 1);
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

        /**
         * Constructs a set of name/value pairs that can be used to create a recurring
         * Calendar event.
         *
         * A duration of "P1D" is treated as an all-day event.
         *
         * @param startWhen Starting date/time in RFC 3339 format
         * @param duration Event duration, in RFC 2445 duration format
         * @param rrule Recurrence rule
         * @return name/value pairs to use when creating event
         */
        public static ContentValues getNewRecurringEventValues(String account, int seed,
                long calendarId, boolean asSyncAdapter, String startWhen, String duration,
                String rrule) {

            // Set up some general stuff.
            ContentValues values = getNewEventValues(account, seed, calendarId, asSyncAdapter);

            // Replace the DTSTART field.
            String timeZone = values.getAsString(Events.EVENT_TIMEZONE);
            Time time = new Time(timeZone);
            time.parse3339(startWhen);
            values.put(Events.DTSTART, time.toMillis(false));

            // Add in the recurrence-specific fields, and drop DTEND.
            values.put(Events.RRULE, rrule);
            values.put(Events.DURATION, duration);
            values.remove(Events.DTEND);

            return values;
        }

        /**
         * Constructs the basic name/value pairs required for an exception to a recurring event.
         *
         * @param instanceStartMillis The start time of the instance
         * @return name/value pairs to use when creating event
         */
        public static ContentValues getNewExceptionValues(long instanceStartMillis) {
            ContentValues values = new ContentValues();
            values.put(Events.ORIGINAL_INSTANCE_TIME, instanceStartMillis);

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
                values.put(Events.SYNC_DATA4, "SYNC_V:" + seedString);
                values.put(Events.SYNC_DATA5, "SYNC_TIME:" + seedString);
                values.put(Events.DIRTY, 0);
            }
            original.putAll(values);
            return values;
        }

        public static void addDefaultReadOnlyValues(ContentValues values, String account,
                boolean asSyncAdapter) {
            values.put(Events.SELF_ATTENDEE_STATUS, Events.STATUS_TENTATIVE);
            values.put(Events.DELETED, 0);
            values.put(Events.DIRTY, asSyncAdapter ? 0 : 1);
            values.put(Events.OWNER_ACCOUNT, CalendarHelper.generateCalendarOwnerEmail(account));
            values.put(Events.ACCOUNT_TYPE, CTS_TEST_TYPE);
            values.put(Events.ACCOUNT_NAME, account);
        }

        /**
         * Generates a RFC2445-format duration string.
         */
        private static String generateDurationString(long durationMillis, boolean isAllDay) {
            long durationSeconds = durationMillis / 1000;

            // The server may react differently to an all-day event specified as "P1D" than
            // it will to "PT86400S"; see b/1594638.
            if (isAllDay && (durationSeconds % 86400) == 0) {
                return "P" + durationSeconds / 86400 + "D";
            } else {
                return "PT" + durationSeconds + "S";
            }
        }

        /**
         * Deletes the event, and updates the values.
         * @param resolver The resolver to issue the query against.
         * @param uri The deletion URI.
         * @param values Set of values to update (sets DELETED and DIRTY).
         * @return The number of rows modified.
         */
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
                .appendQueryParameter(android.provider.CalendarContract.CALLER_IS_SYNCADAPTER,
                        "true")
                .appendQueryParameter(Calendars.ACCOUNT_NAME, account)
                .appendQueryParameter(Calendars.ACCOUNT_TYPE, accountType).build();
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mContentResolver = getInstrumentation().getTargetContext().getContentResolver();
        // IContentProvider provider = mContentResolver.acquireProvider(Calendar.AUTHORITY);
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
        long eventId = createAndVerifyEvent(account, seed, calendarId, true, eventValues);

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

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create event as sync adapter
        ContentValues eventValues = EventHelper
                .getNewEventValues(account, seed++, calendarId, true);
        long eventId = createAndVerifyEvent(account, seed, calendarId, true, eventValues);

        // Update event as app
        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);

        ContentValues updateValues = EventHelper.getUpdateEventValuesWithOriginal(eventValues,
                seed++, false);
        assertEquals(1, mContentResolver.update(eventUri, updateValues, null, null));
        updateValues.put(Events.DIRTY, 1);      // provider should have marked as dirty
        verifyEvent(updateValues, eventId);

        removeAndVerifyEvent(eventUri, eventValues, account);

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests update of multiple events with a single update call.
     */
    @MediumTest
    public void testBulkUpdate() {
        String account = "bup_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);
        String calendarIdStr = String.valueOf(calendarId);

        // Create events
        ContentValues eventValues;
        eventValues = EventHelper.getNewEventValues(account, seed++, calendarId, true);
        long eventId1 = createAndVerifyEvent(account, seed, calendarId, true, eventValues);

        eventValues = EventHelper.getNewEventValues(account, seed++, calendarId, true);
        long eventId2 = createAndVerifyEvent(account, seed, calendarId, true, eventValues);

        // Update the "description" field in all events in this calendar.
        String newDescription = "bulk edit";
        ContentValues updateValues = new ContentValues();
        updateValues.put(Events.DESCRIPTION, newDescription);

        // Must be sync adapter to do a bulk update.
        Uri uri = asSyncAdapter(Events.CONTENT_URI, account, CTS_TEST_TYPE);
        int count = mContentResolver.update(uri, updateValues, SQL_WHERE_CALENDAR_ID,
                new String[] { calendarIdStr });

        // Check to see if the changes went through.
        Uri eventUri = Events.CONTENT_URI;
        Cursor c = mContentResolver.query(eventUri, new String[] { Events.DESCRIPTION },
                SQL_WHERE_CALENDAR_ID, new String[] { calendarIdStr }, null);
        assertEquals(2, c.getCount());
        while (c.moveToNext()) {
            assertEquals(newDescription, c.getString(0));
        }
        c.close();
    }

    /**
     * Tests the content provider's enforcement of restrictions on who is allowed to modify
     * specific columns in a Calendar.
     * <p>
     * This attempts to create a new row in the Calendar table, specifying one restricted
     * column at a time.
     */
    @MediumTest
    public void testSyncOnlyInsertEnforcement() {
        // These operations should not succeed, so there should be nothing to clean up after.
        // TODO: this should be a new event augmented with an illegal column, not a single
        //       column.  Otherwise we might be tripping over a "DTSTART must exist" test.
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
     * Tests creation of a recurring event.
     * <p>
     * This (and the other recurrence tests) uses dates well in the past to reduce the likelihood
     * of encountering non-test recurring events.  (Ideally we would select events associated
     * with a specific calendar.)  With dates well in the past, it's also important to have a
     * fixed maximum count or end date; otherwise, if the metadata min/max instance values are
     * large enough, the recurrence recalculation processor could get triggered on an insert or
     * update and bump up against the 2000-instance limit.
     *
     * TODO: need some allDay tests
     */
    @MediumTest
    public void testRecurrence() {
        String account = "re_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "2003-08-05T09:00:00", "PT1H",
                "FREQ=WEEKLY;INTERVAL=2;COUNT=4;BYDAY=TU,SU;WKST=SU");
        long eventId = createAndVerifyEvent(account, seed, calendarId, true, eventValues);
        //Log.d(TAG, "+++ basic recurrence eventId is " + eventId);

        // Check to see if we have the expected number of instances
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        int instanceCount = getInstanceCount(timeZone, "2003-08-05T00:00:00",
                "2003-08-31T11:59:59");
        if (false) {
            Cursor instances = getInstances(timeZone, "2003-08-05T00:00:00", "2003-08-31T11:59:59",
                    new String[] { Instances.BEGIN });
            dumpInstances(instances, timeZone, "initial");
        }
        assertEquals("recurrence instance count", 4, instanceCount);

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests conversion of a regular event to a recurring event.
     */
    @MediumTest
    public void testConversionToRecurring() {
        String account = "reconv_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar and event
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        ContentValues eventValues = EventHelper
                .getNewEventValues(account, seed++, calendarId, true);
        long eventId = createAndVerifyEvent(account, seed, calendarId, true, eventValues);

        long dtstart = eventValues.getAsLong(Events.DTSTART);
        long dtend = eventValues.getAsLong(Events.DTEND);
        long durationSecs = (dtend - dtstart) / 1000;

        ContentValues updateValues = new ContentValues();
        updateValues.put(Events.RRULE, "FREQ=WEEKLY");   // recurs forever
        updateValues.put(Events.DURATION, "P" + durationSecs + "S");
        updateValues.putNull(Events.DTEND);

        // Issue update; do it as app instead of sync adapter to exercise that path.
        updateAndVerifyEvent(account, calendarId, eventId, false, updateValues);

        // Make sure LAST_DATE got nulled out by our infinitely repeating sequence.
        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);
        Cursor c = mContentResolver.query(eventUri, new String[] { Events.LAST_DATE },
                null, null, null);
        assertEquals(1, c.getCount());
        assertTrue(c.moveToFirst());
        assertNull(c.getString(0));
        c.close();

        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests creation of a recurring event with single-instance exceptions.
     */
    @MediumTest
    public void testSingleRecurrenceExceptions() {
        String account = "rex_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event.
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "1999-03-28T09:00:00", "PT1H", "FREQ=WEEKLY;WKST=SU;COUNT=100");
        long eventId = createAndVerifyEvent(account, seed++, calendarId, true, eventValues);

        // Add some attendees and reminders.
        addAttendees(account, eventId, seed);
        addReminders(account, eventId, seed);

        // Select a period that gives us 5 instances.
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        String testStart = "1999-03-28T00:00:00";
        String testEnd = "1999-04-25T23:59:59";
        String[] projection = { Instances.BEGIN, Instances.START_MINUTE, Instances.END_MINUTE };

        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "initial");
        }

        assertEquals("initial recurrence instance count", 5, instances.getCount());

        /*
         * Advance the start time of a few instances, and verify.
         */

        // Leave first instance alone.
        instances.moveToPosition(1);

        long startMillis;
        ContentValues excepValues;

        // Advance the start time of the 2nd instance.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DTSTART, startMillis + 3600*1000);
        long excepEventId2 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        // Advance the start time of the 3rd instance.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DTSTART, startMillis + 3600*1000*2);
        long excepEventId3 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        // Cancel the 4th instance.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.STATUS, Events.STATUS_CANCELED);
        long excepEventId4 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        // TODO: try to modify a non-existent instance.

        instances.close();

        // TODO: compare Reminders, Attendees, ExtendedProperties on one of the exception events

        // Re-query the instances and figure out if they look right.
        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "with DTSTART exceptions");
        }
        assertEquals("exceptional recurrence instance count", 4, instances.getCount());

        long prevMinute = -1;
        while (instances.moveToNext()) {
            // expect the start times for each entry to be different from the previous entry
            long startMinute = instances.getLong(1);
            assertTrue("instance start times are different", startMinute != prevMinute);

            prevMinute = startMinute;
        }
        instances.close();


        // Delete all of our exceptions, and verify.
        int deleteCount = 0;
        deleteCount += deleteException(account, eventId, excepEventId2);
        deleteCount += deleteException(account, eventId, excepEventId3);
        deleteCount += deleteException(account, eventId, excepEventId4);
        assertEquals("events deleted", 3, deleteCount);

        // Re-query the instances and figure out if they look right.
        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "post exception deletion");
        }
        assertEquals("post-exception deletion instance count", 5, instances.getCount());

        prevMinute = -1;
        while (instances.moveToNext()) {
            // expect the start times for each entry to be the same
            long startMinute = instances.getLong(1);
            if (prevMinute != -1) {
                assertEquals("instance start times are the same", startMinute, prevMinute);
            }
            prevMinute = startMinute;
        }
        instances.close();

        /*
         * Repeat the test, this time modifying DURATION.
         */

        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "initial");
        }

        assertEquals("initial recurrence instance count", 5, instances.getCount());

        // Leave first instance alone.
        instances.moveToPosition(1);

        // Advance the end time of the 2nd instance.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DURATION, "P" + 3600*2 + "S");
        excepEventId2 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        // Advance the end time of the 3rd instance, and change the self-attendee status.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DURATION, "P" + 3600*3 + "S");
        excepValues.put(Events.SELF_ATTENDEE_STATUS, Attendees.ATTENDEE_STATUS_DECLINED);
        excepEventId3 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        // Advance the start time of the 4th instance, which will also advance the end time.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DTSTART, startMillis + 3600*1000);
        excepEventId4 = createAndVerifyException(account, eventId, excepValues, true);
        instances.moveToNext();

        instances.close();

        // TODO: make sure the selfAttendeeStatus change took

        // Re-query the instances and figure out if they look right.
        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "with DURATION exceptions");
        }
        assertEquals("exceptional recurrence instance count", 5, instances.getCount());

        prevMinute = -1;
        while (instances.moveToNext()) {
            // expect the start times for each entry to be different from the previous entry
            long endMinute = instances.getLong(2);
            assertTrue("instance end times are different", endMinute != prevMinute);

            prevMinute = endMinute;
        }
        instances.close();

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests creation of a simple recurrence exception when not pretending to be the sync
     * adapter.  One significant consequence is that we don't set the _sync_id field in the
     * events, which affects how the provider correlates recurrences and exceptions.
     * <p>
     * NOTE: this test currently fails due to limitations in the provider.
     */
    @MediumTest //@Suppress
    public void testNonAdapterRecurrenceExceptions() {
        String account = "rena_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Generate recurring event, with "asSyncAdapter" set to false.
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, false, "1991-02-03T12:00:00", "PT1H", "FREQ=DAILY;WKST=SU;COUNT=10");

        // Select a period that gives us 3 instances.
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        String testStart = "1991-02-03T00:00:00";
        String testEnd = "1991-02-05T23:59:59";
        String[] projection = { Instances.BEGIN, Instances.START_MINUTE };

        // Expand the bounds of the instances table so we expand future events as they are added.
        expandInstanceRange(account, calendarId, testStart, testEnd, timeZone);

        // Create the event in the database.
        long eventId = createAndVerifyEvent(account, seed++, calendarId, false, eventValues);
        assertTrue(eventId >= 0);

        // Add some attendees.
        addAttendees(account, eventId, seed);

        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "initial");
        }
        assertEquals("initial recurrence instance count", 3, instances.getCount());

        /*
         * Alter the attendee status of the second event.  This should cause the instances to
         * be updated, replacing the previous 2nd instance with the exception instance.  If the
         * code is broken we'll see four instances (because the original instance didn't get
         * removed) or one instance (because the code correctly deleted all related events but
         * couldn't correlate the exception with its original recurrence).
         */

        // Leave first instance alone.
        instances.moveToPosition(1);

        long startMillis;
        ContentValues excepValues;

        // Advance the start time of the 2nd instance.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.SELF_ATTENDEE_STATUS, Attendees.ATTENDEE_STATUS_DECLINED);
        long excepEventId2 = createAndVerifyException(account, eventId, excepValues, false);
        instances.moveToNext();

        instances.close();

        // Re-query the instances and figure out if they look right.
        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "with exceptions");
        }
        assertEquals("exceptional recurrence instance count", 3, instances.getCount());

        instances.close();

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests creation of a simple recurrence exception where the exception is created first.
     * Simulates out-of-order delivery from the server.
     * <p>
     * We can't use the /exception URI, because that only works if the events are created
     * in order.
     */
    @MediumTest
    public void testOutOfOrderRecurrenceExceptions() {
        String account = "reooo_account";
        String startWhen = "1987-08-09T12:00:00";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Generate base event.
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed,
                calendarId, true, startWhen, "PT1H", "FREQ=DAILY;WKST=SU;COUNT=10");

        // Select a period that gives us 3 instances.
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        String testStart = "1987-08-09T00:00:00";
        String testEnd = "1987-08-11T23:59:59";
        String[] projection = { Instances.BEGIN, Instances.START_MINUTE, Instances.EVENT_ID };

        /*
         * We're interested in exploring what the instance expansion code does with the events
         * as they arrive.  It won't do anything at event-creation time unless the instance
         * range already covers the interesting set of dates, so we need to create and remove
         * an instance in the same time frame beforehand.
         */
        expandInstanceRange(account, calendarId, testStart, testEnd, timeZone);

        /*
         * Instances table should be expanded.  Do the test.
         */

        // Generate exception from base.  We shift the start time by half an hour.
        ContentValues excepValues = new ContentValues(eventValues);
        excepValues.remove(Events._SYNC_ID);
        excepValues.put(Events.ORIGINAL_SYNC_ID, eventValues.getAsString(Events._SYNC_ID));

        long dtstartMillis = excepValues.getAsLong(Events.DTSTART);
        dtstartMillis += 24 * 60 * 60 * 1000;       // add one day -- use the second instance
        excepValues.put(Events.ORIGINAL_INSTANCE_TIME, dtstartMillis);
        dtstartMillis += 1800 * 1000;
        excepValues.put(Events.DTSTART, dtstartMillis);
        excepValues.put(Events.DTEND, dtstartMillis + 3600*1000);
        excepValues.remove(Events.DURATION);
        excepValues.remove(Events.RRULE);

        // Create exception event.
        long excepId = createAndVerifyEvent(account, seed, calendarId, true, excepValues);
        assertTrue(excepId >= 0);

        // Create recurring event.
        long eventId = createAndVerifyEvent(account, seed, calendarId, true, eventValues);
        assertTrue(eventId >= 0);

        // Check to see how many instances we get.  If the recurrence and the exception don't
        // get paired up correctly, we'll see one instance too many.
        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "with exception");
        }

        assertEquals("initial recurrence instance count", 3, instances.getCount());

        instances.close();

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests exceptions that modify all future instances of a recurring event.
     */
    @MediumTest
    public void testForwardRecurrenceExceptions() {
        String account = "refx_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "1999-01-01T06:00:00", "PT1H", "FREQ=WEEKLY;WKST=SU;COUNT=10");
        long eventId = createAndVerifyEvent(account, seed++, calendarId, true, eventValues);

        // Add some attendees and reminders.
        addAttendees(account, eventId, seed++);
        addReminders(account, eventId, seed++);

        // Get some instances.
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        String testStart = "1999-01-01T00:00:00";
        String testEnd = "1999-01-29T23:59:59";
        String[] projection = { Instances.BEGIN, Instances.START_MINUTE };

        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "initial");
        }

        assertEquals("initial recurrence instance count", 5, instances.getCount());

        // Modify starting from 3rd instance.
        instances.moveToPosition(2);

        long startMillis;
        ContentValues excepValues;

        // Replace with a new recurrence rule.  We move the start time an hour later, and cap
        // it at two instances.
        startMillis = instances.getLong(0);
        excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.DTSTART, startMillis + 3600*1000);
        excepValues.put(Events.RRULE, "FREQ=WEEKLY;COUNT=2;WKST=SU");
        long excepEventId = createAndVerifyException(account, eventId, excepValues, true);
        instances.close();


        // Check to see if it took.
        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "with new rule");
        }

        assertEquals("count with exception", 4, instances.getCount());

        long prevMinute = -1;
        for (int i = 0; i < 4; i++) {
            long startMinute;
            instances.moveToNext();
            switch (i) {
                case 0:
                    startMinute = instances.getLong(1);
                    break;
                case 1:
                case 3:
                    startMinute = instances.getLong(1);
                    assertEquals("first/last pairs match", prevMinute, startMinute);
                    break;
                case 2:
                    startMinute = instances.getLong(1);
                    assertFalse("first two != last two", prevMinute == startMinute);
                    break;
                default:
                    fail();
                    startMinute = -1;   // make compiler happy
                    break;
            }

            prevMinute = startMinute;
        }
        instances.close();

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Tests exceptions that modify all instances of a recurring event.  This is not really an
     * exception, since it won't create a new event, but supporting it allows us to use the
     * exception URI without having to determine whether the "start from here" instance is the
     * very first instance.
     */
    @MediumTest
    public void testFullRecurrenceUpdate() {
        String account = "ref_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event
        String rrule = "FREQ=DAILY;WKST=MO;COUNT=100";
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "1997-08-29T02:14:00", "PT1H", rrule);
        long eventId = createAndVerifyEvent(account, seed++, calendarId, true, eventValues);
        //Log.i(TAG, "+++ eventId is " + eventId);

        // Get some instances.
        String timeZone = eventValues.getAsString(Events.EVENT_TIMEZONE);
        String testStart = "1997-08-01T00:00:00";
        String testEnd = "1997-08-31T23:59:59";
        String[] projection = { Instances.BEGIN, Instances.EVENT_LOCATION };
        String newLocation = "NEW!";

        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "initial");
        }

        assertEquals("initial recurrence instance count", 3, instances.getCount());

        instances.moveToFirst();
        long startMillis = instances.getLong(0);
        ContentValues excepValues = EventHelper.getNewExceptionValues(startMillis);
        excepValues.put(Events.RRULE, rrule);   // identifies this as an "all future events" excep
        excepValues.put(Events.EVENT_LOCATION, newLocation);
        long excepEventId = createAndVerifyException(account, eventId, excepValues, true);
        instances.close();

        // Check results.
        assertEquals("full update does not create new ID", eventId, excepEventId);

        instances = getInstances(timeZone, testStart, testEnd, projection);
        assertEquals("post-update instance count", 3, instances.getCount());
        while (instances.moveToNext()) {
            assertEquals("new location", newLocation, instances.getString(1));
        }

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    @MediumTest
    public void testMultiRuleRecurrence() {
        String account = "multirule_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event
        String rrule = "FREQ=DAILY;WKST=MO;COUNT=5\nFREQ=WEEKLY;WKST=SU;COUNT=5";
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "1997-08-29T02:14:00", "PT1H", rrule);
        long eventId = createAndVerifyEvent(account, seed++, calendarId, true, eventValues);

        // TODO: once multi-rule RRULEs are fully supported, verify that they work

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Issue bad requests and expect them to get rejected.
     */
    @MediumTest
    public void testBadRequests() {
        String account = "neg_account";
        int seed = 0;

        // Clean up just in case
        CalendarHelper.deleteCalendarByAccount(mContentResolver, account);

        // Create calendar
        long calendarId = createAndVerifyCalendar(account, seed++, null);

        // Create recurring event
        String rrule = "FREQ=OFTEN;WKST=MO";
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed++,
                calendarId, true, "1997-08-29T02:14:00", "PT1H", rrule);
        try {
            createAndVerifyEvent(account, seed++, calendarId, true, eventValues);
            fail("Bad recurrence rule should have been rejected");
        } catch (IllegalArgumentException iae) {
            // good
        }

        // delete the calendar
        removeAndVerifyCalendar(account, calendarId);
    }

    /**
     * Acquires the set of instances that appear between the specified start and end points.
     *
     * @param timeZone Time zone to use when parsing startWhen and endWhen
     * @param startWhen Start date/time, in RFC 3339 format
     * @param endWhen End date/time, in RFC 3339 format
     * @param projection Array of desired column names
     * @return Cursor with instances (caller should close when done)
     */
    private Cursor getInstances(String timeZone, String startWhen, String endWhen,
            String[] projection) {
        Time startTime = new Time(timeZone);
        startTime.parse3339(startWhen);
        long startMillis = startTime.toMillis(false);

        Time endTime = new Time(timeZone);
        endTime.parse3339(endWhen);
        long endMillis = endTime.toMillis(false);

        // We want a list of instances that occur between the specified dates.
        Uri uri = Uri.withAppendedPath(CalendarContract.Instances.CONTENT_URI,
                startMillis + "/" + endMillis);

        Cursor instances = mContentResolver.query(uri, projection, null, null,
                projection[0] + " ASC");

        return instances;
    }

    /** debug -- dump instances cursor */
    private static void dumpInstances(Cursor instances, String timeZone, String msg) {
        Log.d(TAG, "Instances (" + msg + ")");

        int posn = instances.getPosition();
        instances.moveToPosition(-1);

        //Log.d(TAG, "+++ instances has " + instances.getCount() + " rows, " +
        //        instances.getColumnCount() + " columns");
        while (instances.moveToNext()) {
            long beginMil = instances.getLong(0);
            Time beginT = new Time(timeZone);
            beginT.set(beginMil);
            String logMsg = "--> begin=" + beginT.format3339(false) + " (" + beginMil + ")";
            for (int i = 2; i < instances.getColumnCount(); i++) {
                logMsg += " [" + instances.getString(i) + "]";
            }
            Log.d(TAG, logMsg);
        }
        instances.moveToPosition(posn);
    }


    /**
     * Counts the number of instances that appear between the specified start and end times.
     */
    private int getInstanceCount(String timeZone, String startWhen, String endWhen) {
        Cursor instances = getInstances(timeZone, startWhen, endWhen,
                new String[] { Instances._ID });
        int count = instances.getCount();
        instances.close();
        return count;
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
     * @param asSyncAdapter
     * @param values optional pre created set of values; will have several new entries added
     * @return the _id for the new event
     */
    private long createAndVerifyEvent(String account, int seed, long calendarId,
            boolean asSyncAdapter, ContentValues values) {
        // Create an event
        if (values == null) {
            values = EventHelper.getNewEventValues(account, seed, calendarId, asSyncAdapter);
        }
        Uri insertUri = Events.CONTENT_URI;
        if (asSyncAdapter) {
            insertUri = asSyncAdapter(insertUri, account, CTS_TEST_TYPE);
        }
        Uri uri = mContentResolver.insert(insertUri, values);

        // Verify
        EventHelper.addDefaultReadOnlyValues(values, account, asSyncAdapter);
        long eventId = ContentUris.parseId(uri);
        assertTrue(eventId >= 0);

        verifyEvent(values, eventId);
        return eventId;
    }

    /**
     * Updates an event, and verifies that the updates took.
     */
    private void updateAndVerifyEvent(String account, long calendarId, long eventId,
            boolean asSyncAdapter, ContentValues updateValues) {
        Uri uri = Uri.withAppendedPath(Events.CONTENT_URI, String.valueOf(eventId));
        if (asSyncAdapter) {
            uri = asSyncAdapter(uri, account, CTS_TEST_TYPE);
        }
        int count = mContentResolver.update(uri, updateValues, null, null);

        // Verify
        assertEquals(1, count);
        verifyEvent(updateValues, eventId);
    }

    /**
     * Creates an exception to a recurring event, and verifies it.
     * @param account The account to use.
     * @param originalEventId The ID of the original event.
     * @param values Values for the exception; must include originalInstanceTime.
     * @return The _id for the new event.
     */
    private long createAndVerifyException(String account, long originalEventId,
            ContentValues values, boolean asSyncAdapter) {
        // Create the exception
        Uri uri = Uri.withAppendedPath(Events.CONTENT_EXCEPTION_URI,
                String.valueOf(originalEventId));
        if (asSyncAdapter) {
            uri = asSyncAdapter(uri, account, CTS_TEST_TYPE);
        }
        Uri resultUri = mContentResolver.insert(uri, values);
        assertNotNull(resultUri);
        long eventId = ContentUris.parseId(resultUri);
        assertTrue(eventId >= 0);
        return eventId;
    }

    /**
     * Deletes an exception to a recurring event.
     * @param account The account to use.
     * @param eventId The ID of the original recurring event.
     * @param excepId The ID of the exception event.
     * @return The number of rows deleted.
     */
    private int deleteException(String account, long eventId, long excepId) {
        Uri uri = Uri.withAppendedPath(Events.CONTENT_EXCEPTION_URI,
                eventId + "/" + excepId);
        uri = asSyncAdapter(uri, account, CTS_TEST_TYPE);
        return mContentResolver.delete(uri, null, null);
    }

    /**
     * Add some attendees to an event.
     */
    private void addAttendees(String account, long eventId, int seed) {

        assertTrue(eventId >= 0);
        Uri syncUri = asSyncAdapter(Attendees.CONTENT_URI, account, CTS_TEST_TYPE);

        ContentValues values = new ContentValues();
        values.put(Attendees.EVENT_ID, eventId);
        values.put(Attendees.ATTENDEE_NAME, "Attender" + seed);
        values.put(Attendees.ATTENDEE_EMAIL, CalendarHelper.generateCalendarOwnerEmail(account));
        values.put(Attendees.ATTENDEE_STATUS, Attendees.ATTENDEE_STATUS_ACCEPTED);
        values.put(Attendees.ATTENDEE_RELATIONSHIP, Attendees.RELATIONSHIP_ORGANIZER);
        values.put(Attendees.ATTENDEE_TYPE, Attendees.TYPE_NONE);
        Uri uri = mContentResolver.insert(syncUri, values);
        seed++;

        values = new ContentValues();
        values.put(Attendees.EVENT_ID, eventId);
        values.put(Attendees.ATTENDEE_NAME, "Attender" + seed);
        values.put(Attendees.ATTENDEE_EMAIL, "attender" + seed + "@example.com");
        values.put(Attendees.ATTENDEE_STATUS, Attendees.ATTENDEE_STATUS_TENTATIVE);
        values.put(Attendees.ATTENDEE_RELATIONSHIP, Attendees.RELATIONSHIP_NONE);
        values.put(Attendees.ATTENDEE_TYPE, Attendees.TYPE_NONE);
        uri = mContentResolver.insert(syncUri, values);
    }

    /**
     * Add some reminders to an event.
     */
    private void addReminders(String account, long eventId, int seed) {
        ContentValues values = new ContentValues();

        values.put(Reminders.EVENT_ID, eventId);
        values.put(Reminders.MINUTES, seed * 5);
        values.put(Reminders.METHOD, Reminders.METHOD_ALERT);

        Uri syncUri = asSyncAdapter(Reminders.CONTENT_URI, account, CTS_TEST_TYPE);
        Uri uri = mContentResolver.insert(syncUri, values);
    }

    /**
     * Creates and removes an event that covers a specific range of dates.  Call this to
     * cause the provider to expand the CalendarMetaData min/max values to include the range.
     * Useful when you want to see the provider expand the instances as the events are added.
     */
    private void expandInstanceRange(String account, long calendarId, String testStart,
            String testEnd, String timeZone) {
        int seed = 0;

        // TODO: this should use an UNTIL rule based on testEnd, not a COUNT
        ContentValues eventValues = EventHelper.getNewRecurringEventValues(account, seed,
                calendarId, true, testStart, "PT1H", "FREQ=DAILY;WKST=SU;COUNT=100");

        /*
         * Some of the helper functions modify "eventValues", so we want to make sure we're
         * passing a copy of anything we want to re-use.
         */
        long eventId = createAndVerifyEvent(account, seed, calendarId, true,
                new ContentValues(eventValues));
        assertTrue(eventId >= 0);

        String[] projection = { Instances.BEGIN, Instances.START_MINUTE };
        Cursor instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "prep-create");
        }
        assertEquals("initial recurrence instance count", 3, instances.getCount());
        instances.close();

        Uri eventUri = ContentUris.withAppendedId(Events.CONTENT_URI, eventId);
        removeAndVerifyEvent(eventUri, new ContentValues(eventValues), account);

        instances = getInstances(timeZone, testStart, testEnd, projection);
        if (DEBUG_RECURRENCE) {
            dumpInstances(instances, timeZone, "prep-clear");
        }
        assertEquals("initial recurrence instance count", 0, instances.getCount());
        instances.close();

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
