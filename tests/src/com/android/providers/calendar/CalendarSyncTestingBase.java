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

import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.text.format.DateUtils;
import android.text.format.Time;
import android.provider.Calendar;
import android.test.SyncBaseInstrumentation;
import android.util.Log;
import com.google.android.collect.Maps;
import com.google.android.googlelogin.GoogleLoginServiceBlockingHelper;
import com.google.android.googlelogin.GoogleLoginServiceNotFoundException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Calendar Sync base tests. Testing creation of setup, syncing calendar, inserting, 
 * deleting and editing events.
 */
public class CalendarSyncTestingBase extends SyncBaseInstrumentation {
    protected Context mTargetContext;
    protected String mAccount;
    protected ContentResolver mResolver;
    protected Uri mEventsUri = Uri.parse("content://calendar/events");

    static final String TAG = "calendar";
    static final String DEFAULT_TIMEZONE = "America/Los_Angeles";
    static final Set<String> EVENT_COLUMNS_TO_SKIP = new HashSet<String>();
    static final Set<String> ATTENDEES_COLUMNS_TO_SKIP = new HashSet<String>();
    static final Set<String> CALENDARS_COLUMNS_TO_SKIP = new HashSet<String>();
    static final Set<String> INSTANCES_COLUMNS_TO_SKIP = new HashSet<String>();

    static {
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._ID);
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._SYNC_TIME);
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._SYNC_VERSION);
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._SYNC_LOCAL_ID);
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._SYNC_DIRTY);
        EVENT_COLUMNS_TO_SKIP.add(Calendar.Events._SYNC_MARK);
        ATTENDEES_COLUMNS_TO_SKIP.add(Calendar.Attendees._ID);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._ID);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._SYNC_TIME);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._SYNC_VERSION);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._SYNC_LOCAL_ID);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._SYNC_DIRTY);
        CALENDARS_COLUMNS_TO_SKIP.add(Calendar.Calendars._SYNC_MARK);
        INSTANCES_COLUMNS_TO_SKIP.add(Calendar.Instances._ID);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        mTargetContext = getInstrumentation().getTargetContext();
        mAccount = getAccount();
        mResolver = mTargetContext.getContentResolver();
    }

    /**
     * A simple method that syncs the calendar provider.
     * @throws Exception
     */
    protected void syncCalendar() throws Exception {
       
        cancelSyncsandDisableAutoSync();
        syncProvider(Calendar.CONTENT_URI, mAccount, Calendar.AUTHORITY);
    }

    /**
     * Creates a new event in the default calendar.
     * @param event Event to be created.
     * @return Uri of the created event.
     * @throws Exception
     */
    protected Uri insertEvent(EventInfo event) throws Exception {
        Log.i(TAG, "Get Dafault Calender Id"); 
        return insertEvent(getDefaultCalendarId(), event);
    }

    /**
     * Creates a new event in the given calendarId.
     * @param calendarId Calendar to be used.
     * @param event Event to be created.
     * @return Uri of the event created.
     * @throws Exception
     */
    protected Uri insertEvent(int calendarId, EventInfo event) throws Exception{
        ContentValues m = new ContentValues();
        m.put(Calendar.Events.CALENDAR_ID, calendarId);
        m.put(Calendar.Events.TITLE, event.mTitle);
        m.put(Calendar.Events.DTSTART, event.mDtstart);
        m.put(Calendar.Events.ALL_DAY, event.mAllDay ? 1 : 0);

        if (event.mRrule == null) {
            // This is a normal event
            m.put(Calendar.Events.DTEND, event.mDtend);
        } else {
            // This is a repeating event
            m.put(Calendar.Events.RRULE, event.mRrule);
            m.put(Calendar.Events.DURATION, event.mDuration);
        }

        if (event.mDescription != null) {
            m.put(Calendar.Events.DESCRIPTION, event.mDescription);
        }
        if (event.mTimezone != null) {
            m.put(Calendar.Events.EVENT_TIMEZONE, event.mTimezone);
        }

        Log.v(TAG, "Test Calendar Event");
        Uri url = mResolver.insert(mEventsUri, m);
        Log.v(TAG, "Insert Calendar Event");
        syncCalendar();
        Log.v(TAG, "Test Sync Calendar");
        return url;
    }

    /**
     * Edits the given event.
     * @param eventId EventID of the event to be edited.
     * @param event Edited event details.
     * @throws Exception
     */
    protected void editEvent(long eventId, EventInfo event) throws Exception {
        ContentValues values = new ContentValues();
        values.put(Calendar.Events.TITLE, event.mTitle);
        values.put(Calendar.Events.DTSTART, event.mDtstart);
        values.put(Calendar.Events.DTEND, event.mDtend);
        values.put(Calendar.Events.ALL_DAY, event.mAllDay ? 1 : 0);

        if (event.mDescription != null) {
            values.put(Calendar.Events.DESCRIPTION, event.mDescription);
        } else if(event.mLocation != null) {
            values.put(Calendar.Events.EVENT_LOCATION, event.mLocation);
        } else if(event.mTitle != null) {
            values.put(Calendar.Events.TITLE, event.mTitle);
        } 
        else {
            throw new RuntimeException("Unsupported: description or location must be specified");
        }

        Uri uri = ContentUris.withAppendedId(Calendar.Events.CONTENT_URI, eventId);
        mResolver.update(uri, values, null, null);
        syncCalendar();
    }

    /**
     * Deletes a given event.
     * @param uri
     * @throws Exception
     */
    protected void deleteEvent(Uri uri) throws Exception {
        mResolver.delete(uri, null, null);
        syncCalendar();
    }

    /**
     * Inserts a new calendar.
     * @param name
     * @param timezone
     * @param calendarUrl
     * @throws Exception
     */
    protected void insertCalendar(String name, String timezone, String calendarUrl)
            throws Exception {
        ContentValues values = new ContentValues();

        values.put(Calendar.Calendars._SYNC_ACCOUNT, getAccount());
        values.put(Calendar.Calendars.URL, calendarUrl);
        values.put(Calendar.Calendars.NAME, name);
        values.put(Calendar.Calendars.DISPLAY_NAME, name);
        values.put(Calendar.Calendars.SYNC_EVENTS, 1);
        values.put(Calendar.Calendars.SELECTED, 1);
        values.put(Calendar.Calendars.HIDDEN, 0);
        values.put(Calendar.Calendars.COLOR, "0xff123456" /* blue */);
        values.put(Calendar.Calendars.ACCESS_LEVEL, Calendar.Calendars.OWNER_ACCESS);
        values.put(Calendar.Calendars.TIMEZONE, timezone);
        mResolver.insert(Calendar.Calendars.CONTENT_URI, values);
        syncCalendar();
    }

    /**
     * Returns a fresh count of events.
     * @return
     */
    protected int getEventsCount() {
        Cursor cursor;
        cursor = mResolver.query(mEventsUri, null, null, null, null);
        return cursor.getCount();
    }

    /**
     * Returns the ID of the default calendar.
     * @return
     */
    protected int getDefaultCalendarId() {
        Cursor calendarsCursor;
        calendarsCursor = mResolver.query(Calendar.Calendars.CONTENT_URI, null, null, null, null);
        calendarsCursor.moveToNext();
        return calendarsCursor.getInt(calendarsCursor.getColumnIndex("_id"));
    }

    /**
     * This class stores all the useful information about an event.
     */
    protected class EventInfo {
        String mTitle;
        String mDescription;
        String mLocation;
        String mTimezone;
        boolean mAllDay;
        long mDtstart;
        long mDtend;
        String mRrule;
        String mDuration;
        String mOriginalTitle;
        long mOriginalInstance;
        int mSyncId;

        // Constructor for normal events, using the default timezone
        public EventInfo(String title, String startDate, String endDate,
                boolean allDay) {
            init(title, startDate, endDate, allDay, DEFAULT_TIMEZONE);
        }

        public EventInfo(String title, long startDate, long endDate,
                boolean allDay) {
            mTitle = title;
            mTimezone = DEFAULT_TIMEZONE;
            mDtstart = startDate;
            mDtend = endDate;
            mDuration = null;
            mRrule = null;
            mAllDay = allDay;
        }

        public EventInfo(String title, long startDate, long endDate,
                boolean allDay, String description) {
            mTitle = title;
            mTimezone = DEFAULT_TIMEZONE;
            mDtstart = startDate;
            mDtend = endDate;
            mDuration = null;
            mRrule = null;
            mAllDay = allDay;
            mDescription = description;
        }
        

        // Constructor for normal events, specifying the timezone
        public EventInfo(String title, String startDate, String endDate,
                boolean allDay, String timezone) {
            init(title, startDate, endDate, allDay, timezone);
        }

        public void init(String title, String startDate, String endDate,
                boolean allDay, String timezone) {
            mTitle = title;
            Time time = new Time();
            if (allDay) {
                time.timezone = Time.TIMEZONE_UTC;
            } else if (timezone != null) {
                time.timezone = timezone;
            }
            mTimezone = time.timezone;
            time.parse3339(startDate);
            mDtstart = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            mDtend = time.toMillis(false /* use isDst */);
            mDuration = null;
            mRrule = null;
            mAllDay = allDay;
        }

        /**
         * Constructor for repeating events, using the default time zone.
         * @param title
         * @param description
         * @param startDate
         * @param endDate
         * @param rrule
         * @param allDay
         */
        public EventInfo(String title, String description, String startDate, String endDate,
                String rrule, boolean allDay) {
            init(title, description, startDate, endDate, rrule, allDay, DEFAULT_TIMEZONE);
        }

        /**
         * Constructor for repeating events, using the specific time zone.
         * @param title
         * @param description
         * @param startDate
         * @param endDate
         * @param rrule
         * @param allDay
         * @param timezone
         */
        public EventInfo(String title, String description, String startDate, String endDate,
                String rrule, boolean allDay, String timezone) {
            init(title, description, startDate, endDate, rrule, allDay, timezone);
        }

        public void init(String title, String description, String startDate, String endDate,
                String rrule, boolean allDay, String timezone) {
            mTitle = title;
            mDescription = description;
            Time time = new Time();
            if (allDay) {
                time.timezone = Time.TIMEZONE_UTC;
            } else if (timezone != null) {
                time.timezone = timezone;
            }
            mTimezone = time.timezone;
            time.parse3339(startDate);
            mDtstart = time.toMillis(false /* use isDst */);
            if (endDate != null) {
                time.parse3339(endDate);
                mDtend = time.toMillis(false /* use isDst */);
            }
            if (allDay) {
                long days = 1;
                if (endDate != null) {
                    days = (mDtend - mDtstart) / DateUtils.DAY_IN_MILLIS;
                }
                mDuration = "P" + days + "D";
            } else {
                long seconds = (mDtend - mDtstart) / DateUtils.SECOND_IN_MILLIS;
                mDuration = "P" + seconds + "S";
            }
            mRrule = rrule;
            mAllDay = allDay;
        }

        // Constructor for recurrence exceptions, using the default timezone
        public EventInfo(String originalTitle, String originalInstance, String title,
                String description, String startDate, String endDate, boolean allDay) {
            init(originalTitle, originalInstance,
                    title, description, startDate, endDate, allDay, DEFAULT_TIMEZONE);
        }

        public void init(String originalTitle, String originalInstance,
                String title, String description, String startDate, String endDate,
                boolean allDay, String timezone) {
            mOriginalTitle = originalTitle;
            Time time = new Time(timezone);
            time.parse3339(originalInstance);
            mOriginalInstance = time.toMillis(false /* use isDst */);
            init(title, description, startDate, endDate, null /* rrule */, allDay, timezone);
        }
    }

    /**
     * Returns the default account on the device.
     * @return
     */
    protected String getAccount() {
        try {
            return GoogleLoginServiceBlockingHelper.getAccount(mTargetContext, false);
        } catch (GoogleLoginServiceNotFoundException e) {
            Log.e("SyncCalendarTest", "Could not find Google login service", e);
            return null;
        }
    }

    /**
     * Compares two cursors
     */
    protected void compareCursors(Cursor cursor1, Cursor cursor2,
                                  Set<String> columnsToSkip, String tableName) {
        String[] cols = cursor1.getColumnNames();
        int length = cols.length;

        assertEquals(tableName + " count failed to match", cursor1.getCount(),
                cursor2.getCount());
        Map<String, String> row = Maps.newHashMap();
        while (cursor1.moveToNext() && cursor2.moveToNext()) {
            for (int i = 0; i < length; i++) {
                String col = cols[i];
                if (columnsToSkip != null && columnsToSkip.contains(col)) {
                    continue;
                }
                row.put(col, cursor1.getString(i));

                assertEquals("Row: " + row + " Table: " + tableName + ": " + cols[i] +
                        " failed to match", cursor1.getString(i),
                        cursor2.getString(i));
            }
        }
    }    
}
