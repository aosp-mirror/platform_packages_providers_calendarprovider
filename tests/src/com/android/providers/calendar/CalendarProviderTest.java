/*
 * Copyright (C) 2008 The Android Open Source Project
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

import com.android.internal.database.ArrayListCursor;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.text.format.DateUtils;
import android.text.format.Time;
import android.provider.Calendar;
import android.provider.Calendar.BusyBits;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Events;
import android.provider.Calendar.Instances;
import android.test.ProviderTestCase2;
import android.test.mock.MockContentResolver;
import android.test.suitebuilder.annotation.LargeTest;
import android.util.Log;

import java.util.ArrayList;


/**
 * Runs various tests on an isolated Calendar provider with its own database.
 */
@LargeTest
public class CalendarProviderTest extends ProviderTestCase2<CalendarProvider> {
    static final String TAG = "calendar";
    static final String DEFAULT_TIMEZONE = "America/Los_Angeles";

    private SQLiteDatabase mDb;
    private MetaData mMetaData;
    private Context mContext;
    private MockContentResolver mResolver;
    private Uri mEventsUri = Uri.parse("content://calendar/events");
    private int mCalendarId;
    
    // We need a unique id to put in the _sync_id field so that we can create
    // recurrence exceptions that refer to recurring events.
    private int mGlobalSyncId = 1;
    
    /**
     * KeyValue is a simple class that stores a pair of strings representing
     * a (key, value) pair.  This is used for updating events.
     */
    private class KeyValue {
        String key;
        String value;
        
        public KeyValue(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * A generic command interface.  This is used to support a sequence of
     * commands that can create events, delete or update events, and then
     * check that the state of the database is as expected.
     */
    private interface Command {
        public void execute();
    }
    
    /**
     * This is used to insert a new event into the database.  The event is
     * specified by its name (or "title").  All of the event fields (the
     * start and end time, whether it is an all-day event, and so on) are
     * stored in a separate table (the "mEvents" table).
     */
    private class Insert implements Command {
        EventInfo eventInfo;
        
        public Insert(String eventName) {
            eventInfo = findEvent(eventName);
        }
        
        public void execute() {
            Log.i(TAG, "insert " + eventInfo.mTitle);
            insertEvent(mCalendarId, eventInfo);
        }
    }
    
    /**
     * This is used to delete an event, specified by the event name.
     */
    private class Delete implements Command {
        String eventName;
        int expected;
        
        public Delete(String eventName, int expected) {
            this.eventName = eventName;
            this.expected = expected;
        }
        
        public void execute() {
            Log.i(TAG, "delete " + eventName);
            int rows = deleteMatchingEvents(eventName);
            assertEquals(expected, rows);
        }
    }
    
    /**
     * This is used to update an event.  The values to update are specified
     * with an array of (key, value) pairs.  Both the key and value are
     * specified as strings.  Event fields that are not really strings (such
     * as DTSTART which is a long) should be converted to the appropriate type
     * but that isn't supported yet.  When needed, that can be added here
     * by checking for specific keys and converting the associated values.
     */
    private class Update implements Command {
        String eventName;
        KeyValue[] pairs;
        
        public Update(String eventName, KeyValue[] pairs) {
            this.eventName = eventName;
            this.pairs = pairs;
        }
        
        public void execute() {
            Log.i(TAG, "update " + eventName);
            ContentValues map = new ContentValues();
            for (KeyValue pair : pairs) {
                String value = pair.value;
                map.put(pair.key, value);
            }
            updateMatchingEvents(eventName, map);
        }
    }
    
    /**
     * This command queries the number of events and compares it to the given
     * expected value.
     */
    private class QueryNumEvents implements Command {
        int expected;
        
        public QueryNumEvents(int expected) {
            this.expected = expected;
        }
        
        public void execute() {
            Cursor cursor = mResolver.query(mEventsUri, null, null, null, null);
            assertEquals(expected, cursor.getCount());
            cursor.close();
        }
    }
    
    
    /**
     * This command dumps the list of events to the log for debugging.
     */
    private class DumpEvents implements Command {
        
        public DumpEvents() {
        }
        
        public void execute() {
            Cursor cursor = mResolver.query(mEventsUri, null, null, null, null);
            dumpCursor(cursor);
            cursor.close();
        }
    }
    
    /**
     * This command dumps the list of instances to the log for debugging.
     */
    private class DumpInstances implements Command {
        long begin;
        long end;
        
        public DumpInstances(String startDate, String endDate) {
            Time time = new Time(DEFAULT_TIMEZONE);
            time.parse3339(startDate);
            begin = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            end = time.toMillis(false /* use isDst */);
        }
        
        public void execute() {
            Cursor cursor = queryInstances(begin, end);
            dumpCursor(cursor);
            cursor.close();
        }
    }
    
    /**
     * This command queries the number of instances and compares it to the given
     * expected value.
     */
    private class QueryNumInstances implements Command {
        int expected;
        long begin;
        long end;
        
        public QueryNumInstances(String startDate, String endDate, int expected) {
            Time time = new Time(DEFAULT_TIMEZONE);
            time.parse3339(startDate);
            begin = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            end = time.toMillis(false /* use isDst */);
            this.expected = expected;
        }
        
        public void execute() {
            Cursor cursor = queryInstances(begin, end);
            assertEquals(expected, cursor.getCount());
            cursor.close();
        }
    }
    
    /**
     * When this command runs it verifies that all of the instances in the
     * given range match the expected instances (each instance is specified by
     * a start date).
     * If you just want to verify that an instance exists in a given date
     * range, use {@link VerifyInstance} instead.
     */
    private class VerifyAllInstances implements Command {
        long[] instances;
        long begin;
        long end;
        
        public VerifyAllInstances(String startDate, String endDate, String[] dates) {
            Time time = new Time(DEFAULT_TIMEZONE);
            time.parse3339(startDate);
            begin = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            end = time.toMillis(false /* use isDst */);
            
            if (dates == null) {
                return;
            }
            
            // Convert all the instance date strings to UTC milliseconds
            int len = dates.length;
            this.instances = new long[len];
            int index = 0;
            for (String instance : dates) {
                time.parse3339(instance);
                this.instances[index++] = time.toMillis(false /* use isDst */);
            }
        }
        
        public void execute() {
            Cursor cursor = queryInstances(begin, end);
            int len = 0;
            if (instances != null) {
                len = instances.length;
            }
            assertEquals("number of instances don't match", len, cursor.getCount());
            
            if (instances == null) {
                return;
            }

            int beginColumn = cursor.getColumnIndex(Instances.BEGIN);
            while (cursor.moveToNext()) {
                long begin = cursor.getLong(beginColumn);
                
                // Search the list of expected instances for a matching start
                // time.
                boolean found = false;
                for (long instance : instances) {
                    if (instance == begin) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    int titleColumn = cursor.getColumnIndex(Events.TITLE);
                    int allDayColumn = cursor.getColumnIndex(Events.ALL_DAY);

                    String title = cursor.getString(titleColumn);
                    boolean allDay = cursor.getInt(allDayColumn) != 0;
                    int flags = DateUtils.FORMAT_SHOW_DATE | DateUtils.FORMAT_NUMERIC_DATE |
                            DateUtils.FORMAT_24HOUR;
                    if (allDay) {
                        flags |= DateUtils.FORMAT_UTC;
                    } else {
                        flags |= DateUtils.FORMAT_SHOW_TIME;
                    }
                    String date = DateUtils.formatDateRange(mContext, begin, begin, flags);
                    String mesg = String.format("Test failed!"
                            + " unexpected instance (\"%s\") at %s",
                            title, date);
                    Log.e(TAG, mesg);
                }
                assertTrue(found);
            }
            cursor.close();
        }
    }

    /**
     * When this command runs it verifies that the given instance exists in
     * the given date range. 
     */
    private class VerifyInstance implements Command {
        long instance;
        boolean allDay;
        long begin;
        long end;
        
        /**
         * Creates a command to check that the given range [startDate,endDate]
         * contains a specific instance of an event (specified by "date").
         * 
         * @param startDate the beginning of the date range
         * @param endDate the end of the date range
         * @param date the date or date-time string of an event instance
         */
        public VerifyInstance(String startDate, String endDate, String date) {
            Time time = new Time(DEFAULT_TIMEZONE);
            time.parse3339(startDate);
            begin = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            end = time.toMillis(false /* use isDst */);
            
            // Convert the instance date string to UTC milliseconds
            time.parse3339(date);
            allDay = time.allDay;
            instance = time.toMillis(false /* use isDst */);
        }
        
        public void execute() {
            Cursor cursor = queryInstances(begin, end);
            int beginColumn = cursor.getColumnIndex(Instances.BEGIN);
            boolean found = false;
            while (cursor.moveToNext()) {
                long begin = cursor.getLong(beginColumn);
                
                if (instance == begin) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                int flags = DateUtils.FORMAT_SHOW_DATE | DateUtils.FORMAT_NUMERIC_DATE;
                if (allDay) {
                    flags |= DateUtils.FORMAT_UTC;
                } else {
                    flags |= DateUtils.FORMAT_SHOW_TIME;
                }
                String date = DateUtils.formatDateRange(mContext, instance, instance, flags);
                String mesg = String.format("Test failed!"
                        + " cannot find instance at %s",
                        date);
                Log.e(TAG, mesg);
            }
            assertTrue(found);
            cursor.close();
        }
    }
    
    /**
     * This class stores all the useful information about an event.
     */
    private class EventInfo {
        String mTitle;
        String mDescription;
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
        
        // Constructor for repeating events, using the default timezone
        public EventInfo(String title, String description, String startDate, String endDate,
                String rrule, boolean allDay) {
            init(title, description, startDate, endDate, rrule, allDay, DEFAULT_TIMEZONE);
        }
        
        // Constructor for repeating events, specifying the timezone
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
    
    private class InstanceInfo {
        EventInfo mEvent;
        long mBegin;
        long mEnd;
        int mExpectedOccurrences;
        
        public InstanceInfo(String eventName, String startDate, String endDate, int expected) {
            // Find the test index that contains the given event name
            mEvent = findEvent(eventName);
            Time time = new Time(mEvent.mTimezone);
            time.parse3339(startDate);
            mBegin = time.toMillis(false /* use isDst */);
            time.parse3339(endDate);
            mEnd = time.toMillis(false /* use isDst */);
            mExpectedOccurrences = expected;
        }
    }
    
    private class BusyBitInfo {
        EventInfo[] mEvents;
        int mStartDay;
        int mNumDays;
        int[] mBusyBits;
        int[] mAllDayCounts;
        
        public BusyBitInfo(EventInfo[] events, String startDate, int numDays,
                int[] busybits, int[] allDayCounts) {
            mEvents = events;
            Time time = new Time(DEFAULT_TIMEZONE);
            time.parse3339(startDate);
            long millis = time.toMillis(true /* ignore isDst */);
            mStartDay = Time.getJulianDay(millis, time.gmtoff);
            mNumDays = numDays;
            mBusyBits = busybits;
            mAllDayCounts = allDayCounts;
        }
    }
    
    /**
     * This is the main table of events.  The events in this table are
     * referred to by name in other places.
     */
    private EventInfo[] mEvents = {
            new EventInfo("normal0", "2008-05-01T00:00:00", "2008-05-02T00:00:00", false),
            new EventInfo("normal1", "2008-05-26T08:30:00", "2008-05-26T09:30:00", false),
            new EventInfo("normal2", "2008-05-26T14:30:00", "2008-05-26T15:30:00", false),
            new EventInfo("allday0", "2008-05-02T00:00:00", "2008-05-03T00:00:00", true),
            new EventInfo("allday1", "2008-05-02T00:00:00", "2008-05-31T00:00:00", true),
            new EventInfo("daily0", "daily from 5/1/2008 12am to 1am",
                    "2008-05-01T00:00:00", "2008-05-01T01:00:00",
                    "FREQ=DAILY;WKST=SU", false),
            new EventInfo("daily1", "daily from 5/1/2008 8:30am to 9:30am until 5/3/2008 8am",
                    "2008-05-01T08:30:00", "2008-05-01T09:30:00",
                    "FREQ=DAILY;UNTIL=20080503T150000Z;WKST=SU", false),
            new EventInfo("daily2", "daily from 5/1/2008 8:45am to 9:15am until 5/3/2008 10am",
                    "2008-05-01T08:45:00", "2008-05-01T09:15:00",
                    "FREQ=DAILY;UNTIL=20080503T170000Z;WKST=SU", false),
            new EventInfo("allday daily0", "all-day daily from 5/1/2008",
                    "2008-05-01", null,
                    "FREQ=DAILY;WKST=SU", true),
            new EventInfo("allday daily1", "all-day daily from 5/1/2008 until 5/3/2008",
                    "2008-05-01", null,
                    "FREQ=DAILY;UNTIL=20080503T000000Z;WKST=SU", true),
            new EventInfo("allday weekly0", "all-day weekly from 5/1/2008",
                    "2008-05-01", null,
                    "FREQ=WEEKLY;WKST=SU", true),
            new EventInfo("allday weekly1", "all-day for 2 days weekly from 5/1/2008",
                    "2008-05-01", "2008-05-03",
                    "FREQ=WEEKLY;WKST=SU", true),
            new EventInfo("allday yearly0", "all-day yearly on 5/1/2008",
                    "2008-05-01T", null,
                    "FREQ=YEARLY;WKST=SU", true),
            new EventInfo("weekly0", "weekly from 5/6/2008 on Tue 1pm to 2pm",
                    "2008-05-06T13:00:00", "2008-05-06T14:00:00",
                    "FREQ=WEEKLY;BYDAY=TU;WKST=MO", false),
            new EventInfo("weekly1", "every 2 weeks from 5/6/2008 on Tue from 2:30pm to 3:30pm",
                    "2008-05-06T14:30:00", "2008-05-06T15:30:00",
                    "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;WKST=MO", false),
            new EventInfo("monthly0", "monthly from 5/20/2008 on the 3rd Tues from 3pm to 4pm",
                    "2008-05-20T15:00:00", "2008-05-20T16:00:00",
                    "FREQ=MONTHLY;BYDAY=3TU;WKST=SU", false),
            new EventInfo("monthly1", "monthly from 5/1/2008 on the 1st from 12:00am to 12:10am",
                    "2008-05-01T00:00:00", "2008-05-01T00:10:00",
                    "FREQ=MONTHLY;WKST=SU;BYMONTHDAY=1", false),
            new EventInfo("monthly2", "monthly from 5/31/2008 on the 31st 11pm to midnight",
                    "2008-05-31T23:00:00", "2008-06-01T00:00:00",
                    "FREQ=MONTHLY;WKST=SU;BYMONTHDAY=31", false),
            new EventInfo("daily0", "2008-05-01T00:00:00",
                    "except0", "daily0 exception for 5/1/2008 12am, change to 5/1/2008 2am to 3am",
                    "2008-05-01T02:00:00", "2008-05-01T01:03:00", false),
            new EventInfo("daily0", "2008-05-03T00:00:00",
                    "except1", "daily0 exception for 5/3/2008 12am, change to 5/3/2008 2am to 3am",
                    "2008-05-03T02:00:00", "2008-05-03T01:03:00", false),
            new EventInfo("daily0", "2008-05-02T00:00:00",
                    "except2", "daily0 exception for 5/2/2008 12am, change to 1/2/2008",
                    "2008-01-02T00:00:00", "2008-01-02T01:00:00", false),
            new EventInfo("yearly0", "yearly on 5/1/2008 from 1pm to 2pm",
                    "2008-05-01T13:00:00", "2008-05-01T14:00:00",
                    "FREQ=YEARLY;WKST=SU", false),
    };

    /**
     * This table is used to create repeating events and then check that the
     * number of instances within a given range matches the expected number
     * of instances.
     */
    private InstanceInfo[] mInstanceRanges = {
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-05-01T00:01:00", 1),
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-05-01T01:00:00", 1),
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-05-02T00:00:00", 2),
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-05-02T23:59:00", 2),
            new InstanceInfo("daily0", "2008-05-02T00:00:00", "2008-05-02T00:01:00", 1),
            new InstanceInfo("daily0", "2008-05-02T00:00:00", "2008-05-02T01:00:00", 1),
            new InstanceInfo("daily0", "2008-05-02T00:00:00", "2008-05-03T00:00:00", 2),
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-05-31T23:59:00", 31),
            new InstanceInfo("daily0", "2008-05-01T00:00:00", "2008-06-01T23:59:00", 32),
            
            new InstanceInfo("daily1", "2008-05-01T00:00:00", "2008-05-02T00:00:00", 1),
            new InstanceInfo("daily1", "2008-05-01T00:00:00", "2008-05-31T23:59:00", 2),
            
            new InstanceInfo("daily2", "2008-05-01T00:00:00", "2008-05-02T00:00:00", 1),
            new InstanceInfo("daily2", "2008-05-01T00:00:00", "2008-05-31T23:59:00", 3),

            new InstanceInfo("allday daily0", "2008-05-01", "2008-05-07", 7),
            new InstanceInfo("allday daily1", "2008-05-01", "2008-05-07", 3),
            new InstanceInfo("allday weekly0", "2008-05-01", "2008-05-07", 1),
            new InstanceInfo("allday weekly0", "2008-05-01", "2008-05-08", 2),
            new InstanceInfo("allday weekly0", "2008-05-01", "2008-05-31", 5),
            new InstanceInfo("allday weekly1", "2008-05-01", "2008-05-31", 5),
            new InstanceInfo("allday yearly0", "2008-05-01", "2009-04-30", 1),
            new InstanceInfo("allday yearly0", "2008-05-01", "2009-05-02", 2),
            
            new InstanceInfo("weekly0", "2008-05-01T00:00:00", "2008-05-02T00:00:00", 0),
            new InstanceInfo("weekly0", "2008-05-06T00:00:00", "2008-05-07T00:00:00", 1),
            new InstanceInfo("weekly0", "2008-05-01T00:00:00", "2008-05-31T00:00:00", 4),
            new InstanceInfo("weekly0", "2008-05-01T00:00:00", "2008-06-30T00:00:00", 8),
            
            new InstanceInfo("weekly1", "2008-05-01T00:00:00", "2008-05-02T00:00:00", 0),
            new InstanceInfo("weekly1", "2008-05-06T00:00:00", "2008-05-07T00:00:00", 1),
            new InstanceInfo("weekly1", "2008-05-01T00:00:00", "2008-05-31T00:00:00", 2),
            new InstanceInfo("weekly1", "2008-05-01T00:00:00", "2008-06-30T00:00:00", 4),

            new InstanceInfo("monthly0", "2008-05-01T00:00:00", "2008-05-20T13:00:00", 0),
            new InstanceInfo("monthly0", "2008-05-01T00:00:00", "2008-05-20T15:00:00", 1),
            new InstanceInfo("monthly0", "2008-05-20T16:01:00", "2008-05-31T00:00:00", 0),
            new InstanceInfo("monthly0", "2008-05-20T16:01:00", "2008-06-17T14:59:00", 0),
            new InstanceInfo("monthly0", "2008-05-20T16:01:00", "2008-06-17T15:00:00", 1),
            new InstanceInfo("monthly0", "2008-05-01T00:00:00", "2008-05-31T00:00:00", 1),
            new InstanceInfo("monthly0", "2008-05-01T00:00:00", "2008-06-30T00:00:00", 2),

            new InstanceInfo("monthly1", "2008-05-01T00:00:00", "2008-05-01T01:00:00", 1),
            new InstanceInfo("monthly1", "2008-05-01T00:00:00", "2008-05-31T00:00:00", 1),
            new InstanceInfo("monthly1", "2008-05-01T00:10:00", "2008-05-31T23:59:00", 1),
            new InstanceInfo("monthly1", "2008-05-01T00:11:00", "2008-05-31T23:59:00", 0),
            new InstanceInfo("monthly1", "2008-05-01T00:00:00", "2008-06-01T00:00:00", 2),

            new InstanceInfo("monthly2", "2008-05-01T00:00:00", "2008-05-31T00:00:00", 0),
            new InstanceInfo("monthly2", "2008-05-01T00:10:00", "2008-05-31T23:00:00", 1),
            new InstanceInfo("monthly2", "2008-05-01T00:00:00", "2008-07-01T00:00:00", 1),
            new InstanceInfo("monthly2", "2008-05-01T00:00:00", "2008-08-01T00:00:00", 2),
            
            new InstanceInfo("yearly0", "2008-05-01", "2009-04-30", 1),
            new InstanceInfo("yearly0", "2008-05-01", "2009-05-02", 2),
    };
    
    /**
     * This tables of events is used to test the BusyBit database table.
     */
    private EventInfo[] mBusyBitEvents = {
            new EventInfo("1: 12am - 1am",       "2008-05-01T00:00:00", "2008-05-01T01:00:00", false),
            new EventInfo("2: 1:30am - 2am",     "2008-05-02T01:30:00", "2008-05-02T02:00:00", false),
            new EventInfo("3: 3am - 5am",        "2008-05-03T03:00:00", "2008-05-03T05:00:00", false),
            new EventInfo("4: 12am - 5am",       "2008-05-04T00:00:00", "2008-05-04T05:00:00", false),
            new EventInfo("5: 1am - 2am",        "2008-05-05T01:00:00", "2008-05-05T02:00:00", false),
            new EventInfo("5: 8am - 9am",        "2008-05-05T08:00:00", "2008-05-05T09:00:00", false),
            new EventInfo("6: 1am - 10am",       "2008-05-06T01:00:00", "2008-05-06T10:00:00", false),
            new EventInfo("6: 8am - 9am",        "2008-05-06T08:00:00", "2008-05-06T09:00:00", false),
            new EventInfo("7: 1am - 5am",        "2008-05-07T01:00:00", "2008-05-07T05:00:00", false),
            new EventInfo("7: 12am - 2am",       "2008-05-07T00:00:00", "2008-05-07T02:00:00", false),
            new EventInfo("7: 8am - 9am",        "2008-05-07T08:00:00", "2008-05-07T09:00:00", false),
            new EventInfo("7: 1pm - 2pm",        "2008-05-07T13:00:00", "2008-05-07T14:00:00", false),
            new EventInfo("7: 3:30pm - 4:30pm",  "2008-05-07T15:30:00", "2008-05-07T16:30:00", false),
            new EventInfo("7: 7pm - 8pm",        "2008-05-07T19:00:00", "2008-05-07T20:00:00", false),
            new EventInfo("7: 6:30pm - 7:30pm",  "2008-05-07T18:30:00", "2008-05-07T19:30:00", false),
            new EventInfo("7: 11pm - midnight",  "2008-05-07T23:00:00", "2008-05-08T00:00:00", false),
            new EventInfo("8: 1am - 2am",        "2008-05-08T01:00:00", "2008-05-08T02:00:00", false),
            new EventInfo("8: 3am - 4am",        "2008-05-08T03:00:00", "2008-05-08T04:00:00", false),
            new EventInfo("8: 5am - 6am",        "2008-05-08T05:00:00", "2008-05-08T06:00:00", false),
            new EventInfo("8: 7am - 8am",        "2008-05-08T07:00:00", "2008-05-08T08:00:00", false),
            new EventInfo("8: 9am - 10am",       "2008-05-08T09:00:00", "2008-05-08T10:00:00", false),
            new EventInfo("8: 11am - 12pm",      "2008-05-08T11:00:00", "2008-05-08T12:00:00", false),
            new EventInfo("8: 1pm - 2pm",        "2008-05-08T13:00:00", "2008-05-08T14:00:00", false),
            new EventInfo("8: 3pm - 4pm",        "2008-05-08T15:00:00", "2008-05-08T16:00:00", false),
            new EventInfo("8: 5pm - 6pm",        "2008-05-08T17:00:00", "2008-05-08T18:00:00", false),
            new EventInfo("8: 7pm - 8pm",        "2008-05-08T19:00:00", "2008-05-08T20:00:00", false),
            new EventInfo("8: 9pm - 10pm",       "2008-05-08T21:00:00", "2008-05-08T22:00:00", false),
            new EventInfo("8: 11pm - midnight",  "2008-05-08T23:00:00", "2008-05-09T00:00:00", false),
            new EventInfo("10: 12am - midnight", "2008-05-10T00:00:00", "2008-05-11T00:00:00", false),
            new EventInfo("12: 1 day",           "2008-05-12T00:00:00", "2008-05-13T00:00:00", true),
            new EventInfo("14: 1 day",           "2008-05-14T00:00:00", "2008-05-15T00:00:00", true),
            new EventInfo("14: 2 days",          "2008-05-14T00:00:00", "2008-05-16T00:00:00", true),
            new EventInfo("14: 3 days",          "2008-05-14T00:00:00", "2008-05-17T00:00:00", true),
            new EventInfo("15: 1am - 2am",       "2008-05-15T01:00:00", "2008-05-15T02:00:00", false),
            new EventInfo("16: 10am - 11am",     "2008-05-16T10:00:00", "2008-05-16T11:00:00", false),
            new EventInfo("16: 11pm - midnight", "2008-05-16T23:00:00", "2008-05-17T00:00:00", false),
    };

    private EventInfo[] mBusyBitRepeatingEvents = {
            new EventInfo("daily0", "daily from 5/1/2008 12am to 1am",
                    "2008-05-01T00:00:00", "2008-05-01T01:00:00",
                    "FREQ=DAILY;WKST=SU", false),
            new EventInfo("daily1", "daily from 5/1/2008 8:30am to 9:30am until 5/3/2008 8am",
                    "2008-05-01T08:30:00", "2008-05-01T09:30:00",
                    "FREQ=DAILY;UNTIL=20080503T150000Z;WKST=SU", false),
            new EventInfo("weekly0", "weekly from 5/6/2008 on Tue 1pm to 2pm",
                    "2008-05-06T13:00:00", "2008-05-06T14:00:00",
                    "FREQ=WEEKLY;BYDAY=TU;WKST=MO", false),
            new EventInfo("weekly1", "every 2 weeks from 5/6/2008 on Tue from 4:30am to 5:30am",
                    "2008-05-06T04:30:00", "2008-05-06T05:30:00",
                    "FREQ=WEEKLY;INTERVAL=2;BYDAY=TU;WKST=MO", false),
            new EventInfo("weekly2", "weekly from 5/5/2008 on Mon 1 day",
                    "2008-05-05T00:00:00", "2008-05-06T00:00:00",
                    "FREQ=WEEKLY;BYDAY=MO;WKST=MO", true),
            new EventInfo("weekly3", "weekly from 5/7/2008 on Wed 3 days",
                    "2008-05-07T00:00:00", "2008-05-10T00:00:00",
                    "FREQ=WEEKLY;BYDAY=WE;WKST=SU", true),
            new EventInfo("weekly4", "weekly from 5/8/2008 on Thu 3 days",
                    "2008-05-08T00:00:00", "2008-05-11T00:00:00",
                    "FREQ=WEEKLY;BYDAY=TH;WKST=SU", true),
            new EventInfo("monthly0", "monthly from 5/20/2008 on the 3rd Tues from 3pm to 4pm",
                    "2008-05-20T15:00:00", "2008-05-20T16:00:00",
                    "FREQ=MONTHLY;BYDAY=3TU;WKST=SU", false),
            new EventInfo("monthly1", "monthly from 5/1/2008 on the 1st from 11:00am to 11:10am",
                    "2008-05-01T11:00:00", "2008-05-01T11:10:00",
                    "FREQ=MONTHLY;WKST=SU;BYMONTHDAY=1", false),
            new EventInfo("monthly2", "monthly from 5/31/2008 on the 31st 11pm to midnight",
                    "2008-05-31T23:00:00", "2008-06-01T00:00:00",
                    "FREQ=MONTHLY;WKST=SU;BYMONTHDAY=31", false),
    };
    
    private BusyBitInfo[] mBusyBitTests = {
            new BusyBitInfo(mBusyBitEvents, "2008-05-01T00:00:00", 1,
                    new int[] { 0x1 }, new int[] { 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-02T00:00:00", 1,
                    new int[] { 0x2 }, new int[] { 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-02T00:00:00", 2,
                    new int[] { 0x2, 0x18 }, new int[] { 0, 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-01T00:00:00", 3,
                    new int[] { 0x1, 0x2, 0x18 }, new int[] { 0, 0, 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-01T00:00:00", 8,
                    new int[] { 0x1, 0x2, 0x18, 0x1f, 0x102, 0x3fe, 0x8da11f, 0xaaaaaa },
                    new int[] { 0, 0, 0, 0, 0, 0, 0, 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-10T00:00:00", 4,
                    new int[] { 0xffffff, 0x0, 0x0, 0x0 }, new int[] { 0, 0, 1, 0 } ),
            new BusyBitInfo(mBusyBitEvents, "2008-05-14T00:00:00", 4,
                    new int[] { 0x0, 0x2, 0x800400, 0x0 }, new int[] { 3, 2, 1, 0 } ),

            // Repeating events
            new BusyBitInfo(mBusyBitRepeatingEvents, "2008-05-01T00:00:00", 3,
                    new int[] { 0xb01, 0x301, 0x1 }, new int[] { 0, 0, 0 } ),
            new BusyBitInfo(mBusyBitRepeatingEvents, "2008-05-01T00:00:00", 10,
                    new int[] { 0xb01, 0x301, 0x1, 0x1, 0x1, 0x2031, 0x1, 0x1, 0x1, 0x1 },
                    new int[] { 0, 0, 0, 0, 1, 0, 1, 2, 2, 1 } ),
            new BusyBitInfo(mBusyBitRepeatingEvents, "2008-05-18T00:00:00", 11,
                    new int[] { 0x1, 0x1, 0xa031, 0x1, 0x1, 0x1, 0x1, 0x1, 0x1, 0x2001, 0x1 },
                    new int[] { 0, 1, 0, 1, 2, 2, 1, 0, 1, 0, 1 } ),
            new BusyBitInfo(mBusyBitRepeatingEvents, "2008-05-30T00:00:00", 5,
                    new int[] { 0x1, 0x800001, 0x801, 0x1, 0x2031 },
                    new int[] { 2, 1, 0, 1, 0 } ),
    };
    
    /**
     * This sequence of commands inserts and deletes some events.
     */
    private Command[] mNormalInsertDelete = {
            new Insert("normal0"),
            new Insert("normal1"),
            new Insert("normal2"),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-31T00:01:00", 3),
            new Delete("normal1", 1),
            new QueryNumEvents(2),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-31T00:01:00", 2),
            new Delete("normal1", 0),
            new Delete("normal2", 1),
            new QueryNumEvents(1),
            new Delete("normal0", 1),
            new QueryNumEvents(0),
    };
    
    /**
     * This sequence of commands inserts and deletes some all-day events.
     */
    private Command[] mAlldayInsertDelete = {
            new Insert("allday0"),
            new Insert("allday1"),
            new QueryNumEvents(2),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-01T00:01:00", 0),
            new QueryNumInstances("2008-05-02T00:00:00", "2008-05-02T00:01:00", 2),
            new QueryNumInstances("2008-05-03T00:00:00", "2008-05-03T00:01:00", 1),
            new Delete("allday0", 1),
            new QueryNumEvents(1),
            new QueryNumInstances("2008-05-02T00:00:00", "2008-05-02T00:01:00", 1),
            new QueryNumInstances("2008-05-03T00:00:00", "2008-05-03T00:01:00", 1),
            new Delete("allday1", 1),
            new QueryNumEvents(0),
    };
    
    /**
     * This sequence of commands inserts and deletes some repeating events.
     */
    private Command[] mRecurringInsertDelete = {
            new Insert("daily0"),
            new Insert("daily1"),
            new QueryNumEvents(2),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-02T00:01:00", 3),
            new QueryNumInstances("2008-05-01T01:01:00", "2008-05-02T00:01:00", 2),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00", 6),
            new Delete("daily1", 1),
            new QueryNumEvents(1),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-02T00:01:00", 2),
            new QueryNumInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00", 4),
            new Delete("daily0", 1),
            new QueryNumEvents(0),
    };

    /**
     * This sequence of commands creates a recurring event with a recurrence
     * exception that moves an event outside the expansion window.  It checks that the
     * recurrence exception does not occur in the Instances database table.
     * Bug 1642665
     */
    private Command[] mExceptionWithMovedRecurrence = {
            new Insert("daily0"),
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-03T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-02T00:00:00",
                            "2008-05-03T00:00:00", }),
            new Insert("except2"),
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-03T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-03T00:00:00"}),
    };
    /**
     * This sequence of commands creates a recurring event with a recurrence
     * exception and then changes the end time of the recurring event.  It then
     * checks that the recurrence exception does not occur in the Instances
     * database table.
     */
    private Command[] mExceptionWithTruncatedRecurrence = {
            new Insert("daily0"),
            // Verify 4 occurrences of the "daily0" repeating event
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-02T00:00:00",
                    "2008-05-03T00:00:00", "2008-05-04T00:00:00"}),
            new Insert("except1"),
            new QueryNumEvents(2),
            
            // Verify that one of the 4 occurrences has its start time changed
            // so that it now matches the recurrence exception.
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-02T00:00:00",
                    "2008-05-03T02:00:00", "2008-05-04T00:00:00"}),
                    
            // Change the end time of "daily0" but it still includes the
            // recurrence exception.
            new Update("daily0", new KeyValue[] {
                    new KeyValue(Events.RRULE, "FREQ=DAILY;UNTIL=20080505T150000Z;WKST=SU"),
            }),
            
            // Verify that the recurrence exception is still there
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-02T00:00:00",
                    "2008-05-03T02:00:00", "2008-05-04T00:00:00"}),
            // This time change the end time of "daily0" so that it excludes
            // the recurrence exception.
            new Update("daily0", new KeyValue[] {
                    new KeyValue(Events.RRULE, "FREQ=DAILY;UNTIL=20080502T150000Z;WKST=SU"),
            }),
            // Verify that the recurrence exception does not appear.
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-04T00:01:00",
                    new String[] {"2008-05-01T00:00:00", "2008-05-02T00:00:00"}),
    };

    /**
     * Bug 135848.  Ensure that a recurrence exception is displayed even if the recurrence
     * is not present.
     */
    private Command[] mExceptionWithNoRecurrence = {
                new Insert("except0"),
            new QueryNumEvents(1),
            new VerifyAllInstances("2008-05-01T00:00:00", "2008-05-03T00:01:00",
                    new String[] {"2008-05-01T02:00:00"}),
    };
    
    private EventInfo findEvent(String name) {
        int len = mEvents.length;
        for (int ii = 0; ii < len; ii++) {
            EventInfo event = mEvents[ii];
            if (name.equals(event.mTitle)) {
                return event;
            }
        }
        return null;
    }

    public CalendarProviderTest() {
        super(CalendarProvider.class, Calendar.AUTHORITY);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mContext = getMockContext();
        mResolver = getMockContentResolver();
        mResolver.addProvider("subscribedfeeds", new MockProvider("subscribedfeeds"));
        mResolver.addProvider("sync", new MockProvider("sync"));

        mDb = getProvider().getDatabase();
        mMetaData = getProvider().mMetaData;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Dumps the contents of the given cursor to the log.  For debugging.
     * @param cursor the database cursor
     */
    private void dumpCursor(Cursor cursor) {
        String[] cols = cursor.getColumnNames();

        Log.i(TAG, "dumpCursor() count: " + cursor.getCount());
        int index = 0;
        while (cursor.moveToNext()) {
            Log.i(TAG, index + " {");
            for (int i = 0; i < cols.length; i++) {
                Log.i(TAG, "    " + cols[i] + '=' + cursor.getString(i));
            }
            Log.i(TAG, "}");
            index += 1;
        }
    }

    private int insertCal(String name, String timezone) {
        ContentValues m = new ContentValues();
        m.put(Calendars.NAME, name);
        m.put(Calendars.DISPLAY_NAME, name);
        m.put(Calendars.COLOR, "0xff123456");
        m.put(Calendars.TIMEZONE, timezone);
        m.put(Calendars.SELECTED, 1);
        Uri url = mResolver.insert(Uri.parse("content://calendar/calendars"), m);
        String id = url.getLastPathSegment();
        return Integer.parseInt(id);
    }
    
    private Uri insertEvent(int calId, EventInfo event) {
        ContentValues m = new ContentValues();
        m.put(Events.CALENDAR_ID, calId);
        m.put(Events.TITLE, event.mTitle);
        m.put(Events.DTSTART, event.mDtstart);
        m.put(Events.ALL_DAY, event.mAllDay ? 1 : 0);
        
        if (event.mRrule == null) {
            // This is a normal event
            m.put(Events.DTEND, event.mDtend);
        } else {
            // This is a repeating event
            m.put(Events.RRULE, event.mRrule);
            m.put(Events.DURATION, event.mDuration);
        }
        
        if (event.mDescription != null) {
            m.put(Events.DESCRIPTION, event.mDescription);
        }
        if (event.mTimezone != null) {
            m.put(Events.EVENT_TIMEZONE, event.mTimezone);
        }
        
        if (event.mOriginalTitle != null) {
            // This is a recurrence exception.
            EventInfo recur = findEvent(event.mOriginalTitle);
            assertNotNull(recur);
            String syncId = String.format("%d", recur.mSyncId);
            m.put(Events.ORIGINAL_EVENT, syncId);
            m.put(Events.ORIGINAL_ALL_DAY, recur.mAllDay ? 1 : 0);
            m.put(Events.ORIGINAL_INSTANCE_TIME, event.mOriginalInstance);
        }
        Uri url = mResolver.insert(mEventsUri, m);
        
        // Create a fake _sync_id and add it to the event.  Update the database
        // directly so that we don't trigger any validation checks in the
        // CalendarProvider.
        long id = ContentUris.parseId(url);
        mDb.execSQL("UPDATE Events SET _sync_id=" + mGlobalSyncId + " WHERE _id=" + id);
        event.mSyncId = mGlobalSyncId;
        mGlobalSyncId += 1;

        return url;
    }
    
    /**
     * Deletes all the events that match the given title.
     * @param title the given title to match events on
     * @return the number of rows deleted
     */
    private int deleteMatchingEvents(String title) {
        Cursor cursor = mResolver.query(mEventsUri, new String[] { Events._ID },
                "title=?", new String[] { title }, null);
        int numRows = 0;
        while (cursor.moveToNext()) {
            long id = cursor.getLong(0);
            Uri uri = ContentUris.withAppendedId(Events.CONTENT_URI, id);
            numRows += mResolver.delete(uri, null, null);
        }
        cursor.close();
        return numRows;
    }
    
    /**
     * Updates all the events that match the given title.
     * @param title the given title to match events on
     * @return the number of rows updated
     */
    private int updateMatchingEvents(String title, ContentValues values) {
        String[] projection = new String[] {
                Events._ID,
                Events.DTSTART,
                Events.DTEND,
                Events.DURATION,
                Events.ALL_DAY,
                Events.RRULE,
                Events.EVENT_TIMEZONE,
        };
        Cursor cursor = mResolver.query(mEventsUri, projection,
                "title=?", new String[] { title }, null);
        int numRows = 0;
        while (cursor.moveToNext()) {
            long id = cursor.getLong(0);
            
            // If any of the following fields are being changed, then we need
            // to include all of them.
            if (values.containsKey(Events.DTSTART) || values.containsKey(Events.DTEND)
                    || values.containsKey(Events.DURATION) || values.containsKey(Events.ALL_DAY)
                    || values.containsKey(Events.RRULE)
                    || values.containsKey(Events.EVENT_TIMEZONE)) {
                long dtstart = cursor.getLong(1);
                long dtend = cursor.getLong(2);
                String duration = cursor.getString(3);
                boolean allDay = cursor.getInt(4) != 0;
                String rrule = cursor.getString(5);
                String timezone = cursor.getString(6);
                
                if (!values.containsKey(Events.DTSTART)) {
                    values.put(Events.DTSTART, dtstart);
                }
                // Don't add DTEND for repeating events
                if (!values.containsKey(Events.DTEND) && rrule == null) {
                    values.put(Events.DTEND, dtend);
                }
                if (!values.containsKey(Events.DURATION) && duration != null) {
                    values.put(Events.DURATION, duration);
                }
                if (!values.containsKey(Events.ALL_DAY)) {
                    values.put(Events.ALL_DAY, allDay ? 1 : 0);
                }
                if (!values.containsKey(Events.RRULE) && rrule != null) {
                    values.put(Events.RRULE, rrule);
                }
                if (!values.containsKey(Events.EVENT_TIMEZONE) && timezone != null) {
                    values.put(Events.EVENT_TIMEZONE, timezone);
                }
            }
            
            Uri uri = ContentUris.withAppendedId(Events.CONTENT_URI, id);
            numRows += mResolver.update(uri, values, null, null);
        }
        cursor.close();
        return numRows;
    }
    
    private void deleteAllEvents() {
        mDb.execSQL("DELETE FROM Events;");
        mMetaData.clearInstanceRange();
    }

    public void testInsertNormalEvents() throws Exception {
        Cursor cursor;
        Uri url = null;

        int calId = insertCal("Calendar0", DEFAULT_TIMEZONE);

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(0, cursor.getCount());
        cursor.close();

        // Keep track of the number of normal events
        int numEvents = 0;
        
        // "begin" is the earliest start time of all the normal events,
        // and "end" is the latest end time of all the normal events.
        long begin = 0, end = 0;
        
        int len = mEvents.length;
        for (int ii = 0; ii < len; ii++) {
            EventInfo event = mEvents[ii];
            // Skip repeating events and recurrence exceptions
            if (event.mRrule != null || event.mOriginalTitle != null) {
                continue;
            }
            if (numEvents == 0) {
                begin = event.mDtstart;
                end = event.mDtend;
            } else {
                if (begin > event.mDtstart) {
                    begin = event.mDtstart;
                }
                if (end < event.mDtend) {
                    end = event.mDtend;
                }
            }
            url = insertEvent(calId, event);
            numEvents += 1;
        }

        // query one
        cursor = mResolver.query(url, null, null, null, null);
        assertEquals(1, cursor.getCount());
        cursor.close();

        // query all
        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(numEvents, cursor.getCount());
        cursor.close();
        
        // Check that the Instances table has one instance of each of the
        // normal events.
        cursor = queryInstances(begin, end);
        assertEquals(numEvents, cursor.getCount());
        cursor.close();
    }

    public void testInsertRepeatingEvents() throws Exception {
        Cursor cursor;
        Uri url = null;

        int calId = insertCal("Calendar0", "America/Los_Angeles");

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(0, cursor.getCount());
        cursor.close();

        // Keep track of the number of repeating events
        int numEvents = 0;

        int len = mEvents.length;
        for (int ii = 0; ii < len; ii++) {
            EventInfo event = mEvents[ii];
            // Skip normal events
            if (event.mRrule == null) {
                continue;
            }
            url = insertEvent(calId, event);
            numEvents += 1;
        }

        // query one
        cursor = mResolver.query(url, null, null, null, null);
        assertEquals(1, cursor.getCount());
        cursor.close();

        // query all
        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(numEvents, cursor.getCount());
        cursor.close();
    }

    public void testInstanceRange() throws Exception {
        Cursor cursor;
        Uri url = null;

        int calId = insertCal("Calendar0", "America/Los_Angeles");

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(0, cursor.getCount());
        cursor.close();

        int len = mInstanceRanges.length;
        for (int ii = 0; ii < len; ii++) {
            InstanceInfo instance = mInstanceRanges[ii];
            EventInfo event = instance.mEvent;
            url = insertEvent(calId, event);
            cursor = queryInstances(instance.mBegin, instance.mEnd);
            if (instance.mExpectedOccurrences != cursor.getCount()) {
                Log.e(TAG, "Test failed! Instance index: " + ii);
                Log.e(TAG, "title: " + event.mTitle + " desc: " + event.mDescription
                        + " [begin,end]: [" + instance.mBegin + " " + instance.mEnd + "]"
                        + " expected: " + instance.mExpectedOccurrences);
                dumpCursor(cursor);
            }
            assertEquals(instance.mExpectedOccurrences, cursor.getCount());
            cursor.close();
            int rows = mResolver.delete(url, null /* selection */, null /* selection args */);
            assertEquals(1, rows);
        }
    }
    
    public void testBusyBitRange() throws Exception {
        Cursor cursor;
        Uri url = null;

        int calId = insertCal("Calendar0", "America/Los_Angeles");

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(0, cursor.getCount());
        cursor.close();

        int len = mBusyBitTests.length;
        for (int ii = 0; ii < len; ii++) {
            deleteAllEvents();
            BusyBitInfo busyInfo = mBusyBitTests[ii];
            EventInfo[] events = busyInfo.mEvents;
            int numEvents = events.length;
            for (int jj = 0; jj < numEvents; jj++) {
                EventInfo event = events[jj];
                insertEvent(calId, event);
            }
            
            int startDay = busyInfo.mStartDay;
            int numDays = busyInfo.mNumDays;
            int[] busybits = new int[numDays];
            int[] allDayCounts = new int[numDays];
            
            if (false) {
                cursor = mResolver.query(mEventsUri, null, null, null, null);
                Log.i(TAG, "Dump of Events table, count: " + cursor.getCount());
                dumpCursor(cursor);
                cursor.close();

                Time time = new Time();
                time.setJulianDay(startDay);
                long begin = time.toMillis(true);
                int endDay = startDay + numDays - 1;
                time.setJulianDay(endDay);
                long end = time.toMillis(true);
                cursor = queryInstances(begin, end);
                Log.i(TAG, "Dump of Instances table, count: " + cursor.getCount()
                        + " startDay: " + startDay + " endDay: " + endDay
                        + " begin: " + begin + " end: " + end);
                dumpCursor(cursor);
                cursor.close();
            }
            
            cursor = queryBusyBits(startDay, numDays);
            try {
                int dayColumnIndex = cursor.getColumnIndexOrThrow(BusyBits.DAY);
                int busybitColumnIndex = cursor.getColumnIndexOrThrow(BusyBits.BUSYBITS);
                int allDayCountColumnIndex = cursor.getColumnIndexOrThrow(BusyBits.ALL_DAY_COUNT);
                
                while (cursor.moveToNext()) {
                    int day = cursor.getInt(dayColumnIndex);
                    int dayIndex = day - startDay;
                    busybits[dayIndex] = cursor.getInt(busybitColumnIndex);
                    allDayCounts[dayIndex] = cursor.getInt(allDayCountColumnIndex);
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }
            
            // Compare the database busy bits with the expected busy bits
            for (int dayIndex = 0; dayIndex < numDays; dayIndex++) {
                if (busyInfo.mBusyBits[dayIndex] != busybits[dayIndex]) {
                    String mesg = String.format("Test failed!"
                            + " BusyBit test index: %d"
                            + " day index: %d"
                            + " mStartDay: %d mNumDays: %d"
                            + " expected busybits: 0x%x was: 0x%x",
                            ii, dayIndex, busyInfo.mStartDay, busyInfo.mNumDays,
                            busyInfo.mBusyBits[dayIndex], busybits[dayIndex]);
                    Log.e(TAG, mesg);
                    
                    cursor = mResolver.query(mEventsUri, null, null, null, null);
                    Log.i(TAG, "Dump of Events table, count: " + cursor.getCount());
                    dumpCursor(cursor);
                    cursor.close();
                }
                assertEquals(busyInfo.mBusyBits[dayIndex], busybits[dayIndex]);
            }
            
            // Compare the database all-day counts with the expected all-day counts
            for (int dayIndex = 0; dayIndex < numDays; dayIndex++) {
                if (busyInfo.mAllDayCounts[dayIndex] != allDayCounts[dayIndex]) {
                    String mesg = String.format("Test failed!"
                            + " BusyBit test index: %d"
                            + " day index: %d"
                            + " expected all-day count: %d was: %d",
                            ii, dayIndex,
                            busyInfo.mAllDayCounts[dayIndex], allDayCounts[dayIndex]);
                    Log.e(TAG, mesg);
                }
                assertEquals(busyInfo.mAllDayCounts[dayIndex], allDayCounts[dayIndex]);
            }
        }
    }

    public void testCommandSequences() throws Exception {
        Cursor cursor;
        Uri url = null;

        mCalendarId = insertCal("Calendar0", DEFAULT_TIMEZONE);

        cursor = mResolver.query(mEventsUri, null, null, null, null);
        assertEquals(0, cursor.getCount());
        cursor.close();

        Log.i(TAG, "Normal insert/delete");
        Command[] commands = mNormalInsertDelete;
        for (Command command : commands) {
            command.execute();
        }
        
        deleteAllEvents();
        
        Log.i(TAG, "All-day insert/delete");
        commands = mAlldayInsertDelete;
        for (Command command : commands) {
            command.execute();
        }
        
        deleteAllEvents();
        
        Log.i(TAG, "Recurring insert/delete");
        commands = mRecurringInsertDelete;
        for (Command command : commands) {
            command.execute();
        }
        
        deleteAllEvents();

        Log.i(TAG, "Exception with truncated recurrence");
        commands = mExceptionWithTruncatedRecurrence;
        for (Command command : commands) {
            command.execute();
        }

        deleteAllEvents();

        Log.i(TAG, "Exception with moved recurrence");
        commands = mExceptionWithMovedRecurrence;
        for (Command command : commands) {
            command.execute();
        }

        deleteAllEvents();

        Log.i(TAG, "Exception with no recurrence");
        commands = mExceptionWithNoRecurrence;
        for (Command command : commands) {
            command.execute();
        }
    }
    
    private Cursor queryInstances(long begin, long end) {
        Uri url = Uri.parse("content://calendar/instances/when/" + begin + "/" + end);
        return mResolver.query(url, null, null, null, null);
    }

    private Cursor queryBusyBits(int startDay, int numDays) {
        int endDay = startDay + numDays - 1;
        Uri url = Uri.parse("content://calendar/busybits/when/" + startDay + "/" + endDay);
        return mResolver.query(url, null, null, null, null);
    }
    
    protected static class MockProvider extends ContentProvider {

        private String mAuthority;

        private int mNumItems = 0;

        public MockProvider(String authority) {
            mAuthority = authority;
        }

        @Override
        public boolean onCreate() {
            return true;
        }

        @Override
        public Cursor query(Uri uri, String[] projection, String selection,
                String[] selectionArgs, String sortOrder) {
            return new ArrayListCursor(new String[]{}, new ArrayList<ArrayList>());
        }

        @Override
        public String getType(Uri uri) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uri insert(Uri uri, ContentValues values) {
            mNumItems++;
            return Uri.parse("content://" + mAuthority + "/" + mNumItems);
        }

        @Override
        public int delete(Uri uri, String selection, String[] selectionArgs) {
            return 0;
        }

        @Override
        public int update(Uri uri, ContentValues values, String selection,
                String[] selectionArgs) {
            return 0;
        }
    }
}
