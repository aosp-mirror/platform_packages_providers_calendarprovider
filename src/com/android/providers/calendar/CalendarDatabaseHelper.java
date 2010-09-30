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
 * limitations under the License
 */

package com.android.providers.calendar;

import com.google.common.annotations.VisibleForTesting;

import com.android.internal.content.SyncStateContentProviderHelper;

import android.accounts.Account;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.res.Resources;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Bundle;
import android.provider.Calendar;
import android.provider.ContactsContract;
import android.provider.SyncStateContract;
import android.text.TextUtils;
import android.text.format.Time;
import android.util.Log;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.TimeZone;

/**
 * Database helper for calendar. Designed as a singleton to make sure that all
 * {@link android.content.ContentProvider} users get the same reference.
 */
/* package */ class CalendarDatabaseHelper extends SQLiteOpenHelper {
    private static final String TAG = "CalendarDatabaseHelper";

    private static final String DATABASE_NAME = "calendar.db";

    private static final int DAY_IN_SECONDS = 24 * 60 * 60;

    // TODO: change the Calendar contract so these are defined there.
    static final String ACCOUNT_NAME = "_sync_account";
    static final String ACCOUNT_TYPE = "_sync_account_type";

    // Note: if you update the version number, you must also update the code
    // in upgradeDatabase() to modify the database (gracefully, if possible).
    static final int DATABASE_VERSION = 101;

    private static final int PRE_FROYO_SYNC_STATE_VERSION = 3;

    // Copied from SyncStateContentProviderHelper.  Don't really want to make them public there.
    private static final String SYNC_STATE_TABLE = "_sync_state";
    private static final String SYNC_STATE_META_TABLE = "_sync_state_metadata";
    private static final String SYNC_STATE_META_VERSION_COLUMN = "version";

    private final Context mContext;
    private final SyncStateContentProviderHelper mSyncState;

    private static CalendarDatabaseHelper sSingleton = null;

    private DatabaseUtils.InsertHelper mCalendarsInserter;
    private DatabaseUtils.InsertHelper mEventsInserter;
    private DatabaseUtils.InsertHelper mEventsRawTimesInserter;
    private DatabaseUtils.InsertHelper mInstancesInserter;
    private DatabaseUtils.InsertHelper mAttendeesInserter;
    private DatabaseUtils.InsertHelper mRemindersInserter;
    private DatabaseUtils.InsertHelper mCalendarAlertsInserter;
    private DatabaseUtils.InsertHelper mExtendedPropertiesInserter;

    public long calendarsInsert(ContentValues values) {
        return mCalendarsInserter.insert(values);
    }

    public long eventsInsert(ContentValues values) {
        return mEventsInserter.insert(values);
    }

    public long eventsRawTimesInsert(ContentValues values) {
        return mEventsRawTimesInserter.insert(values);
    }

    public long eventsRawTimesReplace(ContentValues values) {
        return mEventsRawTimesInserter.replace(values);
    }

    public long instancesInsert(ContentValues values) {
        return mInstancesInserter.insert(values);
    }

    public long instancesReplace(ContentValues values) {
        return mInstancesInserter.replace(values);
    }

    public long attendeesInsert(ContentValues values) {
        return mAttendeesInserter.insert(values);
    }

    public long remindersInsert(ContentValues values) {
        return mRemindersInserter.insert(values);
    }

    public long calendarAlertsInsert(ContentValues values) {
        return mCalendarAlertsInserter.insert(values);
    }

    public long extendedPropertiesInsert(ContentValues values) {
        return mExtendedPropertiesInserter.insert(values);
    }

    public static synchronized CalendarDatabaseHelper getInstance(Context context) {
        if (sSingleton == null) {
            sSingleton = new CalendarDatabaseHelper(context);
        }
        return sSingleton;
    }

    /**
     * Private constructor, callers except unit tests should obtain an instance through
     * {@link #getInstance(android.content.Context)} instead.
     */
    /* package */ CalendarDatabaseHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
        if (false) Log.i(TAG, "Creating OpenHelper");
        Resources resources = context.getResources();

        mContext = context;
        mSyncState = new SyncStateContentProviderHelper();
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        mSyncState.onDatabaseOpened(db);

        mCalendarsInserter = new DatabaseUtils.InsertHelper(db, "Calendars");
        mEventsInserter = new DatabaseUtils.InsertHelper(db, "Events");
        mEventsRawTimesInserter = new DatabaseUtils.InsertHelper(db, "EventsRawTimes");
        mInstancesInserter = new DatabaseUtils.InsertHelper(db, "Instances");
        mAttendeesInserter = new DatabaseUtils.InsertHelper(db, "Attendees");
        mRemindersInserter = new DatabaseUtils.InsertHelper(db, "Reminders");
        mCalendarAlertsInserter = new DatabaseUtils.InsertHelper(db, "CalendarAlerts");
        mExtendedPropertiesInserter =
                new DatabaseUtils.InsertHelper(db, "ExtendedProperties");
    }

    /*
     * Upgrade sync state table if necessary.  Note that the data bundle
     * in the table is not upgraded.
     *
     * The sync state used to be stored with version 3, but now uses the
     * same sync state code as contacts, which is version 1.  This code
     * upgrades from 3 to 1 if necessary.  (Yes, the numbers are unfortunately
     * backwards.)
     *
     * This code is only called when upgrading from an old calendar version,
     * so there is no problem if sync state version 3 gets used again in the
     * future.
     */
    private void upgradeSyncState(SQLiteDatabase db) {
        long version = DatabaseUtils.longForQuery(db,
                 "SELECT " + SYNC_STATE_META_VERSION_COLUMN
                 + " FROM " + SYNC_STATE_META_TABLE,
                 null);
        if (version == PRE_FROYO_SYNC_STATE_VERSION) {
            Log.i(TAG, "Upgrading calendar sync state table");
            db.execSQL("CREATE TEMPORARY TABLE state_backup(_sync_account TEXT, "
                    + "_sync_account_type TEXT, data TEXT);");
            db.execSQL("INSERT INTO state_backup SELECT _sync_account, _sync_account_type, data"
                    + " FROM "
                    + SYNC_STATE_TABLE
                    + " WHERE _sync_account is not NULL and _sync_account_type is not NULL;");
            db.execSQL("DROP TABLE " + SYNC_STATE_TABLE + ";");
            mSyncState.onDatabaseOpened(db);
            db.execSQL("INSERT INTO " + SYNC_STATE_TABLE + "("
                    + SyncStateContract.Columns.ACCOUNT_NAME + ","
                    + SyncStateContract.Columns.ACCOUNT_TYPE + ","
                    + SyncStateContract.Columns.DATA
                    + ") SELECT _sync_account, _sync_account_type, data from state_backup;");
            db.execSQL("DROP TABLE state_backup;");
        } else {
            // Wrong version to upgrade.
            // Don't need to do anything more here because mSyncState.onDatabaseOpened() will blow
            // away and recreate  the database (which will result in a resync).
            Log.w(TAG, "upgradeSyncState: current version is " + version + ", skipping upgrade.");
        }
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        bootstrapDB(db);
    }

    private void bootstrapDB(SQLiteDatabase db) {
        Log.i(TAG, "Bootstrapping database");

        mSyncState.createDatabase(db);

        db.execSQL("CREATE TABLE Calendars (" +
                "_id INTEGER PRIMARY KEY," +
                ACCOUNT_NAME + " TEXT," +
                ACCOUNT_TYPE + " TEXT," +
                "_sync_id TEXT," +
                "_sync_version TEXT," +
                "_sync_time TEXT," +            // UTC
                "_sync_local_id INTEGER," +
                "_sync_dirty INTEGER," +
                "_sync_mark INTEGER," + // Used to filter out new rows
                "url TEXT," +
                "name TEXT," +
                "displayName TEXT," +
                "hidden INTEGER NOT NULL DEFAULT 0," +
                "color INTEGER," +
                "access_level INTEGER," +
                "selected INTEGER NOT NULL DEFAULT 1," +
                "sync_events INTEGER NOT NULL DEFAULT 0," +
                "location TEXT," +
                "timezone TEXT," +
                "ownerAccount TEXT, " +
                "organizerCanRespond INTEGER NOT NULL DEFAULT 1" +
                ");");

        // Trigger to remove a calendar's events when we delete the calendar
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON Calendars " +
                "BEGIN " +
                "DELETE FROM Events WHERE calendar_id = old._id;" +
                "END");

        // TODO: do we need both dtend and duration?
        db.execSQL("CREATE TABLE Events (" +
                "_id INTEGER PRIMARY KEY," +
                ACCOUNT_NAME + " TEXT," +
                ACCOUNT_TYPE + " TEXT," +
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

        // Trigger to set event's sync_account
        db.execSQL("CREATE TRIGGER events_insert AFTER INSERT ON Events " +
                "BEGIN " +
                "UPDATE Events SET _sync_account=" +
                "(SELECT _sync_account FROM Calendars WHERE Calendars._id=new.calendar_id)," +
                "_sync_account_type=" +
                "(SELECT _sync_account_type FROM Calendars WHERE Calendars._id=new.calendar_id) " +
                "WHERE Events._id=new._id;" +
                "END");

        db.execSQL("CREATE INDEX eventSyncAccountAndIdIndex ON Events ("
                + Calendar.Events._SYNC_ACCOUNT_TYPE + ", " + Calendar.Events._SYNC_ACCOUNT + ", "
                + Calendar.Events._SYNC_ID + ");");

        db.execSQL("CREATE INDEX eventsCalendarIdIndex ON Events (" +
                Calendar.Events.CALENDAR_ID +
                ");");

        db.execSQL("CREATE TABLE EventsRawTimes (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER NOT NULL," +
                "dtstart2445 TEXT," +
                "dtend2445 TEXT," +
                "originalInstanceTime2445 TEXT," +
                "lastDate2445 TEXT," +
                "UNIQUE (event_id)" +
                ");");

        db.execSQL("CREATE TABLE Instances (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER," +
                "begin INTEGER," +         // UTC millis
                "end INTEGER," +           // UTC millis
                "startDay INTEGER," +      // Julian start day
                "endDay INTEGER," +        // Julian end day
                "startMinute INTEGER," +   // minutes from midnight
                "endMinute INTEGER," +     // minutes from midnight
                "UNIQUE (event_id, begin, end)" +
                ");");

        db.execSQL("CREATE INDEX instancesStartDayIndex ON Instances (" +
                Calendar.Instances.START_DAY +
                ");");

        createCalendarMetaDataTable(db);

        createCalendarCacheTable(db, null);

        db.execSQL("CREATE TABLE Attendees (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER," +
                "attendeeName TEXT," +
                "attendeeEmail TEXT," +
                "attendeeStatus INTEGER," +
                "attendeeRelationship INTEGER," +
                "attendeeType INTEGER" +
                ");");

        db.execSQL("CREATE INDEX attendeesEventIdIndex ON Attendees (" +
                Calendar.Attendees.EVENT_ID +
                ");");

        db.execSQL("CREATE TABLE Reminders (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER," +
                "minutes INTEGER," +
                "method INTEGER NOT NULL" +
                " DEFAULT " + Calendar.Reminders.METHOD_DEFAULT +
                ");");

        db.execSQL("CREATE INDEX remindersEventIdIndex ON Reminders (" +
                Calendar.Reminders.EVENT_ID +
                ");");

         // This table stores the Calendar notifications that have gone off.
        db.execSQL("CREATE TABLE CalendarAlerts (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER," +
                "begin INTEGER NOT NULL," +         // UTC millis
                "end INTEGER NOT NULL," +           // UTC millis
                "alarmTime INTEGER NOT NULL," +     // UTC millis
                "creationTime INTEGER NOT NULL," +  // UTC millis
                "receivedTime INTEGER NOT NULL," +  // UTC millis
                "notifyTime INTEGER NOT NULL," +    // UTC millis
                "state INTEGER NOT NULL," +
                "minutes INTEGER," +
                "UNIQUE (alarmTime, begin, event_id)" +
                ");");

        db.execSQL("CREATE INDEX calendarAlertsEventIdIndex ON CalendarAlerts (" +
                Calendar.CalendarAlerts.EVENT_ID +
                ");");

        db.execSQL("CREATE TABLE ExtendedProperties (" +
                "_id INTEGER PRIMARY KEY," +
                "event_id INTEGER," +
                "name TEXT," +
                "value TEXT" +
                ");");

        db.execSQL("CREATE INDEX extendedPropertiesEventIdIndex ON ExtendedProperties (" +
                Calendar.ExtendedProperties.EVENT_ID +
                ");");

        // Trigger to remove data tied to an event when we delete that event.
        db.execSQL("CREATE TRIGGER events_cleanup_delete DELETE ON Events " +
                "BEGIN " +
                "DELETE FROM Instances WHERE event_id = old._id;" +
                "DELETE FROM EventsRawTimes WHERE event_id = old._id;" +
                "DELETE FROM Attendees WHERE event_id = old._id;" +
                "DELETE FROM Reminders WHERE event_id = old._id;" +
                "DELETE FROM CalendarAlerts WHERE event_id = old._id;" +
                "DELETE FROM ExtendedProperties WHERE event_id = old._id;" +
                "END");

        createEventsView(db);

        ContentResolver.requestSync(null /* all accounts */,
                ContactsContract.AUTHORITY, new Bundle());
    }

    private void createCalendarMetaDataTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE CalendarMetaData (" +
                "_id INTEGER PRIMARY KEY," +
                "localTimezone TEXT," +
                "minInstance INTEGER," +      // UTC millis
                "maxInstance INTEGER" +       // UTC millis
                ");");
    }

    private void createCalendarCacheTable(SQLiteDatabase db, String oldTimezoneDbVersion) {
        // This is a hack because versioning skipped version number 61 of schema
        // TODO after version 70 this can be removed
        db.execSQL("DROP TABLE IF EXISTS CalendarCache;");

        // IF NOT EXISTS should be normal pattern for table creation
        db.execSQL("CREATE TABLE IF NOT EXISTS CalendarCache (" +
                "_id INTEGER PRIMARY KEY," +
                "key TEXT NOT NULL," +
                "value TEXT" +
                ");");

        initCalendarCacheTable(db, oldTimezoneDbVersion);
        updateCalendarCacheTableTo101(db);
    }

    private void initCalendarCacheTable(SQLiteDatabase db, String oldTimezoneDbVersion) {
        String timezoneDbVersion = (oldTimezoneDbVersion != null) ?
                oldTimezoneDbVersion : CalendarCache.DEFAULT_TIMEZONE_DATABASE_VERSION;

        // Set the default timezone database version
        db.execSQL("INSERT OR REPLACE INTO CalendarCache (_id, key, value) VALUES (" +
                CalendarCache.KEY_TIMEZONE_DATABASE_VERSION.hashCode() + "," +
                "'" + CalendarCache.KEY_TIMEZONE_DATABASE_VERSION + "',"  +
                "'" + timezoneDbVersion + "'" +
                ");");
    }

    private void updateCalendarCacheTableTo101(SQLiteDatabase db) {
        // Define the default timezone type for Instances timezone management
        db.execSQL("INSERT INTO CalendarCache (_id, key, value) VALUES (" +
                CalendarCache.KEY_TIMEZONE_TYPE.hashCode() + "," +
                "'" + CalendarCache.KEY_TIMEZONE_TYPE + "',"  +
                "'" + CalendarCache.TIMEZONE_TYPE_AUTO + "'" +
                ");");

        String defaultTimezone = TimeZone.getDefault().getID();

        // Define the default timezone for Instances
        db.execSQL("INSERT INTO CalendarCache (_id, key, value) VALUES (" +
                CalendarCache.KEY_TIMEZONE_INSTANCES.hashCode() + "," +
                "'" + CalendarCache.KEY_TIMEZONE_INSTANCES + "',"  +
                "'" + defaultTimezone + "'" +
                ");");

        // Define the default previous timezone for Instances
        db.execSQL("INSERT INTO CalendarCache (_id, key, value) VALUES (" +
                CalendarCache.KEY_TIMEZONE_INSTANCES_PREVIOUS.hashCode() + "," +
                "'" + CalendarCache.KEY_TIMEZONE_INSTANCES_PREVIOUS + "',"  +
                "'" + defaultTimezone + "'" +
                ");");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.i(TAG, "Upgrading DB from version " + oldVersion
                + " to " + newVersion);
        if (oldVersion < 49) {
            dropTables(db);
            mSyncState.createDatabase(db);
            return; // this was lossy
        }

        // From schema versions 59 to version 66, the CalendarMetaData table definition had lost
        // the primary key leading to having the CalendarMetaData with multiple rows instead of
        // only one. The Instance table was then corrupted (during Instance expansion we are using
        // the localTimezone, minInstance and maxInstance from CalendarMetaData table.
        // This boolean helps us tracking the need to recreate the CalendarMetaData table and
        // clear the Instance table (and thus force an Instance expansion).
        boolean recreateMetaDataAndInstances = (oldVersion >= 59 && oldVersion <= 66);

        try {
            if (oldVersion < 51) {
                upgradeToVersion51(db); // From 50 or 51
                oldVersion = 51;
            }
            if (oldVersion == 51) {
                upgradeToVersion52(db);
                oldVersion += 1;
            }
            if (oldVersion == 52) {
                upgradeToVersion53(db);
                oldVersion += 1;
            }
            if (oldVersion == 53) {
                upgradeToVersion54(db);
                oldVersion += 1;
            }
            if (oldVersion == 54) {
                upgradeToVersion55(db);
                oldVersion += 1;
            }
            if (oldVersion == 55 || oldVersion == 56) {
                // Both require resync, so just schedule it once
                upgradeResync(db);
            }
            if (oldVersion == 55) {
                upgradeToVersion56(db);
                oldVersion += 1;
            }
            if (oldVersion == 56) {
                upgradeToVersion57(db);
                oldVersion += 1;
            }
            if (oldVersion == 57) {
                // Changes are undone upgrading to 60, so don't do anything.
                oldVersion += 1;
            }
            if (oldVersion == 58) {
                upgradeToVersion59(db);
                oldVersion += 1;
            }
            if (oldVersion == 59) {
                upgradeToVersion60(db);
                oldVersion += 1;
            }
            if (oldVersion == 60) {
                upgradeToVersion61(db);
                oldVersion += 1;
            }
            if (oldVersion == 61) {
                upgradeToVersion62(db);
                oldVersion += 1;
            }
            if (oldVersion == 62) {
                upgradeToVersion63(db);
                oldVersion += 1;
            }
            if (oldVersion == 63) {
                upgradeToVersion64(db);
                oldVersion += 1;
            }
            if (oldVersion == 64) {
                upgradeToVersion65(db);
                oldVersion += 1;
            }
            if (oldVersion == 65) {
                upgradeToVersion66(db);
                oldVersion += 1;
            }
            if (oldVersion == 66) {
                // Changes are done thru recreateMetaDataAndInstances() method
                oldVersion += 1;
            }
            if (recreateMetaDataAndInstances) {
                recreateMetaDataAndInstances(db);
            }
            if(oldVersion == 67 || oldVersion == 68) {
                upgradeToVersion69(db);
                oldVersion = 69;
            }
            if(oldVersion == 69) {
                upgradeToVersion100(db);
                oldVersion = 100;
            }
            if(oldVersion == 70) {
                // Froyo version "70" already has the CalendarCache fix
                oldVersion = 100;
            }
            if(oldVersion == 100) {
                upgradeToVersion101(db);
                oldVersion = 101;
            }
        } catch (SQLiteException e) {
            Log.e(TAG, "onUpgrade: SQLiteException, recreating db. " + e);
            dropTables(db);
            bootstrapDB(db);
            return; // this was lossy
        }
    }

    /**
     * If the user_version of the database if between 59 and 66 (those versions has been deployed
     * with no primary key for the CalendarMetaData table)
     */
    private void recreateMetaDataAndInstances(SQLiteDatabase db) {
        // Recreate the CalendarMetaData table with correct primary key
        db.execSQL("DROP TABLE CalendarMetaData;");
        createCalendarMetaDataTable(db);

        // Also clean the Instance table as this table may be corrupted
        db.execSQL("DELETE FROM Instances;");
    }

    private static boolean fixAllDayTime(Time time, String timezone, Long timeInMillis) {
        time.set(timeInMillis);
        if(time.hour != 0 || time.minute != 0 || time.second != 0) {
            time.hour = 0;
            time.minute = 0;
            time.second = 0;
            return true;
        }
        return false;
    }

    @VisibleForTesting
    void upgradeToVersion101(SQLiteDatabase db) {
        updateCalendarCacheTableTo101(db);
    }

    @VisibleForTesting
    void upgradeToVersion100(SQLiteDatabase db) {
        Cursor cursor = db.rawQuery("SELECT value from CalendarCache WHERE key=?",
                new String[] {"timezoneDatabaseVersion"});

        String oldTimezoneDbVersion = null;
        if (cursor != null && cursor.moveToNext()) {
            try {
                oldTimezoneDbVersion = cursor.getString(0);
            } finally {
                cursor.close();
            }
            // Also clean the CalendarCache table
            db.execSQL("DELETE FROM CalendarCache;");
        }
        initCalendarCacheTable(db, oldTimezoneDbVersion);
    }

    @VisibleForTesting
    static void upgradeToVersion69(SQLiteDatabase db) {
        // Clean up allDay events which could be in an invalid state from an earlier version
        // Some allDay events had hour, min, sec not set to zero, which throws elsewhere. This
        // will go through the allDay events and make sure they have proper values and are in the
        // correct timezone. Verifies that dtstart and dtend are in UTC and at midnight, that
        // eventTimezone is set to UTC, tries to make sure duration is in days, and that dtstart2
        // and dtend2 are at midnight in their timezone.
        Cursor cursor = db.rawQuery("SELECT _id, dtstart, dtend, duration, dtstart2, dtend2, " +
                "eventTimezone, eventTimezone2, rrule FROM Events WHERE allDay=?",
                new String[] {"1"});
        if (cursor != null) {
            try {
                String timezone;
                String timezone2;
                String duration;
                Long dtstart;
                Long dtstart2;
                Long dtend;
                Long dtend2;
                Time time = new Time();
                Long id;
                // some things need to be in utc so we call this frequently, cache to make faster
                final String utc = Time.TIMEZONE_UTC;
                while (cursor.moveToNext()) {
                    String rrule = cursor.getString(8);
                    id = cursor.getLong(0);
                    dtstart = cursor.getLong(1);
                    dtstart2 = null;
                    timezone = cursor.getString(6);
                    timezone2 = cursor.getString(7);
                    duration = cursor.getString(3);

                    if (TextUtils.isEmpty(rrule)) {
                        // For non-recurring events dtstart and dtend should both have values
                        // and duration should be null.
                        dtend = cursor.getLong(2);
                        dtend2 = null;
                        // Since we made all three of these at the same time if timezone2 exists
                        // so should dtstart2 and dtend2.
                        if(!TextUtils.isEmpty(timezone2)) {
                            dtstart2 = cursor.getLong(4);
                            dtend2 = cursor.getLong(5);
                        }

                        boolean update = false;
                        if (!TextUtils.equals(timezone, utc)) {
                            update = true;
                            timezone = utc;
                        }

                        time.clear(timezone);
                        update |= fixAllDayTime(time, timezone, dtstart);
                        dtstart = time.normalize(false);

                        time.clear(timezone);
                        update |= fixAllDayTime(time, timezone, dtend);
                        dtend = time.normalize(false);

                        if (dtstart2 != null) {
                            time.clear(timezone2);
                            update |= fixAllDayTime(time, timezone2, dtstart2);
                            dtstart2 = time.normalize(false);
                        }

                        if (dtend2 != null) {
                            time.clear(timezone2);
                            update |= fixAllDayTime(time, timezone2, dtend2);
                            dtend2 = time.normalize(false);
                        }

                        if (!TextUtils.isEmpty(duration)) {
                            update = true;
                        }

                        if (update) {
                            // enforce duration being null
                            db.execSQL("UPDATE Events " +
                                    "SET dtstart=?, dtend=?, dtstart2=?, dtend2=?, duration=?, " +
                                    "eventTimezone=?, eventTimezone2=? WHERE _id=?",
                                    new Object[] {dtstart, dtend, dtstart2, dtend2, null, timezone,
                                            timezone2, id});
                        }

                    } else {
                        // For recurring events only dtstart and duration should be used.
                        // We ignore dtend since it will be overwritten if the event changes to a
                        // non-recurring event and won't be used otherwise.
                        if(!TextUtils.isEmpty(timezone2)) {
                            dtstart2 = cursor.getLong(4);
                        }

                        boolean update = false;
                        if (!TextUtils.equals(timezone, utc)) {
                            update = true;
                            timezone = utc;
                        }

                        time.clear(timezone);
                        update |= fixAllDayTime(time, timezone, dtstart);
                        dtstart = time.normalize(false);

                        if (dtstart2 != null) {
                            time.clear(timezone2);
                            update |= fixAllDayTime(time, timezone2, dtstart2);
                            dtstart2 = time.normalize(false);
                        }

                        if (TextUtils.isEmpty(duration)) {
                            // If duration was missing assume a 1 day duration
                            duration = "P1D";
                            update = true;
                        } else {
                            int len = duration.length();
                            // TODO fix durations in other formats as well
                            if (duration.charAt(0) == 'P' &&
                                    duration.charAt(len - 1) == 'S') {
                                int seconds = Integer.parseInt(duration.substring(1, len - 1));
                                int days = (seconds + DAY_IN_SECONDS - 1) / DAY_IN_SECONDS;
                                duration = "P" + days + "D";
                                update = true;
                            }
                        }

                        if (update) {
                            // If there were other problems also enforce dtend being null
                            db.execSQL("UPDATE Events " +
                                    "SET dtstart=?,dtend=?,dtstart2=?,dtend2=?,duration=?," +
                                    "eventTimezone=?, eventTimezone2=? WHERE _id=?",
                                    new Object[] {dtstart, null, dtstart2, null, duration,
                                            timezone, timezone2, id});
                        }
                    }
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeToVersion66(SQLiteDatabase db) {
        // Add a column to indicate whether the event organizer can respond to his own events
        // The UI should not show attendee status for events in calendars with this column = 0
        db.execSQL("ALTER TABLE " +
                "Calendars ADD COLUMN organizerCanRespond INTEGER NOT NULL DEFAULT 1;");
    }

    private void upgradeToVersion65(SQLiteDatabase db) {
        // we need to recreate the Events view
        createEventsView(db);
    }

    private void upgradeToVersion64(SQLiteDatabase db) {
        // Add a column that may be used by sync adapters
        db.execSQL("ALTER TABLE Events ADD COLUMN syncAdapterData TEXT;");
    }

    private void upgradeToVersion63(SQLiteDatabase db) {
        // we need to recreate the Events view
        createEventsView(db);
    }

    private void upgradeToVersion62(SQLiteDatabase db) {
        // New columns are to transition to having allDay events in the local timezone
        db.execSQL("ALTER TABLE Events ADD COLUMN dtstart2 INTEGER;");
        db.execSQL("ALTER TABLE Events ADD COLUMN dtend2 INTEGER;");
        db.execSQL("ALTER TABLE Events ADD COLUMN eventTimezone2 TEXT;");

        String[] allDayBit = new String[] {"0"};
        // Copy over all the data that isn't an all day event.
        db.execSQL("UPDATE Events " +
                "SET dtstart2=dtstart,dtend2=dtend,eventTimezone2=eventTimezone " +
                "WHERE allDay=?;",
                allDayBit /* selection args */);

        // "cursor" iterates over all the calendars
        allDayBit[0] = "1";
        Cursor cursor = db.rawQuery("SELECT Events._id,dtstart,dtend,eventTimezone,timezone " +
                "FROM Events INNER JOIN Calendars " +
                "WHERE Events.calendar_id=Calendars._id AND allDay=?",
                allDayBit /* selection args */);

        Time oldTime = new Time();
        Time newTime = new Time();
        // Update the allday events in the new columns
        if (cursor != null) {
            try {
                String[] newData = new String[4];
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    long id = cursor.getLong(0); // Order from query above
                    long dtstart = cursor.getLong(1);
                    long dtend = cursor.getLong(2);
                    String eTz = cursor.getString(3); // current event timezone
                    String tz = cursor.getString(4); // Calendar timezone
                    //If there's no timezone for some reason use UTC by default.
                    if(eTz == null) {
                        eTz = Time.TIMEZONE_UTC;
                    }

                    // Convert start time for all day events into the timezone of their calendar
                    oldTime.clear(eTz);
                    oldTime.set(dtstart);
                    newTime.clear(tz);
                    newTime.set(oldTime.monthDay, oldTime.month, oldTime.year);
                    newTime.normalize(false);
                    dtstart = newTime.toMillis(false /*ignoreDst*/);

                    // Convert end time for all day events into the timezone of their calendar
                    oldTime.clear(eTz);
                    oldTime.set(dtend);
                    newTime.clear(tz);
                    newTime.set(oldTime.monthDay, oldTime.month, oldTime.year);
                    newTime.normalize(false);
                    dtend = newTime.toMillis(false /*ignoreDst*/);

                    newData[0] = String.valueOf(dtstart);
                    newData[1] = String.valueOf(dtend);
                    newData[2] = tz;
                    newData[3] = String.valueOf(id);
                    db.execSQL("UPDATE Events " +
                            "SET dtstart2=?,dtend2=?,eventTimezone2=? " +
                            "WHERE _id=?",
                            newData);
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeToVersion61(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS CalendarCache;");

        // IF NOT EXISTS should be normal pattern for table creation
        db.execSQL("CREATE TABLE IF NOT EXISTS CalendarCache (" +
                "_id INTEGER PRIMARY KEY," +
                "key TEXT NOT NULL," +
                "value TEXT" +
                ");");

        db.execSQL("INSERT INTO CalendarCache (key, value) VALUES (" +
                "'" + CalendarCache.KEY_TIMEZONE_DATABASE_VERSION + "',"  +
                "'" + CalendarCache.DEFAULT_TIMEZONE_DATABASE_VERSION + "'" +
                ");");
    }

    private void upgradeToVersion60(SQLiteDatabase db) {
        // Switch to CalendarProvider2
        upgradeSyncState(db);
        db.execSQL("DROP TRIGGER IF EXISTS calendar_cleanup");
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON Calendars " +
                "BEGIN " +
                "DELETE FROM Events WHERE calendar_id = old._id;" +
                "END");
        db.execSQL("ALTER TABLE Events ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0;");
        db.execSQL("DROP TRIGGER IF EXISTS events_insert");
        db.execSQL("CREATE TRIGGER events_insert AFTER INSERT ON Events " +
                "BEGIN " +
                "UPDATE Events SET _sync_account=" +
                "(SELECT _sync_account FROM Calendars WHERE Calendars._id=new.calendar_id)," +
                "_sync_account_type=" +
                "(SELECT _sync_account_type FROM Calendars WHERE Calendars._id=new.calendar_id) " +
                "WHERE Events._id=new._id;" +
                "END");
        db.execSQL("DROP TABLE IF EXISTS DeletedEvents;");
        db.execSQL("DROP TRIGGER IF EXISTS events_cleanup_delete");
        db.execSQL("CREATE TRIGGER events_cleanup_delete DELETE ON Events " +
                "BEGIN " +
                "DELETE FROM Instances WHERE event_id = old._id;" +
                "DELETE FROM EventsRawTimes WHERE event_id = old._id;" +
                "DELETE FROM Attendees WHERE event_id = old._id;" +
                "DELETE FROM Reminders WHERE event_id = old._id;" +
                "DELETE FROM CalendarAlerts WHERE event_id = old._id;" +
                "DELETE FROM ExtendedProperties WHERE event_id = old._id;" +
                "END");
        db.execSQL("DROP TRIGGER IF EXISTS attendees_update");
        db.execSQL("DROP TRIGGER IF EXISTS attendees_insert");
        db.execSQL("DROP TRIGGER IF EXISTS attendees_delete");
        db.execSQL("DROP TRIGGER IF EXISTS reminders_update");
        db.execSQL("DROP TRIGGER IF EXISTS reminders_insert");
        db.execSQL("DROP TRIGGER IF EXISTS reminders_delete");
        db.execSQL("DROP TRIGGER IF EXISTS extended_properties_update");
        db.execSQL("DROP TRIGGER IF EXISTS extended_properties_insert");
        db.execSQL("DROP TRIGGER IF EXISTS extended_properties_delete");

        createEventsView(db);
    }

    private void upgradeToVersion59(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS BusyBits;");
        db.execSQL("CREATE TEMPORARY TABLE CalendarMetaData_Backup" +
                "(_id,localTimezone,minInstance,maxInstance);");
        db.execSQL("INSERT INTO CalendarMetaData_Backup " +
                "SELECT _id,localTimezone,minInstance,maxInstance FROM CalendarMetaData;");
        db.execSQL("DROP TABLE CalendarMetaData;");
        createCalendarMetaDataTable(db);
        db.execSQL("INSERT INTO CalendarMetaData " +
                "SELECT _id,localTimezone,minInstance,maxInstance FROM CalendarMetaData_Backup;");
        db.execSQL("DROP TABLE CalendarMetaData_Backup;");
    }

    private void upgradeToVersion57(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE Events ADD COLUMN guestsCanModify"
                + " INTEGER NOT NULL DEFAULT 0;");
        db.execSQL("ALTER TABLE Events ADD COLUMN guestsCanInviteOthers"
                + " INTEGER NOT NULL DEFAULT 1;");
        db.execSQL("ALTER TABLE Events ADD COLUMN guestsCanSeeGuests"
                + " INTEGER NOT NULL DEFAULT 1;");
        db.execSQL("ALTER TABLE Events ADD COLUMN organizer STRING;");
        db.execSQL("UPDATE Events SET organizer="
                + "(SELECT attendeeEmail FROM Attendees WHERE "
                + "Attendees.event_id = Events._id"
                + " AND Attendees.attendeeRelationship=2);");
    }

    private void upgradeToVersion56(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE Calendars ADD COLUMN ownerAccount TEXT;");
        db.execSQL("ALTER TABLE Events ADD COLUMN hasAttendeeData INTEGER;");
        // Clear _sync_dirty to avoid a client-to-server sync that could blow away
        // server attendees.
        // Clear _sync_version to pull down the server's event (with attendees)
        // Change the URLs from full-selfattendance to full
        db.execSQL("UPDATE Events"
                + " SET _sync_dirty=0,"
                + " _sync_version=NULL,"
                + " _sync_id="
                + "REPLACE(_sync_id, '/private/full-selfattendance', '/private/full'),"
                + " commentsUri ="
                + "REPLACE(commentsUri, '/private/full-selfattendance', '/private/full');");
        db.execSQL("UPDATE Calendars"
                + " SET url="
                + "REPLACE(url, '/private/full-selfattendance', '/private/full');");

        // "cursor" iterates over all the calendars
        Cursor cursor = db.rawQuery("SELECT _id, url FROM Calendars",
                null /* selection args */);
        // Add the owner column.
        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    Long id = cursor.getLong(0);
                    String url = cursor.getString(1);
                    String owner = calendarEmailAddressFromFeedUrl(url);
                    db.execSQL("UPDATE Calendars SET ownerAccount=? WHERE _id=?",
                            new Object[] {owner, id});
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeResync(SQLiteDatabase db) {
        // Delete sync state, so all records will be re-synced.
        db.execSQL("DELETE FROM _sync_state;");

        // "cursor" iterates over all the calendars
        Cursor cursor = db.rawQuery("SELECT _sync_account,_sync_account_type,url "
                + "FROM Calendars",
                null /* selection args */);
        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    String accountName = cursor.getString(0);
                    String accountType = cursor.getString(1);
                    final Account account = new Account(accountName, accountType);
                    String calendarUrl = cursor.getString(2);
                    scheduleSync(account, false /* two-way sync */, calendarUrl);
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeToVersion55(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE Calendars ADD COLUMN _sync_account_type TEXT;");
        db.execSQL("ALTER TABLE Events ADD COLUMN _sync_account_type TEXT;");
        db.execSQL("ALTER TABLE DeletedEvents ADD COLUMN _sync_account_type TEXT;");
        db.execSQL("UPDATE Calendars"
                + " SET _sync_account_type='com.google'"
                + " WHERE _sync_account IS NOT NULL");
        db.execSQL("UPDATE Events"
                + " SET _sync_account_type='com.google'"
                + " WHERE _sync_account IS NOT NULL");
        db.execSQL("UPDATE DeletedEvents"
                + " SET _sync_account_type='com.google'"
                + " WHERE _sync_account IS NOT NULL");
        Log.w(TAG, "re-creating eventSyncAccountAndIdIndex");
        db.execSQL("DROP INDEX eventSyncAccountAndIdIndex");
        db.execSQL("CREATE INDEX eventSyncAccountAndIdIndex ON Events ("
                + Calendar.Events._SYNC_ACCOUNT_TYPE + ", "
                + Calendar.Events._SYNC_ACCOUNT + ", "
                + Calendar.Events._SYNC_ID + ");");
    }

    private void upgradeToVersion54(SQLiteDatabase db) {
        Log.w(TAG, "adding eventSyncAccountAndIdIndex");
        db.execSQL("CREATE INDEX eventSyncAccountAndIdIndex ON Events ("
                + Calendar.Events._SYNC_ACCOUNT + ", " + Calendar.Events._SYNC_ID + ");");
    }

    private void upgradeToVersion53(SQLiteDatabase db) {
        Log.w(TAG, "Upgrading CalendarAlerts table");
        db.execSQL("ALTER TABLE CalendarAlerts ADD COLUMN creationTime INTEGER DEFAULT 0;");
        db.execSQL("ALTER TABLE CalendarAlerts ADD COLUMN receivedTime INTEGER DEFAULT 0;");
        db.execSQL("ALTER TABLE CalendarAlerts ADD COLUMN notifyTime INTEGER DEFAULT 0;");
    }

    private void upgradeToVersion52(SQLiteDatabase db) {
        // We added "originalAllDay" to the Events table to keep track of
        // the allDay status of the original recurring event for entries
        // that are exceptions to that recurring event.  We need this so
        // that we can format the date correctly for the "originalInstanceTime"
        // column when we make a change to the recurrence exception and
        // send it to the server.
        db.execSQL("ALTER TABLE Events ADD COLUMN originalAllDay INTEGER;");

        // Iterate through the Events table and for each recurrence
        // exception, fill in the correct value for "originalAllDay",
        // if possible.  The only times where this might not be possible
        // are (1) the original recurring event no longer exists, or
        // (2) the original recurring event does not yet have a _sync_id
        // because it was created on the phone and hasn't been synced to the
        // server yet.  In both cases the originalAllDay field will be set
        // to null.  In the first case we don't care because the recurrence
        // exception will not be displayed and we won't be able to make
        // any changes to it (and even if we did, the server should ignore
        // them, right?).  In the second case, the calendar client already
        // disallows making changes to an instance of a recurring event
        // until the recurring event has been synced to the server so the
        // second case should never occur.

        // "cursor" iterates over all the recurrences exceptions.
        Cursor cursor = db.rawQuery("SELECT _id,originalEvent FROM Events"
                + " WHERE originalEvent IS NOT NULL", null /* selection args */);
        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    long id = cursor.getLong(0);
                    String originalEvent = cursor.getString(1);

                    // Find the original recurring event (if it exists)
                    Cursor recur = db.rawQuery("SELECT allDay FROM Events"
                            + " WHERE _sync_id=?", new String[] {originalEvent});
                    if (recur == null) {
                        continue;
                    }

                    try {
                        // Fill in the "originalAllDay" field of the
                        // recurrence exception with the "allDay" value
                        // from the recurring event.
                        if (recur.moveToNext()) {
                            int allDay = recur.getInt(0);
                            db.execSQL("UPDATE Events SET originalAllDay=" + allDay
                                    + " WHERE _id="+id);
                        }
                    } finally {
                        recur.close();
                    }
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeToVersion51(SQLiteDatabase db) {
        Log.w(TAG, "Upgrading DeletedEvents table");

        // We don't have enough information to fill in the correct
        // value of the calendar_id for old rows in the DeletedEvents
        // table, but rows in that table are transient so it is unlikely
        // that there are any rows.  Plus, the calendar_id is used only
        // when deleting a calendar, which is a rare event.  All new rows
        // will have the correct calendar_id.
        db.execSQL("ALTER TABLE DeletedEvents ADD COLUMN calendar_id INTEGER;");

        // Trigger to remove a calendar's events when we delete the calendar
        db.execSQL("DROP TRIGGER IF EXISTS calendar_cleanup");
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON Calendars " +
                "BEGIN " +
                "DELETE FROM Events WHERE calendar_id = old._id;" +
                "DELETE FROM DeletedEvents WHERE calendar_id = old._id;" +
                "END");
        db.execSQL("DROP TRIGGER IF EXISTS event_to_deleted");
    }

    private void dropTables(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS Calendars;");
        db.execSQL("DROP TABLE IF EXISTS Events;");
        db.execSQL("DROP TABLE IF EXISTS EventsRawTimes;");
        db.execSQL("DROP TABLE IF EXISTS Instances;");
        db.execSQL("DROP TABLE IF EXISTS CalendarMetaData;");
        db.execSQL("DROP TABLE IF EXISTS CalendarCache;");
        db.execSQL("DROP TABLE IF EXISTS Attendees;");
        db.execSQL("DROP TABLE IF EXISTS Reminders;");
        db.execSQL("DROP TABLE IF EXISTS CalendarAlerts;");
        db.execSQL("DROP TABLE IF EXISTS ExtendedProperties;");
    }

    @Override
    public synchronized SQLiteDatabase getWritableDatabase() {
        SQLiteDatabase db = super.getWritableDatabase();
        return db;
    }

    public SyncStateContentProviderHelper getSyncState() {
        return mSyncState;
    }

    /**
     * Schedule a calendar sync for the account.
     * @param account the account for which to schedule a sync
     * @param uploadChangesOnly if set, specify that the sync should only send
     *   up local changes.  This is typically used for a local sync, a user override of
     *   too many deletions, or a sync after a calendar is unselected.
     * @param url the url feed for the calendar to sync (may be null, in which case a poll of
     *   all feeds is done.)
     */
    void scheduleSync(Account account, boolean uploadChangesOnly, String url) {
        Bundle extras = new Bundle();
        if (uploadChangesOnly) {
            extras.putBoolean(ContentResolver.SYNC_EXTRAS_UPLOAD, uploadChangesOnly);
        }
        if (url != null) {
            extras.putString("feed", url);
            extras.putBoolean(ContentResolver.SYNC_EXTRAS_MANUAL, true);
        }
        ContentResolver.requestSync(account, Calendar.Calendars.CONTENT_URI.getAuthority(), extras);
    }

    public interface Views {
      public static final String EVENTS = "view_events";
    }

    public interface Tables {
      public static final String EVENTS = "Events";
      public static final String CALENDARS = "Calendars";
    }

    private static void createEventsView(SQLiteDatabase db) {
        db.execSQL("DROP VIEW IF EXISTS " + Views.EVENTS + ";");
        String eventsSelect = "SELECT "
                + Tables.EVENTS + "." + Calendar.Events._ID + " AS " + Calendar.Events._ID + ","
                + Calendar.Events.HTML_URI + ","
                + Calendar.Events.TITLE + ","
                + Calendar.Events.DESCRIPTION + ","
                + Calendar.Events.EVENT_LOCATION + ","
                + Calendar.Events.STATUS + ","
                + Calendar.Events.SELF_ATTENDEE_STATUS + ","
                + Calendar.Events.COMMENTS_URI + ","
                + Calendar.Events.DTSTART + ","
                + Calendar.Events.DTEND + ","
                + Calendar.Events.DURATION + ","
                + Calendar.Events.EVENT_TIMEZONE + ","
                + Calendar.Events.ALL_DAY + ","
                + Calendar.Events.VISIBILITY + ","
                + Calendar.Events.TIMEZONE + ","
                + Calendar.Events.SELECTED + ","
                + Calendar.Events.ACCESS_LEVEL + ","
                + Calendar.Events.TRANSPARENCY + ","
                + Calendar.Events.COLOR + ","
                + Calendar.Events.HAS_ALARM + ","
                + Calendar.Events.HAS_EXTENDED_PROPERTIES + ","
                + Calendar.Events.RRULE + ","
                + Calendar.Events.RDATE + ","
                + Calendar.Events.EXRULE + ","
                + Calendar.Events.EXDATE + ","
                + Calendar.Events.ORIGINAL_EVENT + ","
                + Calendar.Events.ORIGINAL_INSTANCE_TIME + ","
                + Calendar.Events.ORIGINAL_ALL_DAY + ","
                + Calendar.Events.LAST_DATE + ","
                + Calendar.Events.HAS_ATTENDEE_DATA + ","
                + Calendar.Events.CALENDAR_ID + ","
                + Calendar.Events.GUESTS_CAN_INVITE_OTHERS + ","
                + Calendar.Events.GUESTS_CAN_MODIFY + ","
                + Calendar.Events.GUESTS_CAN_SEE_GUESTS + ","
                + Calendar.Events.ORGANIZER + ","
                + Calendar.Events.DELETED + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_ID
                + " AS " + Calendar.Events._SYNC_ID + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_VERSION
                + " AS " + Calendar.Events._SYNC_VERSION + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_DIRTY
                + " AS " + Calendar.Events._SYNC_DIRTY + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_ACCOUNT
                + " AS " + Calendar.Events._SYNC_ACCOUNT + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_ACCOUNT_TYPE
                + " AS " + Calendar.Events._SYNC_ACCOUNT_TYPE + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_TIME
                + " AS " + Calendar.Events._SYNC_TIME + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_DATA
                + " AS " + Calendar.Events._SYNC_DATA + ","
                + Tables.EVENTS + "." + Calendar.Events._SYNC_MARK
                + " AS " + Calendar.Events._SYNC_MARK + ","
                + Calendar.Calendars.URL + ","
                + Calendar.Calendars.OWNER_ACCOUNT + ","
                + Calendar.Calendars.SYNC_EVENTS
                + " FROM " + Tables.EVENTS + " JOIN " + Tables.CALENDARS
                + " ON (" + Tables.EVENTS + "." + Calendar.Events.CALENDAR_ID
                + "=" + Tables.CALENDARS + "." + Calendar.Calendars._ID
                + ")";

        db.execSQL("CREATE VIEW " + Views.EVENTS + " AS " + eventsSelect);
    }

    /**
     * Extracts the calendar email from a calendar feed url.
     * @param feed the calendar feed url
     * @return the calendar email that is in the feed url or null if it can't
     * find the email address.
     * TODO: this is duplicated in CalendarSyncAdapter; move to a library
     */
    public static String calendarEmailAddressFromFeedUrl(String feed) {
        // Example feed url:
        // https://www.google.com/calendar/feeds/foo%40gmail.com/private/full-noattendees
        String[] pathComponents = feed.split("/");
        if (pathComponents.length > 5 && "feeds".equals(pathComponents[4])) {
            try {
                return URLDecoder.decode(pathComponents[5], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                Log.e(TAG, "unable to url decode the email address in calendar " + feed);
                return null;
            }
        }

        Log.e(TAG, "unable to find the email address in calendar " + feed);
        return null;
    }
}
