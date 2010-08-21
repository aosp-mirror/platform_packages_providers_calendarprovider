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

/**
 * Database helper for calendar. Designed as a singleton to make sure that all
 * {@link android.content.ContentProvider} users get the same reference.
 */
/* package */ class CalendarDatabaseHelper extends SQLiteOpenHelper {

    private static final String TAG = "CalendarDatabaseHelper";

    private static final boolean LOGD = false;

    private static final String DATABASE_NAME = "calendar.db";

    private static final int DAY_IN_SECONDS = 24 * 60 * 60;

    // Note: if you update the version number, you must also update the code
    // in upgradeDatabase() to modify the database (gracefully, if possible).
    static final int DATABASE_VERSION = 202;

    private static final int PRE_FROYO_SYNC_STATE_VERSION = 3;

    public interface Tables {
        public static final String CALENDARS = "Calendars";
        public static final String EVENTS = "Events";
        public static final String EVENTS_RAW_TIMES = "EventsRawTimes";
        public static final String INSTANCES = "Instances";
        public static final String ATTENDEES = "Attendees";
        public static final String REMINDERS = "Reminders";
        public static final String CALENDAR_ALERTS = "CalendarAlerts";
        public static final String EXTENDED_PROPERTIES = "ExtendedProperties";
        public static final String CALENDAR_META_DATA = "CalendarMetaData";
        public static final String CALENDAR_CACHE = "CalendarCache";
        public static final String SYNC_STATE = "_sync_state";
        public static final String SYNC_STATE_META = "_sync_state_metadata";
    }

    public interface Views {
      public static final String EVENTS = "view_events";
    }

    // Copied from SyncStateContentProviderHelper.  Don't really want to make them public there.
    private static final String SYNC_STATE_META_VERSION_COLUMN = "version";

    private static final String AFTER_EVENT_INSERT_SQL =
            "UPDATE " + Tables.EVENTS +
            " SET " + Calendar.Events._SYNC_ACCOUNT + "=" +
            " (SELECT " + Calendar.Calendars._SYNC_ACCOUNT + " FROM " + Tables.CALENDARS +
            "   WHERE " + Tables.CALENDARS + "." + Calendar.Calendars._ID +
                        "=new." + Calendar.Events.CALENDAR_ID + ")," +
            Calendar.SyncColumns._SYNC_ACCOUNT_TYPE + "=" +
            " (SELECT " + Calendar.Calendars._SYNC_ACCOUNT_TYPE + " FROM " + Tables.CALENDARS +
            "   WHERE " + Tables.CALENDARS + "." + Calendar.Calendars._ID +
                        "=new." + Calendar.Events.CALENDAR_ID + ") " +
            "WHERE " + Tables.EVENTS + "." + Calendar.Events._ID +
                    "=new." + Calendar.Events._ID + ";";

    // This needs to be done when all the tables are already created
    private static final String EVENTS_CLEANUP_TRIGGER_SQL =
            "DELETE FROM " + Tables.INSTANCES +
                " WHERE "+ Calendar.Instances.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
            "DELETE FROM " + Tables.EVENTS_RAW_TIMES +
                " WHERE " + Calendar.EventsRawTimes.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
            "DELETE FROM " + Tables.ATTENDEES +
                " WHERE " + Calendar.Attendees.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
            "DELETE FROM " + Tables.REMINDERS +
                " WHERE " + Calendar.Reminders.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
            "DELETE FROM " + Tables.CALENDAR_ALERTS +
                " WHERE " + Calendar.CalendarAlerts.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
            "DELETE FROM " + Tables.EXTENDED_PROPERTIES +
                " WHERE " + Calendar.ExtendedProperties.EVENT_ID + "=" +
                    "old." + Calendar.Events._ID + ";";

    private static final String CALENDAR_CLEANUP_TRIGGER_SQL = "DELETE FROM " + Tables.EVENTS +
            " WHERE " + Calendar.Events.CALENDAR_ID + "=" +
                "old." + Calendar.Events._ID + ";";

    private static final String SCHEMA_HTTPS = "https://";
    private static final String SCHEMA_HTTP = "http://";

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
        if (LOGD) Log.d(TAG, "Creating OpenHelper");
        Resources resources = context.getResources();

        mContext = context;
        mSyncState = new SyncStateContentProviderHelper();
    }

    @Override
    public void onOpen(SQLiteDatabase db) {
        mSyncState.onDatabaseOpened(db);

        mCalendarsInserter = new DatabaseUtils.InsertHelper(db, Tables.CALENDARS);
        mEventsInserter = new DatabaseUtils.InsertHelper(db, Tables.EVENTS);
        mEventsRawTimesInserter = new DatabaseUtils.InsertHelper(db, Tables.EVENTS_RAW_TIMES);
        mInstancesInserter = new DatabaseUtils.InsertHelper(db, Tables.INSTANCES);
        mAttendeesInserter = new DatabaseUtils.InsertHelper(db, Tables.ATTENDEES);
        mRemindersInserter = new DatabaseUtils.InsertHelper(db, Tables.REMINDERS);
        mCalendarAlertsInserter = new DatabaseUtils.InsertHelper(db, Tables.CALENDAR_ALERTS);
        mExtendedPropertiesInserter =
                new DatabaseUtils.InsertHelper(db, Tables.EXTENDED_PROPERTIES);
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
                 + " FROM " + Tables.SYNC_STATE_META,
                 null);
        if (version == PRE_FROYO_SYNC_STATE_VERSION) {
            Log.i(TAG, "Upgrading calendar sync state table");
            db.execSQL("CREATE TEMPORARY TABLE state_backup(_sync_account TEXT, "
                    + "_sync_account_type TEXT, data TEXT);");
            db.execSQL("INSERT INTO state_backup SELECT _sync_account, _sync_account_type, data"
                    + " FROM "
                    + Tables.SYNC_STATE
                    + " WHERE _sync_account is not NULL and _sync_account_type is not NULL;");
            db.execSQL("DROP TABLE " + Tables.SYNC_STATE + ";");
            mSyncState.onDatabaseOpened(db);
            db.execSQL("INSERT INTO " + Tables.SYNC_STATE + "("
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

        createCalendarsTable(db);

        // TODO: do we need both dtend and duration?
        db.execSQL("CREATE TABLE " + Tables.EVENTS + " (" +
                Calendar.Events._ID + " INTEGER PRIMARY KEY," +
                Calendar.Events._SYNC_ACCOUNT + " TEXT," +
                Calendar.Events._SYNC_ACCOUNT_TYPE + " TEXT," +
                Calendar.Events._SYNC_ID + " TEXT," +
                Calendar.Events._SYNC_VERSION + " TEXT," +
                // sync time in UTC
                Calendar.Events._SYNC_TIME + " TEXT," +
                Calendar.Events._SYNC_DATA + " INTEGER," +
                Calendar.Events._SYNC_DIRTY + " INTEGER," +
                // sync mark to filter out new rows
                Calendar.Events._SYNC_MARK + " INTEGER," +
                Calendar.Events.CALENDAR_ID + " INTEGER NOT NULL," +
                Calendar.Events.HTML_URI + " TEXT," +
                Calendar.Events.TITLE + " TEXT," +
                Calendar.Events.EVENT_LOCATION + " TEXT," +
                Calendar.Events.DESCRIPTION + " TEXT," +
                Calendar.Events.STATUS + " INTEGER," +
                Calendar.Events.SELF_ATTENDEE_STATUS + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.COMMENTS_URI + " TEXT," +
                // dtstart in millis since epoch
                Calendar.Events.DTSTART + " INTEGER," +
                // dtend in millis since epoch
                Calendar.Events.DTEND + " INTEGER," +
                // timezone for event
                Calendar.Events.EVENT_TIMEZONE + " TEXT," +
                Calendar.Events.DURATION + " TEXT," +
                Calendar.Events.ALL_DAY + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.VISIBILITY + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.TRANSPARENCY + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.HAS_ALARM + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.HAS_EXTENDED_PROPERTIES + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.RRULE + " TEXT," +
                Calendar.Events.RDATE + " TEXT," +
                Calendar.Events.EXRULE + " TEXT," +
                Calendar.Events.EXDATE + " TEXT," +
                // originalEvent is the _sync_id of recurring event
                Calendar.Events.ORIGINAL_EVENT + " TEXT," +
                // originalInstanceTime is in millis since epoch
                Calendar.Events.ORIGINAL_INSTANCE_TIME + " INTEGER," +
                Calendar.Events.ORIGINAL_ALL_DAY + " INTEGER," +
                // lastDate is in millis since epoch
                Calendar.Events.LAST_DATE + " INTEGER," +
                Calendar.Events.HAS_ATTENDEE_DATA + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.GUESTS_CAN_MODIFY + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Events.GUESTS_CAN_INVITE_OTHERS + " INTEGER NOT NULL DEFAULT 1," +
                Calendar.Events.GUESTS_CAN_SEE_GUESTS + " INTEGER NOT NULL DEFAULT 1," +
                Calendar.Events.ORGANIZER + " STRING," +
                Calendar.Events.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                // dstart2 is in millis since epoch, allDay events are in local timezone
                Calendar.Events.DTSTART2 + " INTEGER," +
                // dtend2 is in millis since epoch, allDay events are in local timezone
                Calendar.Events.DTEND2 + " INTEGER," +
                // timezone for event with allDay events are in local timezone
                Calendar.Events.EVENT_TIMEZONE2 + " TEXT," +
                // syncAdapterData is available for use by sync adapters
                Calendar.Events.SYNC_ADAPTER_DATA + " TEXT" +
                ");");

        // Trigger to set event's sync_account
        db.execSQL("CREATE TRIGGER events_insert AFTER INSERT ON " + Tables.EVENTS + " " +
                "BEGIN " +
                AFTER_EVENT_INSERT_SQL +
                "END");

        db.execSQL("CREATE INDEX eventSyncAccountAndIdIndex ON " + Tables.EVENTS + " (" +
                Calendar.Events._SYNC_ACCOUNT_TYPE + ", " +
                Calendar.Events._SYNC_ACCOUNT + ", " +
                Calendar.Events._SYNC_ID +
                ");");

        db.execSQL("CREATE INDEX eventsCalendarIdIndex ON " + Tables.EVENTS + " (" +
                Calendar.Events.CALENDAR_ID +
                ");");

        db.execSQL("CREATE TABLE " + Tables.EVENTS_RAW_TIMES + " (" +
                Calendar.EventsRawTimes._ID + " INTEGER PRIMARY KEY," +
                Calendar.EventsRawTimes.EVENT_ID + " INTEGER NOT NULL," +
                Calendar.EventsRawTimes.DTSTART_2445 + " TEXT," +
                Calendar.EventsRawTimes.DTEND_2445 + " TEXT," +
                Calendar.EventsRawTimes.ORIGINAL_INSTANCE_TIME_2445 + " TEXT," +
                Calendar.EventsRawTimes.LAST_DATE_2445 + " TEXT," +
                "UNIQUE (" + Calendar.EventsRawTimes.EVENT_ID + ")" +
                ");");

        db.execSQL("CREATE TABLE " + Tables.INSTANCES + " (" +
                Calendar.Instances._ID + " INTEGER PRIMARY KEY," +
                Calendar.Instances.EVENT_ID + " INTEGER," +
                Calendar.Instances.BEGIN + " INTEGER," +         // UTC millis
                Calendar.Instances.END + " INTEGER," +           // UTC millis
                Calendar.Instances.START_DAY + " INTEGER," +      // Julian start day
                Calendar.Instances.END_DAY + " INTEGER," +        // Julian end day
                Calendar.Instances.START_MINUTE + " INTEGER," +   // minutes from midnight
                Calendar.Instances.END_MINUTE + " INTEGER," +     // minutes from midnight
                "UNIQUE (" +
                    Calendar.Instances.EVENT_ID + ", " +
                    Calendar.Instances.BEGIN + ", " +
                    Calendar.Instances.END + ")" +
                ");");

        db.execSQL("CREATE INDEX instancesStartDayIndex ON " + Tables.INSTANCES + " (" +
                Calendar.Instances.START_DAY +
                ");");

        createCalendarMetaDataTable(db);

        createCalendarCacheTable(db);

        db.execSQL("CREATE TABLE " + Tables.ATTENDEES + " (" +
                Calendar.Attendees._ID + " INTEGER PRIMARY KEY," +
                Calendar.Attendees.EVENT_ID + " INTEGER," +
                Calendar.Attendees.ATTENDEE_NAME + " TEXT," +
                Calendar.Attendees.ATTENDEE_EMAIL + " TEXT," +
                Calendar.Attendees.ATTENDEE_STATUS + " INTEGER," +
                Calendar.Attendees.ATTENDEE_RELATIONSHIP + " INTEGER," +
                Calendar.Attendees.ATTENDEE_TYPE + " INTEGER" +
                ");");

        db.execSQL("CREATE INDEX attendeesEventIdIndex ON " + Tables.ATTENDEES + " (" +
                Calendar.Attendees.EVENT_ID +
                ");");

        db.execSQL("CREATE TABLE " + Tables.REMINDERS + " (" +
                Calendar.Reminders._ID + " INTEGER PRIMARY KEY," +
                Calendar.Reminders.EVENT_ID + " INTEGER," +
                Calendar.Reminders.MINUTES + " INTEGER," +
                Calendar.Reminders.METHOD + " INTEGER NOT NULL" +
                " DEFAULT " + Calendar.Reminders.METHOD_DEFAULT +
                ");");

        db.execSQL("CREATE INDEX remindersEventIdIndex ON " + Tables.REMINDERS + " (" +
                Calendar.Reminders.EVENT_ID +
                ");");

         // This table stores the Calendar notifications that have gone off.
        db.execSQL("CREATE TABLE " + Tables.CALENDAR_ALERTS + " (" +
                Calendar.CalendarAlerts._ID + " INTEGER PRIMARY KEY," +
                Calendar.CalendarAlerts.EVENT_ID + " INTEGER," +
                Calendar.CalendarAlerts.BEGIN + " INTEGER NOT NULL," +          // UTC millis
                Calendar.CalendarAlerts.END + " INTEGER NOT NULL," +            // UTC millis
                Calendar.CalendarAlerts.ALARM_TIME + " INTEGER NOT NULL," +     // UTC millis
                Calendar.CalendarAlerts.CREATION_TIME + " INTEGER NOT NULL," +  // UTC millis
                Calendar.CalendarAlerts.RECEIVED_TIME + " INTEGER NOT NULL," +  // UTC millis
                Calendar.CalendarAlerts.NOTIFY_TIME + " INTEGER NOT NULL," +    // UTC millis
                Calendar.CalendarAlerts.STATE + " INTEGER NOT NULL," +
                Calendar.CalendarAlerts.MINUTES + " INTEGER," +
                "UNIQUE (" +
                    Calendar.CalendarAlerts.ALARM_TIME + ", " +
                    Calendar.CalendarAlerts.BEGIN + ", " +
                    Calendar.CalendarAlerts.EVENT_ID + ")" +
                ");");

        db.execSQL("CREATE INDEX calendarAlertsEventIdIndex ON " + Tables.CALENDAR_ALERTS + " (" +
                Calendar.CalendarAlerts.EVENT_ID +
                ");");

        db.execSQL("CREATE TABLE " + Tables.EXTENDED_PROPERTIES + " (" +
                Calendar.ExtendedProperties._ID + " INTEGER PRIMARY KEY," +
                Calendar.ExtendedProperties.EVENT_ID + " INTEGER," +
                Calendar.ExtendedProperties.NAME + " TEXT," +
                Calendar.ExtendedProperties.VALUE + " TEXT" +
                ");");

        db.execSQL("CREATE INDEX extendedPropertiesEventIdIndex ON " + Tables.EXTENDED_PROPERTIES
                + " (" +
                Calendar.ExtendedProperties.EVENT_ID +
                ");");

        createEventsView(db);

        // Trigger to remove data tied to an event when we delete that event.
        db.execSQL("CREATE TRIGGER events_cleanup_delete DELETE ON " + Tables.EVENTS + " " +
                "BEGIN " +
                EVENTS_CLEANUP_TRIGGER_SQL +
                "END");

        ContentResolver.requestSync(null /* all accounts */,
                ContactsContract.AUTHORITY, new Bundle());
    }

    private void createCalendarsTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + Tables.CALENDARS + " (" +
                Calendar.Calendars._ID + " INTEGER PRIMARY KEY," +
                Calendar.Calendars._SYNC_ACCOUNT + " TEXT," +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + " TEXT," +
                Calendar.Calendars._SYNC_ID + " TEXT," +
                Calendar.Calendars._SYNC_VERSION + " TEXT," +
                Calendar.Calendars._SYNC_TIME + " TEXT," +  // UTC
                Calendar.Calendars._SYNC_DATA + " INTEGER," +
                Calendar.Calendars._SYNC_DIRTY + " INTEGER," +
                Calendar.Calendars._SYNC_MARK + " INTEGER," + // Used to filter out new rows
                Calendar.Calendars.NAME + " TEXT," +
                Calendar.Calendars.DISPLAY_NAME + " TEXT," +
                Calendar.Calendars.COLOR + " INTEGER," +
                Calendar.Calendars.ACCESS_LEVEL + " INTEGER," +
                Calendar.Calendars.SELECTED + " INTEGER NOT NULL DEFAULT 1," +
                Calendar.Calendars.SYNC_EVENTS + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Calendars.LOCATION + " TEXT," +
                Calendar.Calendars.TIMEZONE + " TEXT," +
                Calendar.Calendars.OWNER_ACCOUNT + " TEXT, " +
                Calendar.Calendars.ORGANIZER_CAN_RESPOND + " INTEGER NOT NULL DEFAULT 1," +
                Calendar.Calendars.DELETED + " INTEGER NOT NULL DEFAULT 0," +
                Calendar.Calendars.SYNC1 + " TEXT," +
                Calendar.Calendars.SYNC2 + " TEXT," +
                Calendar.Calendars.SYNC3 + " TEXT," +
                Calendar.Calendars.SYNC4 + " TEXT," +
                Calendar.Calendars.SYNC5 + " TEXT" +
                ");");

        // Trigger to remove a calendar's events when we delete the calendar
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON " + Tables.CALENDARS + " " +
                "BEGIN " +
                CALENDAR_CLEANUP_TRIGGER_SQL +
                "END");
    }

    private void createCalendarMetaDataTable(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE " + Tables.CALENDAR_META_DATA + " (" +
                Calendar.CalendarMetaData._ID + " INTEGER PRIMARY KEY," +
                Calendar.CalendarMetaData.LOCAL_TIMEZONE + " TEXT," +
                Calendar.CalendarMetaData.MIN_INSTANCE + " INTEGER," +      // UTC millis
                Calendar.CalendarMetaData.MAX_INSTANCE + " INTEGER" +       // UTC millis
                ");");
    }

    private void createCalendarCacheTable(SQLiteDatabase db) {
        // This is a hack because versioning skipped version number 61 of schema
        // TODO after version 70 this can be removed
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CALENDAR_CACHE + ";");

        // IF NOT EXISTS should be normal pattern for table creation
        db.execSQL("CREATE TABLE IF NOT EXISTS " + Tables.CALENDAR_CACHE + " (" +
                CalendarCache.COLUMN_NAME_ID + " INTEGER PRIMARY KEY," +
                CalendarCache.COLUMN_NAME_KEY + " TEXT NOT NULL," +
                CalendarCache.COLUMN_NAME_VALUE + " TEXT" +
                ");");

        initCalendarCacheTable(db);
    }

    private void initCalendarCacheTable(SQLiteDatabase db) {
        db.execSQL("INSERT INTO " + Tables.CALENDAR_CACHE + " (" +
                CalendarCache.COLUMN_NAME_ID + ", " +
                CalendarCache.COLUMN_NAME_KEY + ", " +
                CalendarCache.COLUMN_NAME_VALUE +
                ") VALUES (" +
                CalendarCache.KEY_TIMEZONE_DATABASE_VERSION.hashCode() + "," +
                "'" + CalendarCache.KEY_TIMEZONE_DATABASE_VERSION + "',"  +
                "'" + CalendarCache.DEFAULT_TIMEZONE_DATABASE_VERSION + "'" +
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
            if (oldVersion == 67 || oldVersion == 68) {
                upgradeToVersion69(db);
                oldVersion = 69;
            }
            // 69. 70 are for Froyo/old Gingerbread only and 100s are for Gingerbread only
            // 70 and 71 have been for Honeycomb but no more used
            // 72 and 73 and 74 were for Honeycomb only but are considered as obsolete for enabling
            // room for Froyo version numbers
            if(oldVersion == 69) {
                upgradeToVersion100(db);
                oldVersion = 100;
            }
            if (oldVersion == 70) {
                upgradeToVersion200(db);
                oldVersion = 200;
            }
            if (oldVersion == 100) {
                upgradeToVersion200(db);
                oldVersion = 200;
            }
            // This is needed for "converting" 72 to 200, 73 to 201 and 74 to 202
            // TODO: This code needs to be deleted later on especially when Froyo starts hitting
            // those numbers
            if (oldVersion == 72) {
                oldVersion = 200;
            }
            if (oldVersion == 73) {
                oldVersion = 201;
            }
            if (oldVersion == 74) {
                oldVersion = 202;
            }
            if (oldVersion == 200) {
                upgradeToVersion201(db);
                oldVersion += 1;
            }
            if (oldVersion == 201) {
                upgradeToVersion202(db);
                oldVersion += 1;
            }
            if (oldVersion != DATABASE_VERSION) {
                Log.e(TAG, "Need to recreate Calendar schema because of "
                        + "unknown Calendar database version: " + oldVersion);
                dropTables(db);
                bootstrapDB(db);
                oldVersion = DATABASE_VERSION;
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
        db.execSQL("DROP TABLE " + Tables.CALENDAR_META_DATA + ";");
        createCalendarMetaDataTable(db);

        // Also clean the Instance table as this table may be corrupted
        db.execSQL("DELETE FROM " + Tables.INSTANCES + ";");
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
    void upgradeToVersion202(SQLiteDatabase db) {
        // We will drop the "hidden" column from the calendar schema and add the "sync5" column
        db.execSQL("ALTER TABLE " + Tables.CALENDARS +" RENAME TO " +
                Tables.CALENDARS + "_Backup;");

        db.execSQL("DROP TRIGGER IF EXISTS calendar_cleanup");
        createCalendarsTable(db);

        // Populate the new Calendars table and put into the "sync5" column the value of the
        // old "hidden" column
        db.execSQL("INSERT INTO " + Tables.CALENDARS + " (" +
                Calendar.Calendars._ID + ", " +
                Calendar.Calendars._SYNC_ACCOUNT + ", " +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + ", " +
                Calendar.Calendars._SYNC_ID + ", " +
                Calendar.Calendars._SYNC_VERSION + ", " +
                Calendar.Calendars._SYNC_TIME + ", " +
                Calendar.Calendars._SYNC_DATA + ", " +
                Calendar.Calendars._SYNC_DIRTY + ", " +
                Calendar.Calendars._SYNC_MARK + ", " +
                Calendar.Calendars.NAME + ", " +
                Calendar.Calendars.DISPLAY_NAME + ", " +
                Calendar.Calendars.COLOR + ", " +
                Calendar.Calendars.ACCESS_LEVEL + ", " +
                Calendar.Calendars.SELECTED + ", " +
                Calendar.Calendars.SYNC_EVENTS + ", " +
                Calendar.Calendars.LOCATION + ", " +
                Calendar.Calendars.TIMEZONE + ", " +
                Calendar.Calendars.OWNER_ACCOUNT + ", " +
                Calendar.Calendars.ORGANIZER_CAN_RESPOND + ", " +
                Calendar.Calendars.DELETED + ", " +
                Calendar.Calendars.SYNC1 + ", " +
                Calendar.Calendars.SYNC2 + ", " +
                Calendar.Calendars.SYNC3 + ", " +
                Calendar.Calendars.SYNC4 + ", " +
                Calendar.Calendars.SYNC5 + ") " +
                "SELECT " +
                Calendar.Calendars._ID + ", " +
                Calendar.Calendars._SYNC_ACCOUNT + ", " +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + ", " +
                Calendar.Calendars._SYNC_ID + ", " +
                Calendar.Calendars._SYNC_VERSION + ", " +
                Calendar.Calendars._SYNC_TIME + ", " +
                Calendar.Calendars._SYNC_DATA + ", " +
                Calendar.Calendars._SYNC_DIRTY + ", " +
                Calendar.Calendars._SYNC_MARK + ", " +
                Calendar.Calendars.NAME + ", " +
                Calendar.Calendars.DISPLAY_NAME + ", " +
                Calendar.Calendars.COLOR + ", " +
                Calendar.Calendars.ACCESS_LEVEL + ", " +
                Calendar.Calendars.SELECTED + ", " +
                Calendar.Calendars.SYNC_EVENTS + ", " +
                Calendar.Calendars.LOCATION + ", " +
                Calendar.Calendars.TIMEZONE + ", " +
                Calendar.Calendars.OWNER_ACCOUNT + ", " +
                Calendar.Calendars.ORGANIZER_CAN_RESPOND + ", " +
                Calendar.Calendars.DELETED + ", " +
                Calendar.Calendars.SYNC1 + ", " +
                Calendar.Calendars.SYNC2 + ", " +
                Calendar.Calendars.SYNC3 + ", " +
                Calendar.Calendars.SYNC4 + " " +
                "hidden" + " " +
                "FROM " + Tables.CALENDARS + "_Backup" + ";"
        );

        // Drop the backup table
        db.execSQL("DROP TABLE " + Tables.CALENDARS + "_Backup;");

        // Recreate the Events Views as column "hidden" has been deleted
        createEventsView(db);
    }

    @VisibleForTesting
    void upgradeToVersion201(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.CALENDARS +
                " ADD COLUMN " + Calendar.Calendars.SYNC4 + " TEXT;");
    }

    @VisibleForTesting
    void upgradeToVersion200(SQLiteDatabase db) {
        // we cannot use here a Calendar.Calendars,URL constant for "url" as we are trying to make
        // it disappear so we are keeping the hardcoded name "url" in all the SQLs
        db.execSQL("ALTER TABLE " + Tables.CALENDARS +" RENAME TO " +
                Tables.CALENDARS + "_Backup;");

        db.execSQL("DROP TRIGGER IF EXISTS calendar_cleanup");
        createCalendarsTable(db);

        // Populate the new Calendars table except the SYNC2 / SYNC3 columns
        db.execSQL("INSERT INTO " + Tables.CALENDARS + " (" +
                Calendar.Calendars._ID + ", " +
                Calendar.Calendars._SYNC_ACCOUNT + ", " +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + ", " +
                Calendar.Calendars._SYNC_ID + ", " +
                Calendar.Calendars._SYNC_VERSION + ", " +
                Calendar.Calendars._SYNC_TIME + ", " +
                Calendar.Calendars._SYNC_DATA + ", " +
                Calendar.Calendars._SYNC_DIRTY + ", " +
                Calendar.Calendars._SYNC_MARK + ", " +
                Calendar.Calendars.NAME + ", " +
                Calendar.Calendars.DISPLAY_NAME + ", " +
                Calendar.Calendars.COLOR + ", " +
                Calendar.Calendars.ACCESS_LEVEL + ", " +
                Calendar.Calendars.SELECTED + ", " +
                Calendar.Calendars.SYNC_EVENTS + ", " +
                Calendar.Calendars.LOCATION + ", " +
                Calendar.Calendars.TIMEZONE + ", " +
                Calendar.Calendars.OWNER_ACCOUNT + ", " +
                Calendar.Calendars.ORGANIZER_CAN_RESPOND + ", " +
                Calendar.Calendars.DELETED + ", " +
                Calendar.Calendars.SYNC1 + ") " +
                "SELECT " +
                Calendar.Calendars._ID + ", " +
                Calendar.Calendars._SYNC_ACCOUNT + ", " +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + ", " +
                Calendar.Calendars._SYNC_ID + ", " +
                Calendar.Calendars._SYNC_VERSION + ", " +
                Calendar.Calendars._SYNC_TIME + ", " +
                Calendar.Calendars._SYNC_DATA + ", " +
                Calendar.Calendars._SYNC_DIRTY + ", " +
                Calendar.Calendars._SYNC_MARK + ", " +
                Calendar.Calendars.NAME + ", " +
                Calendar.Calendars.DISPLAY_NAME + ", " +
                Calendar.Calendars.COLOR + ", " +
                Calendar.Calendars.ACCESS_LEVEL + ", " +
                Calendar.Calendars.SELECTED + ", " +
                Calendar.Calendars.SYNC_EVENTS + ", " +
                Calendar.Calendars.LOCATION + ", " +
                Calendar.Calendars.TIMEZONE + ", " +
                Calendar.Calendars.OWNER_ACCOUNT + ", " +
                Calendar.Calendars.ORGANIZER_CAN_RESPOND + ", " +
                "0" + ", " +
                "url" + " " +
                "FROM " + Tables.CALENDARS + "_Backup" + ";"
        );

        // Populate SYNC2 and SYNC3 columns - SYNC1 represent the old "url" column
        // We will need to iterate over all the "com.google" type of calendars
        String selectSql = "SELECT " + Calendar.Calendars._ID + ", " + "url" +
                " FROM " + Tables.CALENDARS + "_Backup" +
                " WHERE " + Calendar.Calendars._SYNC_ACCOUNT_TYPE  + "='com.google'" +
                " AND url IS NOT NULL;";

        String updateSql = "UPDATE " + Tables.CALENDARS + " SET " +
                Calendar.Calendars.SYNC2 + "=?, " + // edit Url
                Calendar.Calendars.SYNC3 + "=? " + // self Url
                "WHERE " + Calendar.Calendars._ID + "=?;";

        Cursor cursor = db.rawQuery(selectSql, null /* selection args */);
        if (cursor != null && cursor.getCount() > 0) {
            try {
                Object[] bindArgs = new Object[3];

                while (cursor.moveToNext()) {
                    Long id = cursor.getLong(0);
                    String url = cursor.getString(1);
                    String selfUrl = getSelfUrlFromEventsUrl(url);
                    String editUrl = getEditUrlFromEventsUrl(url);

                    bindArgs[0] = editUrl;
                    bindArgs[1] = selfUrl;
                    bindArgs[2] = id;

                    db.execSQL(updateSql, bindArgs);
                }
            } finally {
                cursor.close();
            }
        }

        // Drop the backup table
        db.execSQL("DROP TABLE " + Tables.CALENDARS + "_Backup;");

        // Recreate the Events Views as column "deleted" is now ambiguous
        // ("deleted" is now defined in both Calendars and Events tables)
        createEventsView(db);
    }

    @VisibleForTesting
    void upgradeToVersion100(SQLiteDatabase db) {
        createCalendarCacheTable(db);
    }

    @VisibleForTesting
    static void upgradeToVersion69(SQLiteDatabase db) {
        // Clean up allDay events which could be in an invalid state from an earlier version
        // Some allDay events had hour, min, sec not set to zero, which throws elsewhere. This
        // will go through the allDay events and make sure they have proper values and are in the
        // correct timezone. Verifies that dtstart and dtend are in UTC and at midnight, that
        // eventTimezone is set to UTC, tries to make sure duration is in days, and that dtstart2
        // and dtend2 are at midnight in their timezone.
        final String sql = "SELECT " + Calendar.Events._ID + ", " +
                Calendar.Events.DTSTART + ", " +
                Calendar.Events.DTEND + ", " +
                Calendar.Events.DURATION + ", " +
                Calendar.Events.DTSTART2 + ", " +
                Calendar.Events.DTEND2 + ", " +
                Calendar.Events.EVENT_TIMEZONE + ", " +
                Calendar.Events.EVENT_TIMEZONE2 + ", " +
                Calendar.Events.RRULE + " " +
                "FROM " + Tables.EVENTS + " " +
                "WHERE " + Calendar.Events.ALL_DAY + "=?";
        Cursor cursor = db.rawQuery(sql, new String[] {"1"});
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
                            db.execSQL("UPDATE " + Tables.EVENTS + " SET " +
                                    Calendar.Events.DTSTART + "=?, " +
                                    Calendar.Events.DTEND + "=?, " +
                                    Calendar.Events.DTSTART2 + "=?, " +
                                    Calendar.Events.DTEND2 + "=?, " +
                                    Calendar.Events.DURATION + "=?, " +
                                    Calendar.Events.EVENT_TIMEZONE + "=?, " +
                                    Calendar.Events.EVENT_TIMEZONE2 + "=? " +
                                    "WHERE " + Calendar.Events._ID + "=?",
                                    new Object[] {
                                            dtstart,
                                            dtend,
                                            dtstart2,
                                            dtend2,
                                            null,
                                            timezone,
                                            timezone2,
                                            id}
                            );
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
                            db.execSQL("UPDATE " + Tables.EVENTS + " SET " +
                                    Calendar.Events.DTSTART + "=?, " +
                                    Calendar.Events.DTEND + "=?, " +
                                    Calendar.Events.DTSTART2 + "=?, " +
                                    Calendar.Events.DTEND2 + "=?, " +
                                    Calendar.Events.DURATION + "=?," +
                                    Calendar.Events.EVENT_TIMEZONE + "=?, " +
                                    Calendar.Events.EVENT_TIMEZONE2 + "=? " +
                                    "WHERE " + Calendar.Events._ID + "=?",
                                    new Object[] {
                                            dtstart,
                                            null,
                                            dtstart2,
                                            null,
                                            duration,
                                            timezone,
                                            timezone2,
                                            id}
                            );
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
        db.execSQL("ALTER TABLE " + Tables.CALENDARS +
                " ADD COLUMN " + Calendar.Calendars.ORGANIZER_CAN_RESPOND +
                    " INTEGER NOT NULL DEFAULT 1;");
    }

    private void upgradeToVersion65(SQLiteDatabase db) {
        // we need to recreate the Events view
        createEventsView(db);
    }

    private void upgradeToVersion64(SQLiteDatabase db) {
        // Add a column that may be used by sync adapters
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.SYNC_ADAPTER_DATA + " TEXT;");
    }

    private void upgradeToVersion63(SQLiteDatabase db) {
        // we need to recreate the Events view
        createEventsView(db);
    }

    private void upgradeToVersion62(SQLiteDatabase db) {
        // New columns are to transition to having allDay events in the local timezone
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.DTSTART2 + " INTEGER;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.DTEND2 + " INTEGER;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.EVENT_TIMEZONE2 + " TEXT;");

        String[] allDayBit = new String[] {"0"};
        // Copy over all the data that isn't an all day event.
        db.execSQL("UPDATE " + Tables.EVENTS + " SET " +
                Calendar.Events.DTSTART2 + "=" + Calendar.Events.DTSTART + "," +
                Calendar.Events.DTEND2 + "=" + Calendar.Events.DTEND + "," +
                Calendar.Events.EVENT_TIMEZONE2 + "=" + Calendar.Events.EVENT_TIMEZONE + " " +
                "WHERE " + Calendar.Events.ALL_DAY + "=?;",
                allDayBit /* selection args */);

        // "cursor" iterates over all the calendars
        allDayBit[0] = "1";
        Cursor cursor = db.rawQuery("SELECT " + Tables.EVENTS + "." + Calendar.Events._ID + "," +
                Calendar.Events.DTSTART + "," +
                Calendar.Events.DTEND + "," +
                Calendar.Events.EVENT_TIMEZONE + "," +
                Calendar.Calendars.TIMEZONE + " " +
                "FROM " + Tables.EVENTS + " INNER JOIN " + Tables.CALENDARS + " " +
                "WHERE " + Tables.EVENTS + "." + Calendar.Events.CALENDAR_ID + "=" +
                    Tables.CALENDARS + "." + Calendar.Calendars._ID +
                " AND "
                    + Calendar.Events.ALL_DAY + "=?",
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
                    db.execSQL("UPDATE " + Tables.EVENTS + " SET " +
                            Calendar.Events.DTSTART2 + "=?, " +
                            Calendar.Events.DTEND2 + "=?, " +
                            Calendar.Events.EVENT_TIMEZONE2 + "=? " +
                            "WHERE " + Calendar.Events._ID + "=?",
                            newData);
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeToVersion61(SQLiteDatabase db) {
        createCalendarCacheTable(db);
    }

    private void upgradeToVersion60(SQLiteDatabase db) {
        // Switch to CalendarProvider2
        upgradeSyncState(db);
        db.execSQL("DROP TRIGGER IF EXISTS calendar_cleanup");
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON " + Tables.CALENDARS + " " +
                "BEGIN " +
                CALENDAR_CLEANUP_TRIGGER_SQL +
                "END");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.DELETED + " INTEGER NOT NULL DEFAULT 0;");
        db.execSQL("DROP TRIGGER IF EXISTS events_insert");
        // Trigger to set event's sync_account
        db.execSQL("CREATE TRIGGER events_insert AFTER INSERT ON " + Tables.EVENTS + " " +
                "BEGIN " +
                AFTER_EVENT_INSERT_SQL +
                "END");
        db.execSQL("DROP TABLE IF EXISTS DeletedEvents;");
        db.execSQL("DROP TRIGGER IF EXISTS events_cleanup_delete");
        // Trigger to remove data tied to an event when we delete that event.
        db.execSQL("CREATE TRIGGER events_cleanup_delete DELETE ON " + Tables.EVENTS + " " +
                "BEGIN " +
                EVENTS_CLEANUP_TRIGGER_SQL +
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
        db.execSQL("CREATE TEMPORARY TABLE " + Tables.CALENDAR_META_DATA + "_Backup" + "(" +
                Calendar.CalendarMetaData._ID + "," +
                Calendar.CalendarMetaData.LOCAL_TIMEZONE + "," +
                Calendar.CalendarMetaData.MIN_INSTANCE + "," +
                Calendar.CalendarMetaData.MAX_INSTANCE +
                ");");
        db.execSQL("INSERT INTO " + Tables.CALENDAR_META_DATA + "_Backup " +
                "SELECT " +
                Calendar.CalendarMetaData._ID + "," +
                Calendar.CalendarMetaData.LOCAL_TIMEZONE + "," +
                Calendar.CalendarMetaData.MIN_INSTANCE + "," +
                Calendar.CalendarMetaData.MAX_INSTANCE +
                " FROM " + Tables.CALENDAR_META_DATA + ";");
        db.execSQL("DROP TABLE " + Tables.CALENDAR_META_DATA + ";");
        createCalendarMetaDataTable(db);
        db.execSQL("INSERT INTO " + Tables.CALENDAR_META_DATA + " " +
                "SELECT " +
                Calendar.CalendarMetaData._ID + "," +
                Calendar.CalendarMetaData.LOCAL_TIMEZONE + "," +
                Calendar.CalendarMetaData.MIN_INSTANCE + "," +
                Calendar.CalendarMetaData.MAX_INSTANCE +
                " FROM " + Tables.CALENDAR_META_DATA + "_Backup;");
        db.execSQL("DROP TABLE " + Tables.CALENDAR_META_DATA + "_Backup;");
    }

    private void upgradeToVersion57(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.GUESTS_CAN_MODIFY +
                " INTEGER NOT NULL DEFAULT 0;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.GUESTS_CAN_INVITE_OTHERS +
                " INTEGER NOT NULL DEFAULT 1;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.GUESTS_CAN_SEE_GUESTS +
                " INTEGER NOT NULL DEFAULT 1;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.ORGANIZER +
                " STRING;");
        db.execSQL("UPDATE " + Tables.EVENTS + " SET " + Calendar.Events.ORGANIZER + "=" +
                "(SELECT " + Calendar.Attendees.ATTENDEE_EMAIL +
                " FROM " + Tables.ATTENDEES + ""  +
                " WHERE " +
                Tables.ATTENDEES + "." + Calendar.Attendees.EVENT_ID + "=" +
                Tables.EVENTS + "." + Calendar.Events._ID +
                " AND " +
                Tables.ATTENDEES + "." + Calendar.Attendees.ATTENDEE_RELATIONSHIP + "=2);");
    }

    private void upgradeToVersion56(SQLiteDatabase db) {
        db.execSQL("ALTER TABLE " + Tables.CALENDARS +
                " ADD COLUMN " + Calendar.Calendars.OWNER_ACCOUNT + " TEXT;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS +
                " ADD COLUMN " + Calendar.Events.HAS_ATTENDEE_DATA + " INTEGER;");

        // Clear _sync_dirty to avoid a client-to-server sync that could blow away
        // server attendees.
        // Clear _sync_version to pull down the server's event (with attendees)
        // Change the URLs from full-selfattendance to full
        db.execSQL("UPDATE " + Tables.EVENTS
                + " SET " + Calendar.Events._SYNC_DIRTY + "=0, "
                + Calendar.Events._SYNC_VERSION + "=NULL, "
                + Calendar.Events._SYNC_ID + "="
                + "REPLACE(" + Calendar.Events._SYNC_ID + ", " +
                    "'/private/full-selfattendance', '/private/full'),"
                + Calendar.Events.COMMENTS_URI + "="
                + "REPLACE(" + Calendar.Events.COMMENTS_URI + ", " +
                    "'/private/full-selfattendance', '/private/full');");

        db.execSQL("UPDATE " + Tables.CALENDARS
                + " SET " + "url="
                + "REPLACE(" + "url, " + "'/private/full-selfattendance', '/private/full');");

        // "cursor" iterates over all the calendars
        Cursor cursor = db.rawQuery("SELECT " + Calendar.Calendars._ID + ", " +
                Calendar.Calendars.URL + " FROM " + Tables.CALENDARS,
                null /* selection args */);
        // Add the owner column.
        if (cursor != null) {
            try {
                final String updateSql = "UPDATE " + Tables.CALENDARS +
                        " SET " + Calendar.Calendars.OWNER_ACCOUNT + "=?" +
                        " WHERE " + Calendar.Calendars._ID + "=?";
                while (cursor.moveToNext()) {
                    Long id = cursor.getLong(0);
                    String url = cursor.getString(1);
                    String owner = calendarEmailAddressFromFeedUrl(url);
                    db.execSQL(updateSql, new Object[] {owner, id});
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void upgradeResync(SQLiteDatabase db) {
        // Delete sync state, so all records will be re-synced.
        db.execSQL("DELETE FROM " + Tables.SYNC_STATE + ";");

        // "cursor" iterates over all the calendars
        Cursor cursor = db.rawQuery("SELECT " + Calendar.Calendars._SYNC_ACCOUNT + "," +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + ",url FROM " + Tables.CALENDARS,
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
        db.execSQL("ALTER TABLE " + Tables.CALENDARS + " ADD COLUMN " +
                Calendar.Calendars._SYNC_ACCOUNT_TYPE + " TEXT;");
        db.execSQL("ALTER TABLE " + Tables.EVENTS + " ADD COLUMN " +
                Calendar.Events._SYNC_ACCOUNT_TYPE + " TEXT;");
        db.execSQL("ALTER TABLE DeletedEvents ADD COLUMN _sync_account_type TEXT;");
        db.execSQL("UPDATE " + Tables.CALENDARS
                + " SET " + Calendar.Calendars._SYNC_ACCOUNT_TYPE + "='com.google'"
                + " WHERE " + Calendar.Calendars._SYNC_ACCOUNT + " IS NOT NULL");
        db.execSQL("UPDATE " + Tables.EVENTS
                + " SET " + Calendar.Events._SYNC_ACCOUNT_TYPE + "='com.google'"
                + " WHERE " + Calendar.Events._SYNC_ACCOUNT + " IS NOT NULL");
        db.execSQL("UPDATE DeletedEvents"
                + " SET _sync_account_type='com.google'"
                + " WHERE _sync_account IS NOT NULL");
        Log.w(TAG, "re-creating eventSyncAccountAndIdIndex");
        db.execSQL("DROP INDEX eventSyncAccountAndIdIndex");
        db.execSQL("CREATE INDEX eventSyncAccountAndIdIndex ON " + Tables.EVENTS + " ("
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
        db.execSQL("ALTER TABLE " + Tables.CALENDAR_ALERTS + " ADD COLUMN " +
                Calendar.CalendarAlerts.CREATION_TIME + " INTEGER DEFAULT 0;");
        db.execSQL("ALTER TABLE " + Tables.CALENDAR_ALERTS + " ADD COLUMN " +
                Calendar.CalendarAlerts.RECEIVED_TIME + " INTEGER DEFAULT 0;");
        db.execSQL("ALTER TABLE " + Tables.CALENDAR_ALERTS + " ADD COLUMN " +
                Calendar.CalendarAlerts.NOTIFY_TIME + " INTEGER DEFAULT 0;");
    }

    private void upgradeToVersion52(SQLiteDatabase db) {
        // We added "originalAllDay" to the Events table to keep track of
        // the allDay status of the original recurring event for entries
        // that are exceptions to that recurring event.  We need this so
        // that we can format the date correctly for the "originalInstanceTime"
        // column when we make a change to the recurrence exception and
        // send it to the server.
        db.execSQL("ALTER TABLE " + Tables.EVENTS + " ADD COLUMN " +
                Calendar.Events.ORIGINAL_ALL_DAY + " INTEGER;");

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
        Cursor cursor = db.rawQuery("SELECT " + Calendar.Events._ID + "," +
                Calendar.Events.ORIGINAL_EVENT +
                " FROM " + Tables.EVENTS +
                " WHERE " + Calendar.Events.ORIGINAL_EVENT + " IS NOT NULL",
                null /* selection args */);
        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    long id = cursor.getLong(0);
                    String originalEvent = cursor.getString(1);

                    // Find the original recurring event (if it exists)
                    Cursor recur = db.rawQuery("SELECT " + Calendar.Events.ALL_DAY +
                            " FROM " + Tables.EVENTS +
                            " WHERE " + Calendar.Events._SYNC_ID + "=?",
                            new String[] {originalEvent});
                    if (recur == null) {
                        continue;
                    }

                    try {
                        // Fill in the "originalAllDay" field of the
                        // recurrence exception with the "allDay" value
                        // from the recurring event.
                        if (recur.moveToNext()) {
                            int allDay = recur.getInt(0);
                            db.execSQL("UPDATE " + Tables.EVENTS +
                                    " SET " + Calendar.Events.ORIGINAL_ALL_DAY + "=" + allDay +
                                    " WHERE " + Calendar.Events._ID + "="+id);
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
        db.execSQL("CREATE TRIGGER calendar_cleanup DELETE ON " + Tables.CALENDARS + " " +
                "BEGIN " +
                "DELETE FROM " + Tables.EVENTS + " WHERE " + Calendar.Events.CALENDAR_ID + "=" +
                    "old." + Calendar.Events._ID + ";" +
                "DELETE FROM DeletedEvents WHERE calendar_id = old._id;" +
                "END");
        db.execSQL("DROP TRIGGER IF EXISTS event_to_deleted");
    }

    private void dropTables(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CALENDARS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.EVENTS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.EVENTS_RAW_TIMES + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.INSTANCES + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CALENDAR_META_DATA + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CALENDAR_CACHE + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.ATTENDEES + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.REMINDERS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.CALENDAR_ALERTS + ";");
        db.execSQL("DROP TABLE IF EXISTS " + Tables.EXTENDED_PROPERTIES + ";");
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
                + Calendar.Calendars.TIMEZONE + ","
                + Calendar.Calendars.SELECTED + ","
                + Calendar.Calendars.ACCESS_LEVEL + ","
                + Calendar.Events.TRANSPARENCY + ","
                + Calendar.Calendars.COLOR + ","
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
                + Tables.EVENTS + "." + Calendar.Events.DELETED
                + " AS " + Calendar.Events.DELETED + ","
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
                + Calendar.Calendars.SYNC1 + ","
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

    /**
     * Get a "allcalendars" url from a "private/full" or "private/free-busy" url
     * @param url
     * @return the rewritten Url
     *
     * For example:
     *
     *      http://www.google.com/calendar/feeds/joe%40joe.com/private/full
     *      http://www.google.com/calendar/feeds/joe%40joe.com/private/free-busy
     *
     * will be rewriten into:
     *
     *      http://www.google.com/calendar/feeds/default/allcalendars/full/joe%40joe.com
     *      http://www.google.com/calendar/feeds/default/allcalendars/full/joe%40joe.com
     */
    @VisibleForTesting
    private static String getAllCalendarsUrlFromEventsUrl(String url) {
        if (url == null) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Cannot get AllCalendars url from a NULL url");
            }
            return null;
        }
        if (url.contains("/private/full")) {
            return url.replace("/private/full", "").
                    replace("/calendar/feeds", "/calendar/feeds/default/allcalendars/full");
        }
        if (url.contains("/private/free-busy")) {
            return url.replace("/private/free-busy", "").
                    replace("/calendar/feeds", "/calendar/feeds/default/allcalendars/full");
        }
        // Just log as we dont recognize the provided Url
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Log.d(TAG, "Cannot get AllCalendars url from the following url: " + url);
        }
        return null;
    }

    /**
     * Get "selfUrl" from "events url"
     * @param url the Events url (either "private/full" or "private/free-busy"
     * @return the corresponding allcalendar url
     */
    private static String getSelfUrlFromEventsUrl(String url) {
        return rewriteUrlFromHttpToHttps(getAllCalendarsUrlFromEventsUrl(url));
    }

    /**
     * Get "editUrl" from "events url"
     * @param url the Events url (either "private/full" or "private/free-busy"
     * @return the corresponding allcalendar url
     */
    private static String getEditUrlFromEventsUrl(String url) {
        return rewriteUrlFromHttpToHttps(getAllCalendarsUrlFromEventsUrl(url));
    }

    /**
     * Rewrite the url from "http" to "https" scheme
     * @param url the url to rewrite
     * @return the rewritten URL
     */
    private static String rewriteUrlFromHttpToHttps(String url) {
        if (url == null) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Cannot rewrite a NULL url");
            }
            return null;
        }
        if (url.startsWith(SCHEMA_HTTPS)) {
            return url;
        }
        if (!url.startsWith(SCHEMA_HTTP)) {
            throw new IllegalArgumentException("invalid url parameter, unknown scheme: " + url);
        }
        return SCHEMA_HTTPS + url.substring(SCHEMA_HTTP.length());
    }
}
