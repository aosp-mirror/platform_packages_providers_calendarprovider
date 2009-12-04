/*
**
** Copyright 2006, The Android Open Source Project
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** See the License for the specific language governing permissions and
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** limitations under the License.
*/

package com.android.providers.calendar;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.OnAccountsUpdateListener;
import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Entity;
import android.content.EntityIterator;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Debug;
import android.os.Process;
import android.os.RemoteException;
import android.pim.DateException;
import android.pim.RecurrenceSet;
import android.provider.BaseColumns;
import android.provider.Calendar;
import android.provider.Calendar.Attendees;
import android.provider.Calendar.BusyBits;
import android.provider.Calendar.CalendarAlerts;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Events;
import android.provider.Calendar.Instances;
import android.provider.Calendar.Reminders;
import android.text.TextUtils;
import android.text.format.Time;
import android.util.Config;
import android.util.Log;
import android.util.TimeFormatException;
import com.android.internal.content.SyncStateContentProviderHelper;
import com.google.android.collect.Maps;
import com.google.android.collect.Sets;
import com.google.wireless.gdata.calendar.client.CalendarClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Calendar content provider. The contract between this provider and applications
 * is defined in {@link android.provider.Calendar}.
 */
public class CalendarProvider2 extends SQLiteContentProvider implements OnAccountsUpdateListener {

    private static final String TAG = "CalendarProvider2";

    private static final boolean VERBOSE_LOGGING = Log.isLoggable(TAG, Log.VERBOSE) || true;

    private static final boolean PROFILE = false;
    private static final boolean MULTIPLE_ATTENDEES_PER_EVENT = true;
    private static final String[] ACCOUNTS_PROJECTION =
            new String[] {Calendars._SYNC_ACCOUNT, Calendars._SYNC_ACCOUNT_TYPE};

    private static final String[] EVENTS_PROJECTION = new String[] {
            Events._SYNC_ID,
            Events.RRULE,
            Events.RDATE,
            Events.ORIGINAL_EVENT,
    };
    private static final int EVENTS_SYNC_ID_INDEX = 0;
    private static final int EVENTS_RRULE_INDEX = 1;
    private static final int EVENTS_RDATE_INDEX = 2;
    private static final int EVENTS_ORIGINAL_EVENT_INDEX = 3;

    private static final String[] ID_PROJECTION = new String[] {
            Attendees._ID,
            Attendees.EVENT_ID, // Assume these are the same for each table
    };
    private static final int ID_INDEX = 0;
    private static final int EVENT_ID_INDEX = 1;

    /**
     * The cached copy of the CalendarMetaData database table.
     * Make this "package private" instead of "private" so that test code
     * can access it.
     */
    MetaData mMetaData;

    private CalendarDatabaseHelper mDbHelper;

    private static final Uri SYNCSTATE_CONTENT_URI =
            Uri.parse("content://syncstate/state");

    // The interval in minutes for calculating busy bits
    private static final int BUSYBIT_INTERVAL = 60;

    // A lookup table for getting a bit mask of length N, for N <= 32
    // For example, BIT_MASKS[4] gives 0xf (which has 4 bits set to 1).
    // We use this for computing the busy bits for events.
    private static final int[] BIT_MASKS = {
            0,
            0x00000001, 0x00000003, 0x00000007, 0x0000000f,
            0x0000001f, 0x0000003f, 0x0000007f, 0x000000ff,
            0x000001ff, 0x000003ff, 0x000007ff, 0x00000fff,
            0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff,
            0x0001ffff, 0x0003ffff, 0x0007ffff, 0x000fffff,
            0x001fffff, 0x003fffff, 0x007fffff, 0x00ffffff,
            0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff,
            0x1fffffff, 0x3fffffff, 0x7fffffff, 0xffffffff,
    };

    // To determine if a recurrence exception originally overlapped the
    // window, we need to assume a maximum duration, since we only know
    // the original start time.
    private static final int MAX_ASSUMED_DURATION = 7*24*60*60*1000;

    public static final class TimeRange {
        public long begin;
        public long end;
        public boolean allDay;
    }

    public static final class InstancesRange {
        public long begin;
        public long end;

        public InstancesRange(long begin, long end) {
            this.begin = begin;
            this.end = end;
        }
    }

    public static final class InstancesList
            extends ArrayList<ContentValues> {
    }

    public static final class EventInstancesMap
            extends HashMap<String, InstancesList> {
        public void add(String syncId, ContentValues values) {
            InstancesList instances = get(syncId);
            if (instances == null) {
                instances = new InstancesList();
                put(syncId, instances);
            }
            instances.add(values);
        }
    }

    // A thread that runs in the background and schedules the next
    // calendar event alarm.
    private class AlarmScheduler extends Thread {
        boolean mRemoveAlarms;

        public AlarmScheduler(boolean removeAlarms) {
            mRemoveAlarms = removeAlarms;
        }

        public void run() {
            try {
                Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
                runScheduleNextAlarm(mRemoveAlarms);
            } catch (SQLException e) {
                Log.e(TAG, "runScheduleNextAlarm() failed", e);
            }
        }
    }

    /**
     * We search backward in time for event reminders that we may have missed
     * and schedule them if the event has not yet expired.  The amount in
     * the past to search backwards is controlled by this constant.  It
     * should be at least a few minutes to allow for an event that was
     * recently created on the web to make its way to the phone.  Two hours
     * might seem like overkill, but it is useful in the case where the user
     * just crossed into a new timezone and might have just missed an alarm.
     */
    private static final long SCHEDULE_ALARM_SLACK =
        2 * android.text.format.DateUtils.HOUR_IN_MILLIS;

    /**
     * Alarms older than this threshold will be deleted from the CalendarAlerts
     * table.  This should be at least a day because if the timezone is
     * wrong and the user corrects it we might delete good alarms that
     * appear to be old because the device time was incorrectly in the future.
     * This threshold must also be larger than SCHEDULE_ALARM_SLACK.  We add
     * the SCHEDULE_ALARM_SLACK to ensure this.
     *
     * To make it easier to find and debug problems with missed reminders,
     * set this to something greater than a day.
     */
    private static final long CLEAR_OLD_ALARM_THRESHOLD =
            7 * android.text.format.DateUtils.DAY_IN_MILLIS + SCHEDULE_ALARM_SLACK;

    // A lock for synchronizing access to fields that are shared
    // with the AlarmScheduler thread.
    private Object mAlarmLock = new Object();

    // Make sure we load at least two months worth of data.
    // Client apps can load more data in a background thread.
    private static final long MINIMUM_EXPANSION_SPAN =
            2L * 31 * 24 * 60 * 60 * 1000;

    private static final String[] sCalendarsIdProjection = new String[] { Calendars._ID };
    private static final int CALENDARS_INDEX_ID = 0;

    // Allocate the string constant once here instead of on the heap
    private static final String CALENDAR_ID_SELECTION = "calendar_id=?";

    private static final String[] sInstancesProjection =
            new String[] { Instances.START_DAY, Instances.END_DAY,
                    Instances.START_MINUTE, Instances.END_MINUTE, Instances.ALL_DAY };

    private static final int INSTANCES_INDEX_START_DAY = 0;
    private static final int INSTANCES_INDEX_END_DAY = 1;
    private static final int INSTANCES_INDEX_START_MINUTE = 2;
    private static final int INSTANCES_INDEX_END_MINUTE = 3;
    private static final int INSTANCES_INDEX_ALL_DAY = 4;

    private static final String[] sBusyBitProjection = new String[] {
            BusyBits.DAY, BusyBits.BUSYBITS, BusyBits.ALL_DAY_COUNT };

    private static final int BUSYBIT_INDEX_DAY = 0;
    private static final int BUSYBIT_INDEX_BUSYBITS= 1;
    private static final int BUSYBIT_INDEX_ALL_DAY_COUNT = 2;

    private CalendarClient mCalendarClient = null;

    private AlarmManager mAlarmManager;

    private CalendarAppWidgetProvider mAppWidgetProvider = CalendarAppWidgetProvider.getInstance();

    /**
     * Listens for timezone changes and disk-no-longer-full events
     */
    private BroadcastReceiver mIntentReceiver = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String action = intent.getAction();
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "onReceive() " + action);
            }
            if (Intent.ACTION_TIMEZONE_CHANGED.equals(action)) {
                updateTimezoneDependentFields();
                scheduleNextAlarm(false /* do not remove alarms */);
            } else if (Intent.ACTION_DEVICE_STORAGE_OK.equals(action)) {
                // Try to clean up if things were screwy due to a full disk
                updateTimezoneDependentFields();
                scheduleNextAlarm(false /* do not remove alarms */);
            } else if (Intent.ACTION_TIME_CHANGED.equals(action)) {
                scheduleNextAlarm(false /* do not remove alarms */);
            }
        }
    };

    protected void verifyAccounts() {
        AccountManager.get(getContext()).addOnAccountsUpdatedListener(this, null, false);
        onAccountsUpdated(AccountManager.get(getContext()).getAccounts());
    }

    /* Visible for testing */
    @Override
    protected CalendarDatabaseHelper getDatabaseHelper(final Context context) {
        return CalendarDatabaseHelper.getInstance(context);
    }

    @Override
    public boolean onCreate() {
        super.onCreate();
        mDbHelper = (CalendarDatabaseHelper)getDatabaseHelper();

        verifyAccounts();

        // Register for Intent broadcasts
        IntentFilter filter = new IntentFilter();

        filter.addAction(Intent.ACTION_TIMEZONE_CHANGED);
        filter.addAction(Intent.ACTION_DEVICE_STORAGE_OK);
        filter.addAction(Intent.ACTION_TIME_CHANGED);
        final Context c = getContext();

        // We don't ever unregister this because this thread always wants
        // to receive notifications, even in the background.  And if this
        // thread is killed then the whole process will be killed and the
        // memory resources will be reclaimed.
        c.registerReceiver(mIntentReceiver, filter);

        mMetaData = new MetaData(mDbHelper);
        updateTimezoneDependentFields();

        return true;
    }

    /**
     * This creates a background thread to check the timezone and update
     * the timezone dependent fields in the Instances table if the timezone
     * has changes.
     */
    protected void updateTimezoneDependentFields() {
        Thread thread = new TimezoneCheckerThread();
        thread.start();
    }

    private class TimezoneCheckerThread extends Thread {
        @Override
        public void run() {
            Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
            try {
                doUpdateTimezoneDependentFields();
            } catch (SQLException e) {
                Log.e(TAG, "doUpdateTimezoneDependentFields() failed", e);
                try {
                    // Clear at least the in-memory data (and if possible the
                    // database fields) to force a re-computation of Instances.
                    mMetaData.clearInstanceRange();
                } catch (SQLException e2) {
                    Log.e(TAG, "clearInstanceRange() also failed: " + e2);
                }
            }
        }
    }

    /**
     * This method runs in a background thread.  If the timezone has changed
     * then the Instances table will be regenerated.
     */
    private void doUpdateTimezoneDependentFields() {
        MetaData.Fields fields = mMetaData.getFields();
        String localTimezone = TimeZone.getDefault().getID();
        if (TextUtils.equals(fields.timezone, localTimezone)) {
            // Even if the timezone hasn't changed, check for missed alarms.
            // This code executes when the CalendarProvider2 is created and
            // helps to catch missed alarms when the Calendar process is
            // killed (because of low-memory conditions) and then restarted.
            rescheduleMissedAlarms();
            return;
        }

        // The database timezone is different from the current timezone.
        // Regenerate the Instances table for this month.  Include events
        // starting at the beginning of this month.
        long now = System.currentTimeMillis();
        Time time = new Time();
        time.set(now);
        time.monthDay = 1;
        time.hour = 0;
        time.minute = 0;
        time.second = 0;
        long begin = time.normalize(true);
        long end = begin + MINIMUM_EXPANSION_SPAN;
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        handleInstanceQuery(qb, begin, end, new String[] { Instances._ID },
                null /* selection */, null /* sort */, false /* searchByDayInsteadOfMillis */);

        // Also pre-compute the BusyBits table for this month.
        int startDay = Time.getJulianDay(begin, time.gmtoff);
        int endDay = startDay + 31;
        qb = new SQLiteQueryBuilder();
        handleBusyBitsQuery(qb, startDay, endDay, sBusyBitProjection,
                null /* selection */, null /* sort */);
        rescheduleMissedAlarms();
    }

    private void rescheduleMissedAlarms() {
        AlarmManager manager = getAlarmManager();
        if (manager != null) {
            Context context = getContext();
            ContentResolver cr = context.getContentResolver();
            CalendarAlerts.rescheduleMissedAlarms(cr, context, manager);
        }
    }

    /**
     * Appends comma separated ids.
     * @param ids Should not be empty
     */
    private void appendIds(StringBuilder sb, HashSet<Long> ids) {
        for (long id : ids) {
            sb.append(id).append(',');
        }

        sb.setLength(sb.length() - 1); // Yank the last comma
    }

    @Override
    protected void notifyChange() {
        // Note that semantics are changed: notification is for CONTENT_URI, not the specific
        // Uri that was modified.
        getContext().getContentResolver().notifyChange(Calendar.CONTENT_URI, null,
                true /* syncToNetwork */);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "query: " + uri);
        }

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;
        String limit = null; // Not currently implemented

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().query(db, projection, selection,  selectionArgs,
                        sortOrder);

            case EVENTS:
                qb.setTables("Events, Calendars");
                qb.setProjectionMap(sEventsProjectionMap);
                qb.appendWhere("Events.calendar_id=Calendars._id");
                break;
            case EVENTS_ID:
                qb.setTables("Events, Calendars");
                qb.setProjectionMap(sEventsProjectionMap);
                qb.appendWhere("Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;
            case CALENDARS:
                qb.setTables("Calendars");
                break;
            case CALENDARS_ID:
                qb.setTables("Calendars");
                qb.appendWhere("_id=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;
            case INSTANCES:
            case INSTANCES_BY_DAY:
                long begin;
                long end;
                try {
                    begin = Long.valueOf(uri.getPathSegments().get(2));
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Cannot parse begin "
                            + uri.getPathSegments().get(2));
                }
                try {
                    end = Long.valueOf(uri.getPathSegments().get(3));
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Cannot parse end "
                            + uri.getPathSegments().get(3));
                }
                return handleInstanceQuery(qb, begin, end, projection,
                        selection, sortOrder, match == INSTANCES_BY_DAY);
            case BUSYBITS:
                int startDay;
                int endDay;
                try {
                    startDay = Integer.valueOf(uri.getPathSegments().get(2));
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Cannot parse start day "
                            + uri.getPathSegments().get(2));
                }
                try {
                    endDay = Integer.valueOf(uri.getPathSegments().get(3));
                } catch (NumberFormatException nfe) {
                    throw new IllegalArgumentException("Cannot parse end day "
                            + uri.getPathSegments().get(3));
                }
                return handleBusyBitsQuery(qb, startDay, endDay, projection,
                        selection, sortOrder);
            case ATTENDEES:
                qb.setTables("Attendees, Events, Calendars");
                qb.setProjectionMap(sAttendeesProjectionMap);
                qb.appendWhere("Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=Attendees.event_id");
                break;
            case ATTENDEES_ID:
                qb.setTables("Attendees, Events, Calendars");
                qb.setProjectionMap(sAttendeesProjectionMap);
                qb.appendWhere("Attendees._id=");
                qb.appendWhere(uri.getPathSegments().get(1));
                qb.appendWhere(" AND Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=Attendees.event_id");
                break;
            case REMINDERS:
                qb.setTables("Reminders");
                break;
            case REMINDERS_ID:
                qb.setTables("Reminders, Events, Calendars");
                qb.setProjectionMap(sRemindersProjectionMap);
                qb.appendWhere("Reminders._id=");
                qb.appendWhere(uri.getLastPathSegment());
                qb.appendWhere(" AND Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=Reminders.event_id");
                break;
            case CALENDAR_ALERTS:
                qb.setTables("CalendarAlerts, Events, Calendars");
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                qb.appendWhere("Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=CalendarAlerts.event_id");
                break;
            case CALENDAR_ALERTS_BY_INSTANCE:
                qb.setTables("CalendarAlerts, Events, Calendars");
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                qb.appendWhere("Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=CalendarAlerts.event_id");
                groupBy = CalendarAlerts.EVENT_ID + "," + CalendarAlerts.BEGIN;
                break;
            case CALENDAR_ALERTS_ID:
                qb.setTables("CalendarAlerts, Events, Calendars");
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                qb.appendWhere("CalendarAlerts._id=");
                qb.appendWhere(uri.getLastPathSegment());
                qb.appendWhere(" AND Events.calendar_id=Calendars._id");
                qb.appendWhere(" AND Events._id=CalendarAlerts.event_id");
                break;
            case EXTENDED_PROPERTIES:
                qb.setTables("ExtendedProperties");
                break;
            case EXTENDED_PROPERTIES_ID:
                qb.setTables("ExtendedProperties");
                qb.appendWhere("ExtendedProperties._id=");
                qb.appendWhere(uri.getPathSegments().get(1));
                break;
            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        // run the query
        return query(db, qb, projection, selection, selectionArgs, sortOrder, groupBy, limit);
    }

    private Cursor query(final SQLiteDatabase db, SQLiteQueryBuilder qb, String[] projection,
            String selection, String[] selectionArgs, String sortOrder, String groupBy,
            String limit) {
        final Cursor c = qb.query(db, projection, selection, selectionArgs, groupBy, null,
                sortOrder, limit);
        if (c != null) {
            // TODO: is this the right notification Uri?
            c.setNotificationUri(getContext().getContentResolver(), Calendar.Events.CONTENT_URI);
        }
        return c;
    }

    /*
     * Fills the Instances table, if necessary, for the given range and then
     * queries the Instances table.
     *
     * @param qb The query
     * @param rangeBegin start of range (Julian days or ms)
     * @param rangeEnd end of range (Julian days or ms)
     * @param projection The projection
     * @param selection The selection
     * @param sort How to sort
     * @param searchByDay if true, range is in Julian days, if false, range is in ms
     * @return
     */
    private Cursor handleInstanceQuery(SQLiteQueryBuilder qb, long rangeBegin,
            long rangeEnd, String[] projection,
            String selection, String sort, boolean searchByDay) {

        qb.setTables("Instances INNER JOIN Events ON (Instances.event_id=Events._id) " +
                "INNER JOIN Calendars ON (Events.calendar_id = Calendars._id)");
        qb.setProjectionMap(sInstancesProjectionMap);
        if (searchByDay) {
            // Convert the first and last Julian day range to a range that uses
            // UTC milliseconds.
            Time time = new Time();
            long beginMs = time.setJulianDay((int) rangeBegin);
            // We add one to lastDay because the time is set to 12am on the given
            // Julian day and we want to include all the events on the last day.
            long endMs = time.setJulianDay((int) rangeEnd + 1);
            // will lock the database.
            acquireInstanceRange(beginMs, endMs, true /* use minimum expansion window */);
            qb.appendWhere("startDay <= ");
            qb.appendWhere(String.valueOf(rangeEnd));
            qb.appendWhere(" AND endDay >= ");
        } else {
            // will lock the database.
            acquireInstanceRange(rangeBegin, rangeEnd, true /* use minimum expansion window */);
            qb.appendWhere("begin <= ");
            qb.appendWhere(String.valueOf(rangeEnd));
            qb.appendWhere(" AND end >= ");
        }
        qb.appendWhere(String.valueOf(rangeBegin));
        return qb.query(mDb, projection, selection, null /* selectionArgs */, null /* groupBy */,
                null /* having */, sort);
    }

    private Cursor handleBusyBitsQuery(SQLiteQueryBuilder qb, int startDay,
            int endDay, String[] projection,
            String selection, String sort) {
        acquireBusyBitRange(startDay, endDay);
        qb.setTables("BusyBits");
        qb.setProjectionMap(sBusyBitsProjectionMap);
        qb.appendWhere("day >= ");
        qb.appendWhere(String.valueOf(startDay));
        qb.appendWhere(" AND day <= ");
        qb.appendWhere(String.valueOf(endDay));
        return qb.query(mDb, projection, selection, null /* selectionArgs */, null /* groupBy */,
                null /* having */, sort);
    }

    /**
     * Ensure that the date range given has all elements in the instance
     * table.  Acquires the database lock and calls {@link #acquireInstanceRangeLocked}.
     *
     * @param begin start of range (ms)
     * @param end end of range (ms)
     * @param useMinimumExpansionWindow expand by at least MINIMUM_EXPANSION_SPAN
     */
    private void acquireInstanceRange(final long begin,
            final long end,
            final boolean useMinimumExpansionWindow) {
        mDb.beginTransaction();
        try {
            acquireInstanceRangeLocked(begin, end, useMinimumExpansionWindow);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
    }

    /**
     * Expands the Instances table (if needed) and the BusyBits table.
     * Acquires the database lock and calls {@link #acquireBusyBitRangeLocked}.
     */
    private void acquireBusyBitRange(final int startDay, final int endDay) {
        mDb.beginTransaction();
        try {
            acquireBusyBitRangeLocked(startDay, endDay);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
    }

    /**
     * Ensure that the date range given has all elements in the instance
     * table.  The database lock must be held when calling this method.
     *
     * @param begin start of range (ms)
     * @param end end of range (ms)
     * @param useMinimumExpansionWindow expand by at least MINIMUM_EXPANSION_SPAN
     */
    private void acquireInstanceRangeLocked(long begin, long end,
            boolean useMinimumExpansionWindow) {
        long expandBegin = begin;
        long expandEnd = end;

        if (useMinimumExpansionWindow) {
            // if we end up having to expand events into the instances table, expand
            // events for a minimal amount of time, so we do not have to perform
            // expansions frequently.
            long span = end - begin;
            if (span < MINIMUM_EXPANSION_SPAN) {
                long additionalRange = (MINIMUM_EXPANSION_SPAN - span) / 2;
                expandBegin -= additionalRange;
                expandEnd += additionalRange;
            }
        }

        // Check if the timezone has changed.
        // We do this check here because the database is locked and we can
        // safely delete all the entries in the Instances table.
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        String dbTimezone = fields.timezone;
        long maxInstance = fields.maxInstance;
        long minInstance = fields.minInstance;
        String localTimezone = TimeZone.getDefault().getID();
        boolean timezoneChanged = (dbTimezone == null) || !dbTimezone.equals(localTimezone);

        if (maxInstance == 0 || timezoneChanged) {
            // Empty the Instances table and expand from scratch.
            mDb.execSQL("DELETE FROM Instances;");
            mDb.execSQL("DELETE FROM BusyBits;");
            if (Config.LOGV) {
                Log.v(TAG, "acquireInstanceRangeLocked() deleted Instances and Busybits,"
                        + " timezone changed: " + timezoneChanged);
            }
            expandInstanceRangeLocked(expandBegin, expandEnd, localTimezone);

            mMetaData.writeLocked(localTimezone, expandBegin, expandEnd,
                    0 /* startDay */, 0 /* endDay */);
            return;
        }

        // If the desired range [begin, end] has already been
        // expanded, then simply return.  The range is inclusive, that is,
        // events that touch either endpoint are included in the expansion.
        // This means that a zero-duration event that starts and ends at
        // the endpoint will be included.
        // We use [begin, end] here and not [expandBegin, expandEnd] for
        // checking the range because a common case is for the client to
        // request successive days or weeks, for example.  If we checked
        // that the expanded range [expandBegin, expandEnd] then we would
        // always be expanding because there would always be one more day
        // or week that hasn't been expanded.
        if ((begin >= minInstance) && (end <= maxInstance)) {
            if (Config.LOGV) {
                Log.v(TAG, "Canceled instance query (" + expandBegin + ", " + expandEnd
                        + ") falls within previously expanded range.");
            }
            return;
        }

        // If the requested begin point has not been expanded, then include
        // more events than requested in the expansion (use "expandBegin").
        if (begin < minInstance) {
            expandInstanceRangeLocked(expandBegin, minInstance, localTimezone);
            minInstance = expandBegin;
        }

        // If the requested end point has not been expanded, then include
        // more events than requested in the expansion (use "expandEnd").
        if (end > maxInstance) {
            expandInstanceRangeLocked(maxInstance, expandEnd, localTimezone);
            maxInstance = expandEnd;
        }

        // Update the bounds on the Instances table.
        mMetaData.writeLocked(localTimezone, minInstance, maxInstance,
                fields.minBusyBit, fields.maxBusyBit);
    }

    private void acquireBusyBitRangeLocked(int firstDay, int lastDay) {
        if (firstDay > lastDay) {
            throw new IllegalArgumentException("firstDay must not be greater than lastDay");
        }
        String localTimezone = TimeZone.getDefault().getID();
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        String dbTimezone = fields.timezone;
        int minBusyBit = fields.minBusyBit;
        int maxBusyBit = fields.maxBusyBit;
        boolean timezoneChanged = (dbTimezone == null) || !dbTimezone.equals(localTimezone);
        if (firstDay >= minBusyBit && lastDay <= maxBusyBit && !timezoneChanged) {
            if (Config.LOGV) {
                Log.v(TAG, "acquireBusyBitRangeLocked() no expansion needed");
            }
            return;
        }

        // Avoid gaps in the BusyBit table and avoid recomputing the busy bits
        // that are already in the table.  If the busy bit range has been cleared,
        // don't bother checking.
        if (maxBusyBit != 0) {
            if (firstDay > maxBusyBit) {
                firstDay = maxBusyBit;
            } else if (lastDay < minBusyBit) {
                lastDay = minBusyBit;
            } else if (firstDay < minBusyBit && lastDay <= maxBusyBit) {
                lastDay = minBusyBit;
            } else if (lastDay > maxBusyBit && firstDay >= minBusyBit) {
                firstDay = maxBusyBit;
            }
        }

        // Allocate space for the busy bits, one 32-bit integer for each day.
        int numDays = lastDay - firstDay + 1;
        int[] busybits = new int[numDays];
        int[] allDayCounts = new int[numDays];

        // Convert the first and last Julian day range to a range that uses
        // UTC milliseconds.
        Time time = new Time();
        long begin = time.setJulianDay(firstDay);

        // We add one to lastDay because the time is set to 12am on the given
        // Julian day and we want to include all the events on the last day.
        long end = time.setJulianDay(lastDay + 1);

        // Make sure the Instances table includes events in the range
        // [begin, end].
        acquireInstanceRange(begin, end, true /* use minimum expansion window */);

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables("Instances INNER JOIN Events ON (Instances.event_id=Events._id) " +
                "INNER JOIN Calendars ON (Events.calendar_id = Calendars._id)");
        qb.setProjectionMap(sInstancesProjectionMap);
        qb.appendWhere("begin <= ");
        qb.appendWhere(String.valueOf(end));
        qb.appendWhere(" AND end >= ");
        qb.appendWhere(String.valueOf(begin));
        qb.appendWhere(" AND ");
        qb.appendWhere(Instances.SELECTED);
        qb.appendWhere("=1");

        // Get all the instances that overlap the range [begin,end]
        Cursor cursor = qb.query(mDb, sInstancesProjection, null /* selection */,
                null /* selectionArgs */, null /* groupBy */,
                null /* having */, null /* sortOrder */);
        int count = 0;
        try {
            count = cursor.getCount();
            while (cursor.moveToNext()) {
                int startDay = cursor.getInt(INSTANCES_INDEX_START_DAY);
                int endDay = cursor.getInt(INSTANCES_INDEX_END_DAY);
                int startMinute = cursor.getInt(INSTANCES_INDEX_START_MINUTE);
                int endMinute = cursor.getInt(INSTANCES_INDEX_END_MINUTE);
                boolean allDay = cursor.getInt(INSTANCES_INDEX_ALL_DAY) != 0;
                fillBusyBits(firstDay, startDay, endDay, startMinute, endMinute,
                        allDay, busybits, allDayCounts);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        if (count == 0) {
            return;
        }

        // Read the busybit range again because that may have changed when we
        // called acquireInstanceRange().
        fields = mMetaData.getFieldsLocked();
        minBusyBit = fields.minBusyBit;
        maxBusyBit = fields.maxBusyBit;

        // If the busybit range was cleared, then delete all the entries.
        if (maxBusyBit == 0) {
            mDb.execSQL("DELETE FROM BusyBits;");
        }

        // Merge the busy bits with the database.
        mergeBusyBits(firstDay, lastDay, busybits, allDayCounts);
        if (maxBusyBit == 0) {
            minBusyBit = firstDay;
            maxBusyBit = lastDay;
        } else {
            if (firstDay < minBusyBit) {
                minBusyBit = firstDay;
            }
            if (lastDay > maxBusyBit) {
                maxBusyBit = lastDay;
            }
        }
        // Update the busy bit range
        mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                minBusyBit, maxBusyBit);
    }

    private static final String[] EXPAND_COLUMNS = new String[] {
            Events._ID,
            Events._SYNC_ID,
            Events.STATUS,
            Events.DTSTART,
            Events.DTEND,
            Events.EVENT_TIMEZONE,
            Events.RRULE,
            Events.RDATE,
            Events.EXRULE,
            Events.EXDATE,
            Events.DURATION,
            Events.ALL_DAY,
            Events.ORIGINAL_EVENT,
            Events.ORIGINAL_INSTANCE_TIME
    };

    /**
     * Make instances for the given range.
     */
    private void expandInstanceRangeLocked(long begin, long end, String localTimezone) {

        if (PROFILE) {
            Debug.startMethodTracing("expandInstanceRangeLocked");
        }

        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Expanding events between " + begin + " and " + end);
        }

        Cursor entries = getEntries(begin, end);
        try {
            performInstanceExpansion(begin, end, localTimezone, entries);
        } finally {
            if (entries != null) {
                entries.close();
            }
        }
        if (PROFILE) {
            Debug.stopMethodTracing();
        }
    }

    /**
     * Get all entries affecting the given window.
     * @param begin Window start (ms).
     * @param end Window end (ms).
     * @return Cursor for the entries; caller must close it.
     */
    private Cursor getEntries(long begin, long end) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables("Events INNER JOIN Calendars ON (calendar_id = Calendars._id)");
        qb.setProjectionMap(sEventsProjectionMap);

        String beginString = String.valueOf(begin);
        String endString = String.valueOf(end);

        qb.appendWhere("(dtstart <= ");
        qb.appendWhere(endString);
        qb.appendWhere(" AND ");
        qb.appendWhere("(lastDate IS NULL OR lastDate >= ");
        qb.appendWhere(beginString);
        qb.appendWhere(")) OR (");
        // grab recurrence exceptions that fall outside our expansion window but modify
        // recurrences that do fall within our window.  we won't insert these into the output
        // set of instances, but instead will just add them to our cancellations list, so we
        // can cancel the correct recurrence expansion instances.
        qb.appendWhere("originalInstanceTime IS NOT NULL ");
        qb.appendWhere("AND originalInstanceTime <= ");
        qb.appendWhere(endString);
        qb.appendWhere(" AND ");
        // we don't have originalInstanceDuration or end time.  for now, assume the original
        // instance lasts no longer than 1 week.
        // TODO: compute the originalInstanceEndTime or get this from the server.
        qb.appendWhere("originalInstanceTime >= ");
        qb.appendWhere(String.valueOf(begin - MAX_ASSUMED_DURATION));
        qb.appendWhere(")");

        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Retrieving events to expand: " + qb.toString());
        }

        return qb.query(mDb, EXPAND_COLUMNS, null /* selection */,
                null /* selectionArgs */, null /* groupBy */,
                null /* having */, null /* sortOrder */);
    }

    /**
     * Perform instance expansion on the given entries.
     * @param begin Window start (ms).
     * @param end Window end (ms).
     * @param localTimezone
     * @param entries The entries to process.
     */
    private void performInstanceExpansion(long begin, long end, String localTimezone,
                                          Cursor entries) {
        RecurrenceProcessor rp = new RecurrenceProcessor();

        int statusColumn = entries.getColumnIndex(Events.STATUS);
        int dtstartColumn = entries.getColumnIndex(Events.DTSTART);
        int dtendColumn = entries.getColumnIndex(Events.DTEND);
        int eventTimezoneColumn = entries.getColumnIndex(Events.EVENT_TIMEZONE);
        int durationColumn = entries.getColumnIndex(Events.DURATION);
        int rruleColumn = entries.getColumnIndex(Events.RRULE);
        int rdateColumn = entries.getColumnIndex(Events.RDATE);
        int exruleColumn = entries.getColumnIndex(Events.EXRULE);
        int exdateColumn = entries.getColumnIndex(Events.EXDATE);
        int allDayColumn = entries.getColumnIndex(Events.ALL_DAY);
        int idColumn = entries.getColumnIndex(Events._ID);
        int syncIdColumn = entries.getColumnIndex(Events._SYNC_ID);
        int originalEventColumn = entries.getColumnIndex(Events.ORIGINAL_EVENT);
        int originalInstanceTimeColumn = entries.getColumnIndex(Events.ORIGINAL_INSTANCE_TIME);

        ContentValues initialValues;
        EventInstancesMap instancesMap = new EventInstancesMap();

        Duration duration = new Duration();
        Time eventTime = new Time();

        // Invariant: entries contains all events that affect the current
        // window.  It consists of:
        // a) Individual events that fall in the window.  These will be
        //    displayed.
        // b) Recurrences that included the window.  These will be displayed
        //    if not canceled.
        // c) Recurrence exceptions that fall in the window.  These will be
        //    displayed if not cancellations.
        // d) Recurrence exceptions that modify an instance inside the
        //    window (subject to 1 week assumption above), but are outside
        //    the window.  These will not be displayed.  Cases c and d are
        //    distingushed by the start / end time.

        while (entries.moveToNext()) {
            try {
                initialValues = null;

                boolean allDay = entries.getInt(allDayColumn) != 0;

                String eventTimezone = entries.getString(eventTimezoneColumn);
                if (allDay || TextUtils.isEmpty(eventTimezone)) {
                    // in the events table, allDay events start at midnight.
                    // this forces them to stay at midnight for all day events
                    // TODO: check that this actually does the right thing.
                    eventTimezone = Time.TIMEZONE_UTC;
                }

                long dtstartMillis = entries.getLong(dtstartColumn);
                Long eventId = Long.valueOf(entries.getLong(idColumn));

                String durationStr = entries.getString(durationColumn);
                if (durationStr != null) {
                    try {
                        duration.parse(durationStr);
                    }
                    catch (DateException e) {
                        Log.w(TAG, "error parsing duration for event "
                                + eventId + "'" + durationStr + "'", e);
                        duration.sign = 1;
                        duration.weeks = 0;
                        duration.days = 0;
                        duration.hours = 0;
                        duration.minutes = 0;
                        duration.seconds = 0;
                        durationStr = "+P0S";
                    }
                }

                String syncId = entries.getString(syncIdColumn);
                String originalEvent = entries.getString(originalEventColumn);

                long originalInstanceTimeMillis = -1;
                if (!entries.isNull(originalInstanceTimeColumn)) {
                    originalInstanceTimeMillis= entries.getLong(originalInstanceTimeColumn);
                }
                int status = entries.getInt(statusColumn);

                String rruleStr = entries.getString(rruleColumn);
                String rdateStr = entries.getString(rdateColumn);
                String exruleStr = entries.getString(exruleColumn);
                String exdateStr = entries.getString(exdateColumn);

                RecurrenceSet recur = new RecurrenceSet(rruleStr, rdateStr, exruleStr, exdateStr);

                if (recur.hasRecurrence()) {
                    // the event is repeating

                    if (status == Events.STATUS_CANCELED) {
                        // should not happen!
                        Log.e(TAG, "Found canceled recurring event in "
                                + "Events table.  Ignoring.");
                        continue;
                    }

                    // need to parse the event into a local calendar.
                    eventTime.timezone = eventTimezone;
                    eventTime.set(dtstartMillis);
                    eventTime.allDay = allDay;

                    if (durationStr == null) {
                        // should not happen.
                        Log.e(TAG, "Repeating event has no duration -- "
                                + "should not happen.");
                        if (allDay) {
                            // set to one day.
                            duration.sign = 1;
                            duration.weeks = 0;
                            duration.days = 1;
                            duration.hours = 0;
                            duration.minutes = 0;
                            duration.seconds = 0;
                            durationStr = "+P1D";
                        } else {
                            // compute the duration from dtend, if we can.
                            // otherwise, use 0s.
                            duration.sign = 1;
                            duration.weeks = 0;
                            duration.days = 0;
                            duration.hours = 0;
                            duration.minutes = 0;
                            if (!entries.isNull(dtendColumn)) {
                                long dtendMillis = entries.getLong(dtendColumn);
                                duration.seconds = (int) ((dtendMillis - dtstartMillis) / 1000);
                                durationStr = "+P" + duration.seconds + "S";
                            } else {
                                duration.seconds = 0;
                                durationStr = "+P0S";
                            }
                        }
                    }

                    long[] dates;
                    dates = rp.expand(eventTime, recur, begin, end);

                    // Initialize the "eventTime" timezone outside the loop.
                    // This is used in computeTimezoneDependentFields().
                    if (allDay) {
                        eventTime.timezone = Time.TIMEZONE_UTC;
                    } else {
                        eventTime.timezone = localTimezone;
                    }

                    long durationMillis = duration.getMillis();
                    for (long date : dates) {
                        initialValues = new ContentValues();
                        initialValues.put(Instances.EVENT_ID, eventId);

                        initialValues.put(Instances.BEGIN, date);
                        long dtendMillis = date + durationMillis;
                        initialValues.put(Instances.END, dtendMillis);

                        computeTimezoneDependentFields(date, dtendMillis,
                                eventTime, initialValues);
                        instancesMap.add(syncId, initialValues);
                    }
                } else {
                    // the event is not repeating
                    initialValues = new ContentValues();

                    // if this event has an "original" field, then record
                    // that we need to cancel the original event (we can't
                    // do that here because the order of this loop isn't
                    // defined)
                    if (originalEvent != null && originalInstanceTimeMillis != -1) {
                        initialValues.put(Events.ORIGINAL_EVENT, originalEvent);
                        initialValues.put(Events.ORIGINAL_INSTANCE_TIME,
                                originalInstanceTimeMillis);
                        initialValues.put(Events.STATUS, status);
                    }

                    long dtendMillis = dtstartMillis;
                    if (durationStr == null) {
                        if (!entries.isNull(dtendColumn)) {
                            dtendMillis = entries.getLong(dtendColumn);
                        }
                    } else {
                        dtendMillis = duration.addTo(dtstartMillis);
                    }

                    // this non-recurring event might be a recurrence exception that doesn't
                    // actually fall within our expansion window, but instead was selected
                    // so we can correctly cancel expanded recurrence instances below.  do not
                    // add events to the instances map if they don't actually fall within our
                    // expansion window.
                    if ((dtendMillis < begin) || (dtstartMillis > end)) {
                        if (originalEvent != null && originalInstanceTimeMillis != -1) {
                            initialValues.put(Events.STATUS, Events.STATUS_CANCELED);
                        } else {
                            Log.w(TAG, "Unexpected event outside window: " + syncId);
                            continue;
                        }
                    }

                    initialValues.put(Instances.EVENT_ID, eventId);
                    initialValues.put(Instances.BEGIN, dtstartMillis);

                    initialValues.put(Instances.END, dtendMillis);

                    if (allDay) {
                        eventTime.timezone = Time.TIMEZONE_UTC;
                    } else {
                        eventTime.timezone = localTimezone;
                    }
                    computeTimezoneDependentFields(dtstartMillis, dtendMillis,
                            eventTime, initialValues);

                    instancesMap.add(syncId, initialValues);
                }
            } catch (DateException e) {
                Log.w(TAG, "RecurrenceProcessor error ", e);
            } catch (TimeFormatException e) {
                Log.w(TAG, "RecurrenceProcessor error ", e);
            }
        }

        // Invariant: instancesMap contains all instances that affect the
        // window, indexed by original sync id.  It consists of:
        // a) Individual events that fall in the window.  They have:
        //   EVENT_ID, BEGIN, END
        // b) Instances of recurrences that fall in the window.  They may
        //   be subject to exceptions.  They have:
        //   EVENT_ID, BEGIN, END
        // c) Exceptions that fall in the window.  They have:
        //   ORIGINAL_EVENT, ORIGINAL_INSTANCE_TIME, STATUS (since they can
        //   be a modification or cancellation), EVENT_ID, BEGIN, END
        // d) Recurrence exceptions that modify an instance inside the
        //   window but fall outside the window.  They have:
        //   ORIGINAL_EVENT, ORIGINAL_INSTANCE_TIME, STATUS =
        //   STATUS_CANCELED, EVENT_ID, BEGIN, END

        // First, delete the original instances corresponding to recurrence
        // exceptions.  We do this by iterating over the list and for each
        // recurrence exception, we search the list for an instance with a
        // matching "original instance time".  If we find such an instance,
        // we remove it from the list.  If we don't find such an instance
        // then we cancel the recurrence exception.
        Set<String> keys = instancesMap.keySet();
        for (String syncId : keys) {
            InstancesList list = instancesMap.get(syncId);
            for (ContentValues values : list) {

                // If this instance is not a recurrence exception, then
                // skip it.
                if (!values.containsKey(Events.ORIGINAL_EVENT)) {
                    continue;
                }

                String originalEvent = values.getAsString(Events.ORIGINAL_EVENT);
                long originalTime = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME);
                InstancesList originalList = instancesMap.get(originalEvent);
                if (originalList == null) {
                    // The original recurrence is not present, so don't try canceling it.
                    continue;
                }

                // Search the original event for a matching original
                // instance time.  If there is a matching one, then remove
                // the original one.  We do this both for exceptions that
                // change the original instance as well as for exceptions
                // that delete the original instance.
                for (int num = originalList.size() - 1; num >= 0; num--) {
                    ContentValues originalValues = originalList.get(num);
                    long beginTime = originalValues.getAsLong(Instances.BEGIN);
                    if (beginTime == originalTime) {
                        // We found the original instance, so remove it.
                        originalList.remove(num);
                    }
                }
            }
        }

        // Invariant: instancesMap contains filtered instances.
        // It consists of:
        // a) Individual events that fall in the window.
        // b) Instances of recurrences that fall in the window and have not
        //   been subject to exceptions.
        // c) Exceptions that fall in the window.  They will have
        //   STATUS_CANCELED if they are cancellations.
        // d) Recurrence exceptions that modify an instance inside the
        //   window but fall outside the window.  These are STATUS_CANCELED.

        // Now do the inserts.  Since the db lock is held when this method is executed,
        // this will be done in a transaction.
        // NOTE: if there is lock contention (e.g., a sync is trying to merge into the db
        // while the calendar app is trying to query the db (expanding instances)), we will
        // not be "polite" and yield the lock until we're done.  This will favor local query
        // operations over sync/write operations.
        for (String syncId : keys) {
            InstancesList list = instancesMap.get(syncId);
            for (ContentValues values : list) {

                // If this instance was cancelled then don't create a new
                // instance.
                Integer status = values.getAsInteger(Events.STATUS);
                if (status != null && status == Events.STATUS_CANCELED) {
                    continue;
                }

                // Remove these fields before inserting a new instance
                values.remove(Events.ORIGINAL_EVENT);
                values.remove(Events.ORIGINAL_INSTANCE_TIME);
                values.remove(Events.STATUS);

                mDbHelper.instancesInsert(values);
            }
        }
    }

    /**
     * Computes the timezone-dependent fields of an instance of an event and
     * updates the "values" map to contain those fields.
     *
     * @param begin the start time of the instance (in UTC milliseconds)
     * @param end the end time of the instance (in UTC milliseconds)
     * @param local a Time object with the timezone set to the local timezone
     * @param values a map that will contain the timezone-dependent fields
     */
    private void computeTimezoneDependentFields(long begin, long end,
            Time local, ContentValues values) {
        local.set(begin);
        int startDay = Time.getJulianDay(begin, local.gmtoff);
        int startMinute = local.hour * 60 + local.minute;

        local.set(end);
        int endDay = Time.getJulianDay(end, local.gmtoff);
        int endMinute = local.hour * 60 + local.minute;

        // Special case for midnight, which has endMinute == 0.  Change
        // that to +24 hours on the previous day to make everything simpler.
        // Exception: if start and end minute are both 0 on the same day,
        // then leave endMinute alone.
        if (endMinute == 0 && endDay > startDay) {
            endMinute = 24 * 60;
            endDay -= 1;
        }

        values.put(Instances.START_DAY, startDay);
        values.put(Instances.END_DAY, endDay);
        values.put(Instances.START_MINUTE, startMinute);
        values.put(Instances.END_MINUTE, endMinute);
    }

    private void fillBusyBits(int minDay, int startDay, int endDay, int startMinute,
            int endMinute, boolean allDay, int[] busybits, int[] allDayCounts) {

        // The startDay can be less than the minDay if we have an event
        // that starts earlier than the time range we are interested in.
        // In that case, we ignore the time range that falls outside the
        // the range we are interested in.
        if (startDay < minDay) {
            startDay = minDay;
            startMinute = 0;
        }

        // Likewise, truncate the event's end day so that it doesn't go past
        // the expected range.
        int numDays = busybits.length;
        int stopDay = endDay;
        if (stopDay > minDay + numDays - 1) {
            stopDay = minDay + numDays - 1;
        }
        int dayIndex = startDay - minDay;

        if (allDay) {
            for (int day = startDay; day <= stopDay; day++, dayIndex++) {
                allDayCounts[dayIndex] += 1;
            }
            return;
        }

        for (int day = startDay; day <= stopDay; day++, dayIndex++) {
            int endTime = endMinute;
            // If the event ends on a future day, then show it extending to
            // the end of this day.
            if (endDay > day) {
                endTime = 24 * 60;
            }

            int startBit = startMinute / BUSYBIT_INTERVAL ;
            int endBit = (endTime + BUSYBIT_INTERVAL - 1) / BUSYBIT_INTERVAL;
            int len = endBit - startBit;
            if (len == 0) {
                len = 1;
            }
            if (len < 0 || len > 24) {
                Log.e("Cal", "fillBusyBits() error: len " + len
                        + " startMinute,endTime " + startMinute + " , " + endTime
                        + " startDay,endDay " + startDay + " , " + endDay);
            } else {
                int oneBits = BIT_MASKS[len];
                busybits[dayIndex] |= oneBits << startBit;
            }

            // Set the start minute to the beginning of the day, in
            // case this event spans multiple days.
            startMinute = 0;
        }
    }

    private void mergeBusyBits(int startDay, int endDay, int[] busybits, int[] allDayCounts) {
        mDb.beginTransaction();
        try {
            mergeBusyBitsLocked(startDay, endDay, busybits, allDayCounts);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
    }

    private void mergeBusyBitsLocked(int startDay, int endDay, int[] busybits,
            int[] allDayCounts) {
        Cursor cursor = null;
        try {
            String selection = "day>=" + startDay + " AND day<=" + endDay;
            cursor = mDb.query("BusyBits", sBusyBitProjection, selection,
                null /* selectionArgs */, null /* groupBy */,
                null /* having */, null /* sortOrder */);
            if (cursor == null) {
                return;
            }
            while (cursor.moveToNext()) {
                int day = cursor.getInt(BUSYBIT_INDEX_DAY);
                int busy = cursor.getInt(BUSYBIT_INDEX_BUSYBITS);
                int allDayCount = cursor.getInt(BUSYBIT_INDEX_ALL_DAY_COUNT);

                int dayIndex = day - startDay;
                busybits[dayIndex] |= busy;
                allDayCounts[dayIndex] += allDayCount;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // Allocate a map that we can reuse
        ContentValues values = new ContentValues();

        // Write the busy bits to the database
        int len = busybits.length;
        for (int dayIndex = 0; dayIndex < len; dayIndex++) {
            int busy = busybits[dayIndex];
            int allDayCount = allDayCounts[dayIndex];
            if (busy == 0 && allDayCount == 0) {
                continue;
            }
            int day = startDay + dayIndex;

            values.clear();
            values.put(BusyBits.DAY, day);
            values.put(BusyBits.BUSYBITS, busy);
            values.put(BusyBits.ALL_DAY_COUNT, allDayCount);
            mDb.replace("BusyBits", null, values);
        }
    }

    /**
     * Updates the BusyBit table when a new event is inserted into the Events
     * table.  This is called after the event has been entered into the Events
     * table.  If the event time is not within the date range of the current
     * BusyBits table, then the busy bits are not updated.  The BusyBits
     * table is not automatically expanded to include this event.
     *
     * @param eventId the id of the newly created event
     * @param values the ContentValues for the new event
     */
    private void insertBusyBitsLocked(long eventId, ContentValues values) {
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        if (fields.maxBusyBit == 0) {
            return;
        }

        // If this is a recurrence event, then the expanded Instances range
        // should be 0 because this is called after updateInstancesLocked().
        // But for now check this condition and report an error if it occurs.
        // In the future, we could even support recurring events by
        // expanding them here and updating the busy bits for each instance.
        if (isRecurrenceEvent(values))  {
            Log.e(TAG, "insertBusyBitsLocked(): unexpected recurrence event\n");
            return;
        }

        long dtstartMillis = values.getAsLong(Events.DTSTART);
        Long dtendMillis = values.getAsLong(Events.DTEND);
        if (dtendMillis == null) {
            dtendMillis = dtstartMillis;
        }

        boolean allDay = false;
        Integer allDayInteger = values.getAsInteger(Events.ALL_DAY);
        if (allDayInteger != null) {
            allDay = allDayInteger != 0;
        }

        Time time = new Time();
        if (allDay) {
            time.timezone = Time.TIMEZONE_UTC;
        }

        ContentValues busyValues = new ContentValues();
        computeTimezoneDependentFields(dtstartMillis, dtendMillis, time, busyValues);

        int startDay = busyValues.getAsInteger(Instances.START_DAY);
        int endDay = busyValues.getAsInteger(Instances.END_DAY);

        // If the event time is not in the expanded BusyBits range,
        // then return.
        if (startDay > fields.maxBusyBit || endDay < fields.minBusyBit) {
            return;
        }

        // Allocate space for the busy bits, one 32-bit integer for each day,
        // plus 24 bytes for the count of events that occur in each time slot.
        int numDays = endDay - startDay + 1;
        int[] busybits = new int[numDays];
        int[] allDayCounts = new int[numDays];

        int startMinute = busyValues.getAsInteger(Instances.START_MINUTE);
        int endMinute = busyValues.getAsInteger(Instances.END_MINUTE);
        fillBusyBits(startDay, startDay, endDay, startMinute, endMinute,
                allDay, busybits, allDayCounts);
        mergeBusyBits(startDay, endDay, busybits, allDayCounts);
    }

    /**
     * Updates the busy bits for an event that is being updated.  This is
     * called before the event is updated in the Events table because we need
     * to know the time of the event before it was changed.
     *
     * @param eventId the id of the event being updated
     * @param values the ContentValues for the updated event
     */
    private void updateBusyBitsLocked(long eventId, ContentValues values) {
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        if (fields.maxBusyBit == 0) {
            return;
        }

        // If this is a recurring event, then clear the BusyBits table.
        if (isRecurrenceEvent(values))  {
            mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                    0 /* startDay */, 0 /* endDay */);
            return;
        }

        // If the event fields being updated don't contain the start or end
        // time, then we don't need to bother updating the BusyBits table.
        Long dtstartLong = values.getAsLong(Events.DTSTART);
        Long dtendLong = values.getAsLong(Events.DTEND);
        if (dtstartLong == null && dtendLong == null) {
            return;
        }

        // If the timezone has changed, then clear the busy bits table
        // and return.
        String dbTimezone = fields.timezone;
        String localTimezone = TimeZone.getDefault().getID();
        boolean timezoneChanged = (dbTimezone == null) || !dbTimezone.equals(localTimezone);
        if (timezoneChanged) {
            mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                    0 /* startDay */, 0 /* endDay */);
            return;
        }

        // Read the existing event start and end times from the Events table.
        TimeRange eventRange = readEventStartEnd(eventId);

        // Fill in the new start time (if missing) or the new end time (if
        // missing) from the existing event start and end times.
        long dtstartMillis;
        if (dtstartLong != null) {
            dtstartMillis = dtstartLong;
        } else {
            dtstartMillis = eventRange.begin;
        }

        long dtendMillis;
        if (dtendLong != null) {
            dtendMillis = dtendLong;
        } else {
            dtendMillis = eventRange.end;
        }

        // Compute the start and end Julian days for the event.
        Time time = new Time();
        if (eventRange.allDay) {
            time.timezone = Time.TIMEZONE_UTC;
        }
        ContentValues busyValues = new ContentValues();
        computeTimezoneDependentFields(eventRange.begin, eventRange.end, time, busyValues);
        int oldStartDay = busyValues.getAsInteger(Instances.START_DAY);
        int oldEndDay = busyValues.getAsInteger(Instances.END_DAY);

        boolean allDay = false;
        Integer allDayInteger = values.getAsInteger(Events.ALL_DAY);
        if (allDayInteger != null) {
            allDay = allDayInteger != 0;
        }

        if (allDay) {
            time.timezone = Time.TIMEZONE_UTC;
        } else {
            time.timezone = TimeZone.getDefault().getID();
        }

        computeTimezoneDependentFields(dtstartMillis, dtendMillis, time, busyValues);
        int newStartDay = busyValues.getAsInteger(Instances.START_DAY);
        int newEndDay = busyValues.getAsInteger(Instances.END_DAY);

        // If both the old and new event times are outside the expanded
        // BusyBits table, then return.
        if ((oldStartDay > fields.maxBusyBit || oldEndDay < fields.minBusyBit)
                && (newStartDay > fields.maxBusyBit || newEndDay < fields.minBusyBit)) {
            return;
        }

        // If the old event time is within the expanded Instances range,
        // then clear the BusyBits table and return.
        if (oldStartDay <= fields.maxBusyBit && oldEndDay >= fields.minBusyBit) {
            // We could recompute the busy bits for the days containing the
            // old event time.  For now, just clear the BusyBits table.
            mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                    0 /* startDay */, 0 /* endDay */);
            return;
        }

        // The new event time is within the expanded Instances range.
        // So insert the busy bits for that day (or days).

        // Allocate space for the busy bits, one 32-bit integer for each day,
        // plus 24 bytes for the count of events that occur in each time slot.
        int numDays = newEndDay - newStartDay + 1;
        int[] busybits = new int[numDays];
        int[] allDayCounts = new int[numDays];

        int startMinute = busyValues.getAsInteger(Instances.START_MINUTE);
        int endMinute = busyValues.getAsInteger(Instances.END_MINUTE);
        fillBusyBits(newStartDay, newStartDay, newEndDay, startMinute, endMinute,
                allDay, busybits, allDayCounts);
        mergeBusyBits(newStartDay, newEndDay, busybits, allDayCounts);
    }

    /**
     * This method is called just before an event is deleted.
     *
     * @param eventId
     */
    private void deleteBusyBitsLocked(long eventId) {
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        if (fields.maxBusyBit == 0) {
            return;
        }

        // TODO: if the event being deleted is not a recurring event and the
        // start and end time are outside the BusyBit range, then we could
        // avoid clearing the BusyBits table.  For now, always clear the
        // BusyBits table because deleting events is relatively rare.
        mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                0 /* startDay */, 0 /* endDay */);
    }

    // Read the start and end time for an event from the Events table.
    // Also read the "all-day" indicator.
    private TimeRange readEventStartEnd(long eventId) {
        Cursor cursor = null;
        TimeRange range = new TimeRange();
        try {
            cursor = query(ContentUris.withAppendedId(Events.CONTENT_URI, eventId),
                    new String[] { Events.DTSTART, Events.DTEND, Events.ALL_DAY },
                    null /* selection */,
                    null /* selectionArgs */,
                    null /* sort */);
            if (cursor == null || !cursor.moveToFirst()) {
                Log.d(TAG, "Couldn't find " + eventId + " in Events table");
                return null;
            }
            range.begin = cursor.getLong(0);
            range.end = cursor.getLong(1);
            range.allDay = cursor.getInt(2) != 0;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return range;
    }

    @Override
    public String getType(Uri url) {
        int match = sUriMatcher.match(url);
        switch (match) {
            case EVENTS:
                return "vnd.android.cursor.dir/event";
            case EVENTS_ID:
                return "vnd.android.cursor.item/event";
            case REMINDERS:
                return "vnd.android.cursor.dir/reminder";
            case REMINDERS_ID:
                return "vnd.android.cursor.item/reminder";
            case CALENDAR_ALERTS:
                return "vnd.android.cursor.dir/calendar-alert";
            case CALENDAR_ALERTS_BY_INSTANCE:
                return "vnd.android.cursor.dir/calendar-alert-by-instance";
            case CALENDAR_ALERTS_ID:
                return "vnd.android.cursor.item/calendar-alert";
            case INSTANCES:
            case INSTANCES_BY_DAY:
                return "vnd.android.cursor.dir/event-instance";
            case BUSYBITS:
                return "vnd.android.cursor.dir/busybits";
            default:
                throw new IllegalArgumentException("Unknown URL " + url);
        }
    }

    public static boolean isRecurrenceEvent(ContentValues values) {
        return (!TextUtils.isEmpty(values.getAsString(Events.RRULE))||
                !TextUtils.isEmpty(values.getAsString(Events.RDATE))||
                !TextUtils.isEmpty(values.getAsString(Events.ORIGINAL_EVENT)));
    }

    @Override
    protected Uri insertInTransaction(Uri uri, ContentValues values) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "insertInTransaction: " + uri);
        }

        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, Calendar.CALLER_IS_SYNCADAPTER, false);

        final int match = sUriMatcher.match(uri);
        long id = 0;

        switch (match) {
              case SYNCSTATE:
                id = mDbHelper.getSyncState().insert(mDb, values);
                break;
            case EVENTS:
                if (!callerIsSyncAdapter) {
                    values.put(Events._SYNC_DIRTY, 1);
                }
                if (!values.containsKey(Events.DTSTART)) {
                    throw new RuntimeException("DTSTART field missing from event");
                }
                // TODO: avoid the call to updateBundleFromEvent if this is just finding local
                // changes.
                // TODO: do we really need to make a copy?
                ContentValues updatedValues = updateContentValuesFromEvent(values);
                if (updatedValues == null) {
                    throw new RuntimeException("Could not insert event.");
                    // return null;
                }
                String owner = null;
                if (updatedValues.containsKey(Events.CALENDAR_ID) &&
                        !updatedValues.containsKey(Events.ORGANIZER)) {
                    owner = getOwner(updatedValues.getAsLong(Events.CALENDAR_ID));
                    // TODO: This isn't entirely correct.  If a guest is adding a recurrence
                    // exception to an event, the organizer should stay the original organizer.
                    // This value doesn't go to the server and it will get fixed on sync,
                    // so it shouldn't really matter.
                    if (owner != null) {
                        updatedValues.put(Events.ORGANIZER, owner);
                    }
                }

                id = mDbHelper.eventsInsert(updatedValues);
                if (id != -1) {
                    updateEventRawTimesLocked(id, updatedValues);
                    updateInstancesLocked(updatedValues, id, true /* new event */, mDb);
                    insertBusyBitsLocked(id, updatedValues);

                    // If we inserted a new event that specified the self-attendee
                    // status, then we need to add an entry to the attendees table.
                    if (values.containsKey(Events.SELF_ATTENDEE_STATUS)) {
                        int status = values.getAsInteger(Events.SELF_ATTENDEE_STATUS);
                        if (owner == null) {
                            owner = getOwner(updatedValues.getAsLong(Events.CALENDAR_ID));
                        }
                        createAttendeeEntry(id, status, owner);
                    }
                    triggerAppWidgetUpdate(id);
                }
                break;
            case CALENDARS:
                Integer syncEvents = values.getAsInteger(Calendars.SYNC_EVENTS);
                if (syncEvents != null && syncEvents == 1) {
                    String accountName = values.getAsString(Calendars._SYNC_ACCOUNT);
                    String accountType = values.getAsString(
                            Calendars._SYNC_ACCOUNT_TYPE);
                    final Account account = new Account(accountName, accountType);
                    String calendarUrl = values.getAsString(Calendars.URL);
                    mDbHelper.scheduleSync(account, false /* two-way sync */, calendarUrl);
                }
                id = mDbHelper.calendarsInsert(values);
                break;
            case ATTENDEES:
                if (!values.containsKey(Attendees.EVENT_ID)) {
                    throw new IllegalArgumentException("Attendees values must "
                            + "contain an event_id");
                }
                id = mDbHelper.attendeesInsert(values);
                if (!callerIsSyncAdapter) {
                    setEventDirty(values.getAsInteger(Attendees.EVENT_ID));
                }

                // Copy the attendee status value to the Events table.
                updateEventAttendeeStatus(mDb, values);
                break;
            case REMINDERS:
                if (!values.containsKey(Reminders.EVENT_ID)) {
                    throw new IllegalArgumentException("Reminders values must "
                            + "contain an event_id");
                }
                id = mDbHelper.remindersInsert(values);
                if (!callerIsSyncAdapter) {
                    setEventDirty(values.getAsInteger(Reminders.EVENT_ID));
                }

                // Schedule another event alarm, if necessary
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "insertInternal() changing reminder");
                }
                scheduleNextAlarm(false /* do not remove alarms */);
                break;
            case CALENDAR_ALERTS:
                if (!values.containsKey(CalendarAlerts.EVENT_ID)) {
                    throw new IllegalArgumentException("CalendarAlerts values must "
                            + "contain an event_id");
                }
                id = mDbHelper.calendarAlertsInsert(values);
                // Note: dirty bit is not set for Alerts because it is not synced.
                // It is generated from Reminders, which is synced.
                break;
            case EXTENDED_PROPERTIES:
                if (!values.containsKey(Calendar.ExtendedProperties.EVENT_ID)) {
                    throw new IllegalArgumentException("ExtendedProperties values must "
                            + "contain an event_id");
                }
                id = mDbHelper.extendedPropertiesInsert(values);
                if (!callerIsSyncAdapter) {
                    setEventDirty(values.getAsInteger(Calendar.ExtendedProperties.EVENT_ID));
                }
                break;
            case DELETED_EVENTS:
            case EVENTS_ID:
            case REMINDERS_ID:
            case CALENDAR_ALERTS_ID:
            case EXTENDED_PROPERTIES_ID:
            case INSTANCES:
            case INSTANCES_BY_DAY:
                throw new UnsupportedOperationException("Cannot insert into that URL: " + uri);
            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        if (id < 0) {
            return null;
        }

        return ContentUris.withAppendedId(uri, id);
    }

    private void setEventDirty(int eventId) {
        mDb.execSQL("UPDATE Events SET _sync_dirty=1 where _id=" + eventId);
    }

    /**
     * Gets the calendar's owner for an event.
     * @param calId
     * @return email of owner or null
     */
    private String getOwner(long calId) {
        // Get the email address of this user from this Calendar
        String emailAddress = null;
        Cursor cursor = null;
        try {
            cursor = query(ContentUris.withAppendedId(Calendars.CONTENT_URI, calId),
                    new String[] { Calendars.OWNER_ACCOUNT },
                    null /* selection */,
                    null /* selectionArgs */,
                    null /* sort */);
            if (cursor == null || !cursor.moveToFirst()) {
                Log.d(TAG, "Couldn't find " + calId + " in Calendars table");
                return null;
            }
            emailAddress = cursor.getString(0);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return emailAddress;
    }

    /**
     * Creates an entry in the Attendees table that refers to the given event
     * and that has the given response status.
     *
     * @param eventId the event id that the new entry in the Attendees table
     * should refer to
     * @param status the response status
     * @param emailAddress the email of the attendee
     */
    private void createAttendeeEntry(long eventId, int status, String emailAddress) {
        ContentValues values = new ContentValues();
        values.put(Attendees.EVENT_ID, eventId);
        values.put(Attendees.ATTENDEE_STATUS, status);
        values.put(Attendees.ATTENDEE_TYPE, Attendees.TYPE_NONE);
        // TODO: The relationship could actually be ORGANIZER, but it will get straightened out
        // on sync.
        values.put(Attendees.ATTENDEE_RELATIONSHIP,
                Attendees.RELATIONSHIP_ATTENDEE);
        values.put(Attendees.ATTENDEE_EMAIL, emailAddress);

        // We don't know the ATTENDEE_NAME but that will be filled in by the
        // server and sent back to us.
        mDbHelper.attendeesInsert(values);
    }

    /**
     * Updates the attendee status in the Events table to be consistent with
     * the value in the Attendees table.
     *
     * @param db the database
     * @param attendeeValues the column values for one row in the Attendees
     * table.
     */
    private void updateEventAttendeeStatus(SQLiteDatabase db, ContentValues attendeeValues) {
        // Get the event id for this attendee
        long eventId = attendeeValues.getAsLong(Attendees.EVENT_ID);

        if (MULTIPLE_ATTENDEES_PER_EVENT) {
            // Get the calendar id for this event
            Cursor cursor = null;
            long calId;
            try {
                cursor = query(ContentUris.withAppendedId(Events.CONTENT_URI, eventId),
                        new String[] { Events.CALENDAR_ID },
                        null /* selection */,
                        null /* selectionArgs */,
                        null /* sort */);
                if (cursor == null || !cursor.moveToFirst()) {
                    Log.d(TAG, "Couldn't find " + eventId + " in Events table");
                    return;
                }
                calId = cursor.getLong(0);
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }

            // Get the owner email for this Calendar
            String calendarEmail = null;
            cursor = null;
            try {
                cursor = query(ContentUris.withAppendedId(Calendars.CONTENT_URI, calId),
                        new String[] { Calendars.OWNER_ACCOUNT },
                        null /* selection */,
                        null /* selectionArgs */,
                        null /* sort */);
                if (cursor == null || !cursor.moveToFirst()) {
                    Log.d(TAG, "Couldn't find " + calId + " in Calendars table");
                    return;
                }
                calendarEmail = cursor.getString(0);
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }

            if (calendarEmail == null) {
                return;
            }

            // Get the email address for this attendee
            String attendeeEmail = null;
            if (attendeeValues.containsKey(Attendees.ATTENDEE_EMAIL)) {
                attendeeEmail = attendeeValues.getAsString(Attendees.ATTENDEE_EMAIL);
            }

            // If the attendee email does not match the calendar email, then this
            // attendee is not the owner of this calendar so we don't update the
            // selfAttendeeStatus in the event.
            if (!calendarEmail.equals(attendeeEmail)) {
                return;
            }
        }

        int status = Attendees.ATTENDEE_STATUS_NONE;
        if (attendeeValues.containsKey(Attendees.ATTENDEE_RELATIONSHIP)) {
            int rel = attendeeValues.getAsInteger(Attendees.ATTENDEE_RELATIONSHIP);
            if (rel == Attendees.RELATIONSHIP_ORGANIZER) {
                status = Attendees.ATTENDEE_STATUS_ACCEPTED;
            }
        }

        if (attendeeValues.containsKey(Attendees.ATTENDEE_STATUS)) {
            status = attendeeValues.getAsInteger(Attendees.ATTENDEE_STATUS);
        }

        ContentValues values = new ContentValues();
        values.put(Events.SELF_ATTENDEE_STATUS, status);
        db.update("Events", values, "_id="+eventId, null);
    }

    /**
     * Updates the instances table when an event is added or updated.
     * @param values The new values of the event.
     * @param rowId The database row id of the event.
     * @param newEvent true if the event is new.
     * @param db The database
     */
    private void updateInstancesLocked(ContentValues values,
            long rowId,
            boolean newEvent,
            SQLiteDatabase db) {

        // If there are no expanded Instances, then return.
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        if (fields.maxInstance == 0) {
            return;
        }

        Long dtstartMillis = values.getAsLong(Events.DTSTART);
        if (dtstartMillis == null) {
            if (newEvent) {
                // must be present for a new event.
                throw new RuntimeException("DTSTART missing.");
            }
            if (Config.LOGV) Log.v(TAG, "Missing DTSTART.  "
                    + "No need to update instance.");
            return;
        }

        Long lastDateMillis = values.getAsLong(Events.LAST_DATE);
        Long originalInstanceTime = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME);

        if (!newEvent) {
            // Want to do this for regular event, recurrence, or exception.
            // For recurrence or exception, more deletion may happen below if we
            // do an instance expansion.  This deletion will suffice if the exception
            // is moved outside the window, for instance.
            db.delete("Instances", "event_id=" + rowId, null /* selectionArgs */);
        }

        if (isRecurrenceEvent(values))  {
            // The recurrence or exception needs to be (re-)expanded if:
            // a) Exception or recurrence that falls inside window
            boolean insideWindow = dtstartMillis <= fields.maxInstance &&
                    (lastDateMillis == null || lastDateMillis >= fields.minInstance);
            // b) Exception that affects instance inside window
            // These conditions match the query in getEntries
            //  See getEntries comment for explanation of subtracting 1 week.
            boolean affectsWindow = originalInstanceTime != null &&
                    originalInstanceTime <= fields.maxInstance &&
                    originalInstanceTime >= fields.minInstance - MAX_ASSUMED_DURATION;
            if (insideWindow || affectsWindow) {
                updateRecurrenceInstancesLocked(values, rowId, db);
            }
            // TODO: an exception creation or update could be optimized by
            // updating just the affected instances, instead of regenerating
            // the recurrence.
            return;
        }

        Long dtendMillis = values.getAsLong(Events.DTEND);
        if (dtendMillis == null) {
            dtendMillis = dtstartMillis;
        }

        // if the event is in the expanded range, insert
        // into the instances table.
        // TODO: deal with durations.  currently, durations are only used in
        // recurrences.

        if (dtstartMillis <= fields.maxInstance && dtendMillis >= fields.minInstance) {
            ContentValues instanceValues = new ContentValues();
            instanceValues.put(Instances.EVENT_ID, rowId);
            instanceValues.put(Instances.BEGIN, dtstartMillis);
            instanceValues.put(Instances.END, dtendMillis);

            boolean allDay = false;
            Integer allDayInteger = values.getAsInteger(Events.ALL_DAY);
            if (allDayInteger != null) {
                allDay = allDayInteger != 0;
            }

            // Update the timezone-dependent fields.
            Time local = new Time();
            if (allDay) {
                local.timezone = Time.TIMEZONE_UTC;
            } else {
                local.timezone = fields.timezone;
            }

            computeTimezoneDependentFields(dtstartMillis, dtendMillis, local, instanceValues);
            mDbHelper.instancesInsert(instanceValues);
        }
    }

    /**
     * Determines the recurrence entries associated with a particular recurrence.
     * This set is the base recurrence and any exception.
     *
     * Normally the entries are indicated by the sync id of the base recurrence
     * (which is the originalEvent in the exceptions).
     * However, a complication is that a recurrence may not yet have a sync id.
     * In that case, the recurrence is specified by the rowId.
     *
     * @param recurrenceSyncId The sync id of the base recurrence, or null.
     * @param rowId The row id of the base recurrence.
     * @return the relevant entries.
     */
    private Cursor getRelevantRecurrenceEntries(String recurrenceSyncId, long rowId) {
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();

        qb.setTables("Events INNER JOIN Calendars ON (calendar_id = Calendars._id)");
        qb.setProjectionMap(sEventsProjectionMap);
        if (recurrenceSyncId == null) {
            String where = "Events._id = " + rowId;
            qb.appendWhere(where);
        } else {
            String where = "Events._sync_id = \"" + recurrenceSyncId + "\""
                    + " OR Events.originalEvent = \"" + recurrenceSyncId + "\"";
            qb.appendWhere(where);
        }
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Retrieving events to expand: " + qb.toString());
        }

        return qb.query(mDb, EXPAND_COLUMNS, null /* selection */, null /* selectionArgs */,
                null /* groupBy */, null /* having */, null /* sortOrder */);
    }

    /**
     * Do incremental Instances update of a recurrence or recurrence exception.
     *
     * This method does performInstanceExpansion on just the modified recurrence,
     * to avoid the overhead of recomputing the entire instance table.
     *
     * @param values The new values of the event.
     * @param rowId The database row id of the event.
     * @param db The database
     */
    private void updateRecurrenceInstancesLocked(ContentValues values,
            long rowId,
            SQLiteDatabase db) {
        MetaData.Fields fields = mMetaData.getFieldsLocked();
        String originalEvent = values.getAsString(Events.ORIGINAL_EVENT);
        String recurrenceSyncId = null;
        if (originalEvent != null) {
            recurrenceSyncId = originalEvent;
        } else {
            // Get the recurrence's sync id from the database
            recurrenceSyncId = DatabaseUtils.stringForQuery(db, "SELECT _sync_id FROM Events"
                    + " WHERE _id = " + rowId, null /* selection args */);
        }
        // recurrenceSyncId is the _sync_id of the underlying recurrence
        // If the recurrence hasn't gone to the server, it will be null.

        // Need to clear out old instances
        if (recurrenceSyncId == null) {
            // Creating updating a recurrence that hasn't gone to the server.
            // Need to delete based on row id
            String where = "_id IN (SELECT Instances._id as _id"
                    + " FROM Instances INNER JOIN Events"
                    + " ON (Events._id = Instances.event_id)"
                    + " WHERE Events._id =?)";
            db.delete("Instances", where, new String[]{"" + rowId});
        } else {
            // Creating or modifying a recurrence or exception.
            // Delete instances for recurrence (_sync_id = recurrenceSyncId)
            // and all exceptions (originalEvent = recurrenceSyncId)
            String where = "_id IN (SELECT Instances._id as _id"
                    + " FROM Instances INNER JOIN Events"
                    + " ON (Events._id = Instances.event_id)"
                    + " WHERE Events._sync_id =?"
                    + " OR Events.originalEvent =?)";
            db.delete("Instances", where, new String[]{recurrenceSyncId, recurrenceSyncId});
        }

        // Now do instance expansion
        Cursor entries = getRelevantRecurrenceEntries(recurrenceSyncId, rowId);
        try {
            performInstanceExpansion(fields.minInstance, fields.maxInstance, fields.timezone,
                                     entries);
        } finally {
            if (entries != null) {
                entries.close();
            }
        }

        // Clear busy bits
        mMetaData.writeLocked(fields.timezone, fields.minInstance, fields.maxInstance,
                0 /* startDay */, 0 /* endDay */);
    }

    long calculateLastDate(ContentValues values)
            throws DateException {
        // Allow updates to some event fields like the title or hasAlarm
        // without requiring DTSTART.
        if (!values.containsKey(Events.DTSTART)) {
            if (values.containsKey(Events.DTEND) || values.containsKey(Events.RRULE)
                    || values.containsKey(Events.DURATION)
                    || values.containsKey(Events.EVENT_TIMEZONE)
                    || values.containsKey(Events.RDATE)
                    || values.containsKey(Events.EXRULE)
                    || values.containsKey(Events.EXDATE)) {
                throw new RuntimeException("DTSTART field missing from event");
            }
            return -1;
        }
        long dtstartMillis = values.getAsLong(Events.DTSTART);
        long lastMillis = -1;

        // Can we use dtend with a repeating event?  What does that even
        // mean?
        // NOTE: if the repeating event has a dtend, we convert it to a
        // duration during event processing, so this situation should not
        // occur.
        Long dtEnd = values.getAsLong(Events.DTEND);
        if (dtEnd != null) {
            lastMillis = dtEnd;
        } else {
            // find out how long it is
            Duration duration = new Duration();
            String durationStr = values.getAsString(Events.DURATION);
            if (durationStr != null) {
                duration.parse(durationStr);
            }

            RecurrenceSet recur = new RecurrenceSet(values);

            if (recur.hasRecurrence()) {
                // the event is repeating, so find the last date it
                // could appear on

                String tz = values.getAsString(Events.EVENT_TIMEZONE);

                if (TextUtils.isEmpty(tz)) {
                    // floating timezone
                    tz = Time.TIMEZONE_UTC;
                }
                Time dtstartLocal = new Time(tz);

                dtstartLocal.set(dtstartMillis);

                RecurrenceProcessor rp = new RecurrenceProcessor();
                lastMillis = rp.getLastOccurence(dtstartLocal, recur);
                if (lastMillis == -1) {
                    return lastMillis;  // -1
                }
            } else {
                // the event is not repeating, just use dtstartMillis
                lastMillis = dtstartMillis;
            }

            // that was the beginning of the event.  this is the end.
            lastMillis = duration.addTo(lastMillis);
        }
        return lastMillis;
    }

    private ContentValues updateContentValuesFromEvent(ContentValues initialValues) {
        try {
            ContentValues values = new ContentValues(initialValues);

            long last = calculateLastDate(values);
            if (last != -1) {
                values.put(Events.LAST_DATE, last);
            }

            return values;
        } catch (DateException e) {
            // don't add it if there was an error
            Log.w(TAG, "Could not calculate last date.", e);
            return null;
        }
    }

    private void updateEventRawTimesLocked(long eventId, ContentValues values) {
        ContentValues rawValues = new ContentValues();

        rawValues.put("event_id", eventId);

        String timezone = values.getAsString(Events.EVENT_TIMEZONE);

        boolean allDay = false;
        Integer allDayInteger = values.getAsInteger(Events.ALL_DAY);
        if (allDayInteger != null) {
            allDay = allDayInteger != 0;
        }

        if (allDay || TextUtils.isEmpty(timezone)) {
            // floating timezone
            timezone = Time.TIMEZONE_UTC;
        }

        Time time = new Time(timezone);
        time.allDay = allDay;
        Long dtstartMillis = values.getAsLong(Events.DTSTART);
        if (dtstartMillis != null) {
            time.set(dtstartMillis);
            rawValues.put("dtstart2445", time.format2445());
        }

        Long dtendMillis = values.getAsLong(Events.DTEND);
        if (dtendMillis != null) {
            time.set(dtendMillis);
            rawValues.put("dtend2445", time.format2445());
        }

        Long originalInstanceMillis = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME);
        if (originalInstanceMillis != null) {
            // This is a recurrence exception so we need to get the all-day
            // status of the original recurring event in order to format the
            // date correctly.
            allDayInteger = values.getAsInteger(Events.ORIGINAL_ALL_DAY);
            if (allDayInteger != null) {
                time.allDay = allDayInteger != 0;
            }
            time.set(originalInstanceMillis);
            rawValues.put("originalInstanceTime2445", time.format2445());
        }

        Long lastDateMillis = values.getAsLong(Events.LAST_DATE);
        if (lastDateMillis != null) {
            time.allDay = allDay;
            time.set(lastDateMillis);
            rawValues.put("lastDate2445", time.format2445());
        }

        mDbHelper.eventsRawTimesReplace(rawValues);
    }

    @Override
    protected int deleteInTransaction(Uri uri, String selection, String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "deleteInTransaction: " + uri);
        }
        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, Calendar.CALLER_IS_SYNCADAPTER, false);
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().delete(mDb, selection, selectionArgs);

            case SYNCSTATE_ID:
                String selectionWithId =
                        (BaseColumns._ID + "=" + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND (" + selection + ")");
                return mDbHelper.getSyncState().delete(mDb, selectionWithId, selectionArgs);

            case EVENTS_ID:
            {
                long id = ContentUris.parseId(uri);
                int result = 0;
                if (selection != null) {
                    throw new UnsupportedOperationException("CalendarProvider2 "
                            + "doesn't support selection based deletion for type "
                            + match);
                }
                deleteBusyBitsLocked(id);

                if (callerIsSyncAdapter) {
                    mDb.delete("Events", "_id = " + id, null);
                } else {
                    ContentValues values = new ContentValues();
                    values.put(Events.DELETED, 1);
                    values.put(Events._SYNC_DIRTY, 1);
                    mDb.update("Events", values, "_id = " + id, null);
                }

                // Query this event to get the fields needed for deleting.
                Cursor cursor = mDb.query("Events", EVENTS_PROJECTION,
                        "_id=" + id,
                        null /* selectionArgs */, null /* groupBy */,
                        null /* having */, null /* sortOrder */);
                try {
                    if (cursor.moveToNext()) {
                        result = 1;
                        String syncId = cursor.getString(EVENTS_SYNC_ID_INDEX);
                        if (!TextUtils.isEmpty(syncId)) {

                            // TODO: we may also want to delete exception
                            // events for this event (in case this was a
                            // recurring event).  We can do that with the
                            // following code:
                            // mDb.delete("Events", "originalEvent=?", new String[] {syncId});
                        }

                        // If this was a recurring event or a recurrence
                        // exception, then force a recalculation of the
                        // instances.
                        String rrule = cursor.getString(EVENTS_RRULE_INDEX);
                        String rdate = cursor.getString(EVENTS_RDATE_INDEX);
                        String origEvent = cursor.getString(EVENTS_ORIGINAL_EVENT_INDEX);
                        if (!TextUtils.isEmpty(rrule) || !TextUtils.isEmpty(rdate)
                                || !TextUtils.isEmpty(origEvent)) {
                            mMetaData.clearInstanceRange();
                        }
                    }
                } finally {
                    cursor.close();
                    cursor = null;
                }
                triggerAppWidgetUpdate(-1);

                mDb.delete("Instances", "event_id=" + id, null /* selectionArgs */);
                mDb.delete("EventsRawTimes", "event_id=" + id, null /* selectionArgs */);
                mDb.delete("Attendees", "event_id=" + id, null /* selectionArgs */);
                mDb.delete("Reminders", "event_id=" + id, null /* selectionArgs */);
                mDb.delete("CalendarAlerts", "event_id=" + id, null /* selectionArgs */);
                mDb.delete("ExtendedProperties", "event_id=" + id, null /* selectionArgs */);
                return result;
            }
            case ATTENDEES:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete("Attendees", selection, selectionArgs);
                } else {
                    return deleteFromTable("Attendees", uri, selection, selectionArgs);
                }
            }
            case ATTENDEES_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete("Attendees", "_id=" + id, null /* selectionArgs */);
                } else {
                    return deleteFromTable("Attendees", uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case REMINDERS:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete("Reminders", selection, selectionArgs);
                } else {
                    return deleteFromTable("Reminders", uri, selection, selectionArgs);
                }
            }
            case REMINDERS_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete("Reminders", "_id=" + id, null /* selectionArgs */);
                } else {
                    return deleteFromTable("Reminders", uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case EXTENDED_PROPERTIES:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete("ExtendedProperties", selection, selectionArgs);
                } else {
                    return deleteFromTable("ExtendedProperties", uri, selection, selectionArgs);
                }
            }
            case EXTENDED_PROPERTIES_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete("ExtendedProperties", "_id=" + id, null /* selectionArgs */);
                } else {
                    return deleteFromTable("ExtendedProperties", uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case CALENDAR_ALERTS:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete("CalendarAlerts", selection, selectionArgs);
                } else {
                    return deleteFromTable("CalendarAlerts", uri, selection, selectionArgs);
                }
            }
            case CALENDAR_ALERTS_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                // Note: dirty bit is not set for Alerts because it is not synced.
                // It is generated from Reminders, which is synced.
                long id = ContentUris.parseId(uri);
                return mDb.delete("CalendarAlerts", "_id=" + id, null /* selectionArgs */);
            }
            case DELETED_EVENTS:
            case EVENTS:
                throw new UnsupportedOperationException("Cannot delete that URL: " + uri);
            case CALENDARS_ID:
                StringBuilder selectionSb = new StringBuilder("_id=");
                selectionSb.append(uri.getPathSegments().get(1));
                if (!TextUtils.isEmpty(selection)) {
                    selectionSb.append(" AND (");
                    selectionSb.append(selection);
                    selectionSb.append(')');
                }
                selection = selectionSb.toString();
                // fall through to CALENDARS for the actual delete
            case CALENDARS:
                return deleteMatchingCalendars(selection); // TODO: handle in sync adapter
            case INSTANCES:
            case INSTANCES_BY_DAY:
                throw new UnsupportedOperationException("Cannot delete that URL");
            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }
    }

    /**
     * Delete rows from a table and mark corresponding events as dirty.
     * @param table The table to delete from
     * @param uri The URI specifying the rows
     * @param selection for the query
     * @param selectionArgs for the query
     */
    private int deleteFromTable(String table, Uri uri, String selection, String[] selectionArgs) {
        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about deleting data we don't have permission to read.
        Cursor c = query(uri, ID_PROJECTION, selection, selectionArgs, null);
        ContentValues values = new ContentValues();
        values.put(Events._SYNC_DIRTY, "1");
        int count = 0;
        try {
            while(c.moveToNext()) {
                long id = c.getLong(ID_INDEX);
                long event_id = c.getLong(EVENT_ID_INDEX);
                mDb.delete(table, "_id = " + id, null /* selectionArgs */);
                mDb.update("Events", values, "_id = " + event_id, null /* selectionArgs */);
                count++;
            }
        } finally {
            c.close();
        }
        return count;
    }

    /**
     * Update rows in a table and mark corresponding events as dirty.
     * @param table The table to delete from
     * @param values The values to update
     * @param uri The URI specifying the rows
     * @param selection for the query
     * @param selectionArgs for the query
     */
    private int updateInTable(String table, ContentValues values, Uri uri, String selection,
            String[] selectionArgs) {
        // Note that the query will return data according to the access restrictions,
        // so we don't need to worry about deleting data we don't have permission to read.
        Cursor c = query(uri, ID_PROJECTION, selection, selectionArgs, null);
        ContentValues dirtyValues = new ContentValues();
        dirtyValues.put(Events._SYNC_DIRTY, "1");
        int count = 0;
        try {
            while(c.moveToNext()) {
                long id = c.getLong(ID_INDEX);
                long event_id = c.getLong(EVENT_ID_INDEX);
                mDb.update(table, values, "_id = " + id, null /* selectionArgs */);
                mDb.update("Events", dirtyValues, "_id = " + event_id, null /* selectionArgs */);
                mDb.update(table, values, "_id = " + id, null /* selectionArgs */);
                count++;
            }
        } finally {
            c.close();
        }
        return count;
    }

    private int deleteMatchingCalendars(String where) {
        // query to find all the calendars that match, for each
        // - delete calendar subscription
        // - delete calendar

        int numDeleted = 0;
        Cursor c = mDb.query("Calendars", sCalendarsIdProjection, where,
                null /* selectionArgs */, null /* groupBy */,
                null /* having */, null /* sortOrder */);
        if (c == null) {
            return 0;
        }
        try {
            while (c.moveToNext()) {
                long id = c.getLong(CALENDARS_INDEX_ID);
                modifyCalendarSubscription(id, false /* not selected */);
                c.deleteRow();
                numDeleted++;
            }
        } finally {
            c.close();
        }
        return numDeleted;
    }

    // TODO: call calculateLastDate()!
    @Override
    protected int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs) {
        if (VERBOSE_LOGGING) {
            Log.v(TAG, "updateInTransaction: " + uri);
        }

        int count = 0;

        final int match = sUriMatcher.match(uri);

        final boolean callerIsSyncAdapter =
                readBooleanQueryParameter(uri, Calendar.CALLER_IS_SYNCADAPTER, false);

        // TODO: remove this restriction
        if (!TextUtils.isEmpty(selection) && match != CALENDAR_ALERTS && match != EVENTS) {
            throw new IllegalArgumentException(
                    "WHERE based updates not supported");
        }
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().update(mDb, values,
                        appendAccountToSelection(uri, selection), selectionArgs);

            case SYNCSTATE_ID: {
                selection = appendAccountToSelection(uri, selection);
                String selectionWithId =
                        (BaseColumns._ID + "=" + ContentUris.parseId(uri) + " ")
                        + (selection == null ? "" : " AND (" + selection + ")");
                return mDbHelper.getSyncState().update(mDb, values,
                        selectionWithId, selectionArgs);
            }

            case CALENDARS_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                long id = ContentUris.parseId(uri);
                Integer syncEvents = values.getAsInteger(Calendars.SYNC_EVENTS);
                if (syncEvents != null) {
                    modifyCalendarSubscription(id, syncEvents == 1);
                }

                int result = mDb.update("Calendars", values, "_id="+ id, null /* selectionArgs */);
                // When we change the display status of a Calendar
                // we need to update the busy bits.
                if (values.containsKey(Calendars.SELECTED) || (syncEvents != null)) {
                    // Clear the BusyBits table.
                    mMetaData.clearBusyBitRange();
                }

                return result;
            }
            case EVENTS:
            case EVENTS_ID:
            {
                long id = 0;
                if (match == EVENTS_ID) {
                    id = ContentUris.parseId(uri);
                } else if (callerIsSyncAdapter && selection != null &&
                        selection.startsWith("_id=")) {
                        // The ContentProviderOperation generates an _id=n string instead of
                        // adding the id to the URL, so parse that out here.
                        id = Long.parseLong(selection.substring(4));
                } else {
                    throw new IllegalArgumentException(
                            "When updating Event, _id must be specified");
                }
                if (!callerIsSyncAdapter) {
                    values.put(Events._SYNC_DIRTY, 1);
                }
                // Disallow updating the attendee status in the Events
                // table.  In the future, we could support this but we
                // would have to query and update the attendees table
                // to keep the values consistent.
                if (values.containsKey(Events.SELF_ATTENDEE_STATUS)) {
                    throw new IllegalArgumentException("Updating "
                            + Events.SELF_ATTENDEE_STATUS
                            + " in Events table is not allowed.");
                }

                // TODO: should we allow this?
                if (values.containsKey(Events.HTML_URI) && !callerIsSyncAdapter) {
                    throw new IllegalArgumentException("Updating "
                            + Events.HTML_URI
                            + " in Events table is not allowed.");
                }

                updateBusyBitsLocked(id, values);

                ContentValues updatedValues = updateContentValuesFromEvent(values);
                if (updatedValues == null) {
                    Log.w(TAG, "Could not update event.");
                    return 0;
                }

                int result = mDb.update("Events", updatedValues, "_id=" + id,
                        null /* selectionArgs */);
                if (result > 0) {
                    updateEventRawTimesLocked(id, updatedValues);
                    updateInstancesLocked(updatedValues, id, false /* not a new event */, mDb);

                    if (values.containsKey(Events.DTSTART)) {
                        // The start time of the event changed, so run the
                        // event alarm scheduler.
                        if (Log.isLoggable(TAG, Log.DEBUG)) {
                            Log.d(TAG, "updateInternal() changing event");
                        }
                        scheduleNextAlarm(false /* do not remove alarms */);
                        triggerAppWidgetUpdate(id);
                    }
                }
                return result;
            }
            case ATTENDEES_ID: {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                // Copy the attendee status value to the Events table.
                updateEventAttendeeStatus(mDb, values);

                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.update("Attendees", values, "_id=" + id, null);
                } else {
                    return updateInTable("Attendees", values, uri, null /* selection */,
                            null /* selectionArgs */);
                }
            }
            case CALENDAR_ALERTS_ID: {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                // Note: dirty bit is not set for Alerts because it is not synced.
                // It is generated from Reminders, which is synced.
                long id = ContentUris.parseId(uri);
                return mDb.update("CalendarAlerts", values, "_id=" + id, null /* selectionArgs */);
            }
            case CALENDAR_ALERTS: {
                // Note: dirty bit is not set for Alerts because it is not synced.
                // It is generated from Reminders, which is synced.
                return mDb.update("CalendarAlerts", values, selection, selectionArgs);
            }
            case REMINDERS_ID: {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    count = mDb.update("Reminders", values, "_id=" + id, null /* selectionArgs */);
                } else {
                    count = updateInTable("Reminders", values, uri, null /* selection */,
                            null /* selectionArgs */);
                }

                // Reschedule the event alarms because the
                // "minutes" field may have changed.
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "updateInternal() changing reminder");
                }
                scheduleNextAlarm(false /* do not remove alarms */);
                return count;
            }
            case EXTENDED_PROPERTIES_ID: {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.update("ExtendedProperties", values, "_id=" + id,
                            null /* selectionArgs */);
                } else {
                    return updateInTable("ExtendedProperties", values, uri, null /* selection */,
                            null /* selectionArgs */);
                }
            }

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }
    }

    private String appendAccountToSelection(Uri uri, String selection) {
        final String accountName = uri.getQueryParameter(Calendar.Calendars._SYNC_ACCOUNT);
        final String accountType = uri.getQueryParameter(Calendar.Calendars._SYNC_ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName)) {
            StringBuilder selectionSb = new StringBuilder(Calendar.Calendars._SYNC_ACCOUNT + "="
                    + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                    + Calendar.Calendars._SYNC_ACCOUNT_TYPE + "="
                    + DatabaseUtils.sqlEscapeString(accountType));
            if (!TextUtils.isEmpty(selection)) {
                selectionSb.append(" AND (");
                selectionSb.append(selection);
                selectionSb.append(')');
            }
            return selectionSb.toString();
        } else {
            return selection;
        }
    }

    private void modifyCalendarSubscription(long id, boolean syncEvents) {
        // get the account, url, and current selected state
        // for this calendar.
        Cursor cursor = query(ContentUris.withAppendedId(Calendars.CONTENT_URI, id),
                new String[] {Calendars._SYNC_ACCOUNT, Calendars._SYNC_ACCOUNT_TYPE,
                        Calendars.URL, Calendars.SYNC_EVENTS},
                null /* selection */,
                null /* selectionArgs */,
                null /* sort */);

        Account account = null;
        String calendarUrl = null;
        boolean oldSyncEvents = false;
        if (cursor != null && cursor.moveToFirst()) {
            try {
                final String accountName = cursor.getString(0);
                final String accountType = cursor.getString(1);
                account = new Account(accountName, accountType);
                calendarUrl = cursor.getString(2);
                oldSyncEvents = (cursor.getInt(3) != 0);
            } finally {
                cursor.close();
            }
        }

        if (account == null || TextUtils.isEmpty(calendarUrl)) {
            // should not happen?
            Log.w(TAG, "Cannot update subscription because account "
                    + "or calendar url empty -- should not happen.");
            return;
        }

        if (oldSyncEvents == syncEvents) {
            // nothing to do
            return;
        }

        // If we are no longer syncing a calendar then make sure that the
        // old calendar sync data is cleared.  Then if we later add this
        // calendar back, we will sync all the events.
        if (!syncEvents) {
            // TODO: clear out the SyncState
//            byte[] data = readSyncDataBytes(account);
//            GDataSyncData syncData = AbstractGDataSyncAdapter.newGDataSyncDataFromBytes(data);
//            if (syncData != null) {
//                syncData.feedData.remove(calendarUrl);
//                data = AbstractGDataSyncAdapter.newBytesFromGDataSyncData(syncData);
//                writeSyncDataBytes(account, data);
//            }

            // Delete all of the events in this calendar to save space.
            // This is the closest we can come to deleting a calendar.
            // Clients should never actually delete a calendar.  That won't
            // work.  We need to keep the calendar entry in the Calendars table
            // in order to know not to sync the events for that calendar from
            // the server.
            String[] args = new String[] {Long.toString(id)};
            mDb.delete("Events", CALENDAR_ID_SELECTION, args);

            // TODO: cancel any pending/ongoing syncs for this calendar.

            // TODO: there is a corner case to deal with here: namely, if
            // we edit or delete an event on the phone and then remove
            // (that is, stop syncing) a calendar, and if we also make a
            // change on the server to that event at about the same time,
            // then we will never propagate the changes from the phone to
            // the server.
        }

        // If the calendar is not selected for syncing, then don't download
        // events.
        mDbHelper.scheduleSync(account, !syncEvents, calendarUrl);
    }

    // TODO: is this needed
//    @Override
//    public void onSyncStop(SyncContext context, boolean success) {
//        super.onSyncStop(context, success);
//        if (Log.isLoggable(TAG, Log.DEBUG)) {
//            Log.d(TAG, "onSyncStop() success: " + success);
//        }
//        scheduleNextAlarm(false /* do not remove alarms */);
//        triggerAppWidgetUpdate(-1);
//    }

    /**
     * Update any existing widgets with the changed events.
     *
     * @param changedEventId Specific event known to be changed, otherwise -1.
     *            If present, we use it to decide if an update is necessary.
     */
    private synchronized void triggerAppWidgetUpdate(long changedEventId) {
        Context context = getContext();
        if (context != null) {
            mAppWidgetProvider.providerUpdated(context, changedEventId);
        }
    }

    void bootCompleted() {
        // Remove alarms from the CalendarAlerts table that have been marked
        // as "scheduled" but not fired yet.  We do this because the
        // AlarmManagerService loses all information about alarms when the
        // power turns off but we store the information in a database table
        // that persists across reboots. See the documentation for
        // scheduleNextAlarmLocked() for more information.
        scheduleNextAlarm(true /* remove alarms */);
    }

    /* Retrieve and cache the alarm manager */
    private AlarmManager getAlarmManager() {
        synchronized(mAlarmLock) {
            if (mAlarmManager == null) {
                Context context = getContext();
                if (context == null) {
                    Log.e(TAG, "getAlarmManager() cannot get Context");
                    return null;
                }
                Object service = context.getSystemService(Context.ALARM_SERVICE);
                mAlarmManager = (AlarmManager) service;
            }
            return mAlarmManager;
        }
    }

    void scheduleNextAlarmCheck(long triggerTime) {
        AlarmManager manager = getAlarmManager();
        if (manager == null) {
            Log.e(TAG, "scheduleNextAlarmCheck() cannot get AlarmManager");
            return;
        }
        Context context = getContext();
        Intent intent = new Intent(CalendarReceiver.SCHEDULE);
        intent.setClass(context, CalendarReceiver.class);
        PendingIntent pending = PendingIntent.getBroadcast(context,
                0, intent, PendingIntent.FLAG_NO_CREATE);
        if (pending != null) {
            // Cancel any previous alarms that do the same thing.
            manager.cancel(pending);
        }
        pending = PendingIntent.getBroadcast(context,
                0, intent, PendingIntent.FLAG_CANCEL_CURRENT);

        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Time time = new Time();
            time.set(triggerTime);
            String timeStr = time.format(" %a, %b %d, %Y %I:%M%P");
            Log.d(TAG, "scheduleNextAlarmCheck at: " + triggerTime + timeStr);
        }

        manager.set(AlarmManager.RTC_WAKEUP, triggerTime, pending);
    }

    /*
     * This method runs the alarm scheduler in a background thread.
     */
    void scheduleNextAlarm(boolean removeAlarms) {
        Thread thread = new AlarmScheduler(removeAlarms);
        thread.start();
    }

    /**
     * This method runs in a background thread and schedules an alarm for
     * the next calendar event, if necessary.
     */
    private void runScheduleNextAlarm(boolean removeAlarms) {
        final SQLiteDatabase db = mDbHelper.getWritableDatabase();
        db.beginTransaction();
        try {
            if (removeAlarms) {
                removeScheduledAlarmsLocked(db);
            }
            scheduleNextAlarmLocked(db);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    /**
     * This method looks at the 24-hour window from now for any events that it
     * needs to schedule.  This method runs within a database transaction. It
     * also runs in a background thread.
     *
     * The CalendarProvider2 keeps track of which alarms it has already scheduled
     * to avoid scheduling them more than once and for debugging problems with
     * alarms.  It stores this knowledge in a database table called CalendarAlerts
     * which persists across reboots.  But the actual alarm list is in memory
     * and disappears if the phone loses power.  To avoid missing an alarm, we
     * clear the entries in the CalendarAlerts table when we start up the
     * CalendarProvider2.
     *
     * Scheduling an alarm multiple times is not tragic -- we filter out the
     * extra ones when we receive them. But we still need to keep track of the
     * scheduled alarms. The main reason is that we need to prevent multiple
     * notifications for the same alarm (on the receive side) in case we
     * accidentally schedule the same alarm multiple times.  We don't have
     * visibility into the system's alarm list so we can never know for sure if
     * we have already scheduled an alarm and it's better to err on scheduling
     * an alarm twice rather than missing an alarm.  Another reason we keep
     * track of scheduled alarms in a database table is that it makes it easy to
     * run an SQL query to find the next reminder that we haven't scheduled.
     *
     * @param db the database
     */
    private void scheduleNextAlarmLocked(SQLiteDatabase db) {
        AlarmManager alarmManager = getAlarmManager();
        if (alarmManager == null) {
            Log.e(TAG, "Failed to find the AlarmManager. Could not schedule the next alarm!");
            return;
        }

        final long currentMillis = System.currentTimeMillis();
        final long start = currentMillis - SCHEDULE_ALARM_SLACK;
        final long end = start + (24 * 60 * 60 * 1000);
        ContentResolver cr = getContext().getContentResolver();
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Time time = new Time();
            time.set(start);
            String startTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
            Log.d(TAG, "runScheduleNextAlarm() start search: " + startTimeStr);
        }

        // Clear old alarms but keep alarms around for a while to prevent
        // multiple alerts for the same reminder.  The "clearUpToTime'
        // should be further in the past than the point in time where
        // we start searching for events (the "start" variable defined above).
        long clearUpToTime = currentMillis - CLEAR_OLD_ALARM_THRESHOLD;
        db.delete("CalendarAlerts", CalendarAlerts.ALARM_TIME + "<" + clearUpToTime, null);

        long nextAlarmTime = end;
        long alarmTime = CalendarAlerts.findNextAlarmTime(cr, currentMillis);
        if (alarmTime != -1 && alarmTime < nextAlarmTime) {
            nextAlarmTime = alarmTime;
        }

        // Extract events from the database sorted by alarm time.  The
        // alarm times are computed from Instances.begin (whose units
        // are milliseconds) and Reminders.minutes (whose units are
        // minutes).
        //
        // Also, ignore events whose end time is already in the past.
        // Also, ignore events alarms that we have already scheduled.
        //
        // Note 1: we can add support for the case where Reminders.minutes
        // equals -1 to mean use Calendars.minutes by adding a UNION for
        // that case where the two halves restrict the WHERE clause on
        // Reminders.minutes != -1 and Reminders.minutes = 1, respectively.
        //
        // Note 2: we have to name "myAlarmTime" different from the
        // "alarmTime" column in CalendarAlerts because otherwise the
        // query won't find multiple alarms for the same event.
        String query = "SELECT begin-(minutes*60000) AS myAlarmTime,"
                + " Instances.event_id AS eventId, begin, end,"
                + " title, allDay, method, minutes"
                + " FROM Instances INNER JOIN Events"
                + " ON (Events._id = Instances.event_id)"
                + " INNER JOIN Reminders"
                + " ON (Instances.event_id = Reminders.event_id)"
                + " WHERE method=" + Reminders.METHOD_ALERT
                + " AND myAlarmTime>=" + start
                + " AND myAlarmTime<=" + nextAlarmTime
                + " AND end>=" + currentMillis
                + " AND 0=(SELECT count(*) from CalendarAlerts CA"
                + " where CA.event_id=Instances.event_id AND CA.begin=Instances.begin"
                + " AND CA.alarmTime=myAlarmTime)"
                + " ORDER BY myAlarmTime,begin,title";

        acquireInstanceRangeLocked(start, end, false /* don't use minimum expansion windows */);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(query, null);

            int beginIndex = cursor.getColumnIndex(Instances.BEGIN);
            int endIndex = cursor.getColumnIndex(Instances.END);
            int eventIdIndex = cursor.getColumnIndex("eventId");
            int alarmTimeIndex = cursor.getColumnIndex("myAlarmTime");
            int minutesIndex = cursor.getColumnIndex(Reminders.MINUTES);

            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Time time = new Time();
                time.set(nextAlarmTime);
                String alarmTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                Log.d(TAG, "nextAlarmTime: " + alarmTimeStr
                        + " cursor results: " + cursor.getCount()
                        + " query: " + query);
            }

            while (cursor.moveToNext()) {
                // Schedule all alarms whose alarm time is as early as any
                // scheduled alarm.  For example, if the earliest alarm is at
                // 1pm, then we will schedule all alarms that occur at 1pm
                // but no alarms that occur later than 1pm.
                // Actually, we allow alarms up to a minute later to also
                // be scheduled so that we don't have to check immediately
                // again after an event alarm goes off.
                alarmTime = cursor.getLong(alarmTimeIndex);
                long eventId = cursor.getLong(eventIdIndex);
                int minutes = cursor.getInt(minutesIndex);
                long startTime = cursor.getLong(beginIndex);

                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    int titleIndex = cursor.getColumnIndex(Events.TITLE);
                    String title = cursor.getString(titleIndex);
                    Time time = new Time();
                    time.set(alarmTime);
                    String schedTime = time.format(" %a, %b %d, %Y %I:%M%P");
                    time.set(startTime);
                    String startTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                    long endTime = cursor.getLong(endIndex);
                    time.set(endTime);
                    String endTimeStr = time.format(" - %a, %b %d, %Y %I:%M%P");
                    time.set(currentMillis);
                    String currentTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                    Log.d(TAG, "  looking at id: " + eventId + " " + title
                            + " " + startTime
                            + startTimeStr + endTimeStr + " alarm: "
                            + alarmTime + schedTime
                            + " currentTime: " + currentTimeStr);
                }

                if (alarmTime < nextAlarmTime) {
                    nextAlarmTime = alarmTime;
                } else if (alarmTime >
                           nextAlarmTime + android.text.format.DateUtils.MINUTE_IN_MILLIS) {
                    // This event alarm (and all later ones) will be scheduled
                    // later.
                    break;
                }

                // Avoid an SQLiteContraintException by checking if this alarm
                // already exists in the table.
                if (CalendarAlerts.alarmExists(cr, eventId, startTime, alarmTime)) {
                    if (Log.isLoggable(TAG, Log.DEBUG)) {
                        int titleIndex = cursor.getColumnIndex(Events.TITLE);
                        String title = cursor.getString(titleIndex);
                        Log.d(TAG, "  alarm exists for id: " + eventId + " " + title);
                    }
                    continue;
                }

                // Insert this alarm into the CalendarAlerts table
                long endTime = cursor.getLong(endIndex);
                Uri uri = CalendarAlerts.insert(cr, eventId, startTime,
                        endTime, alarmTime, minutes);
                if (uri == null) {
                    Log.e(TAG, "runScheduleNextAlarm() insert into CalendarAlerts table failed");
                    continue;
                }

                Intent intent = new Intent(android.provider.Calendar.EVENT_REMINDER_ACTION);
                intent.setData(uri);

                // Also include the begin and end time of this event, because
                // we cannot determine that from the Events database table.
                intent.putExtra(android.provider.Calendar.EVENT_BEGIN_TIME, startTime);
                intent.putExtra(android.provider.Calendar.EVENT_END_TIME, endTime);
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    int titleIndex = cursor.getColumnIndex(Events.TITLE);
                    String title = cursor.getString(titleIndex);
                    Time time = new Time();
                    time.set(alarmTime);
                    String schedTime = time.format(" %a, %b %d, %Y %I:%M%P");
                    time.set(startTime);
                    String startTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                    time.set(endTime);
                    String endTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                    time.set(currentMillis);
                    String currentTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                    Log.d(TAG, "  scheduling " + title
                            + startTimeStr  + " - " + endTimeStr + " alarm: " + schedTime
                            + " currentTime: " + currentTimeStr
                            + " uri: " + uri);
                }
                PendingIntent sender = PendingIntent.getBroadcast(getContext(),
                        0, intent, PendingIntent.FLAG_CANCEL_CURRENT);
                alarmManager.set(AlarmManager.RTC_WAKEUP, alarmTime, sender);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // If we scheduled an event alarm, then schedule the next alarm check
        // for one minute past that alarm.  Otherwise, if there were no
        // event alarms scheduled, then check again in 24 hours.  If a new
        // event is inserted before the next alarm check, then this method
        // will be run again when the new event is inserted.
        if (nextAlarmTime != Long.MAX_VALUE) {
            scheduleNextAlarmCheck(nextAlarmTime + android.text.format.DateUtils.MINUTE_IN_MILLIS);
        } else {
            scheduleNextAlarmCheck(currentMillis + android.text.format.DateUtils.DAY_IN_MILLIS);
        }
    }

    /**
     * Removes the entries in the CalendarAlerts table for alarms that we have
     * scheduled but that have not fired yet. We do this to ensure that we
     * don't miss an alarm.  The CalendarAlerts table keeps track of the
     * alarms that we have scheduled but the actual alarm list is in memory
     * and will be cleared if the phone reboots.
     *
     * We don't need to remove entries that have already fired, and in fact
     * we should not remove them because we need to display the notifications
     * until the user dismisses them.
     *
     * We could remove entries that have fired and been dismissed, but we leave
     * them around for a while because it makes it easier to debug problems.
     * Entries that are old enough will be cleaned up later when we schedule
     * new alarms.
     */
    private void removeScheduledAlarmsLocked(SQLiteDatabase db) {
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Log.d(TAG, "removing scheduled alarms");
        }
        db.delete(CalendarAlerts.TABLE_NAME,
                CalendarAlerts.STATE + "=" + CalendarAlerts.SCHEDULED, null /* whereArgs */);
    }

    private static String sEventsTable = "Events";
    private static String sAttendeesTable = "Attendees";
    private static String sRemindersTable = "Reminders";
    private static String sCalendarAlertsTable = "CalendarAlerts";
    private static String sExtendedPropertiesTable = "ExtendedProperties";

    private static final int EVENTS = 1;
    private static final int EVENTS_ID = 2;
    private static final int INSTANCES = 3;
    private static final int DELETED_EVENTS = 4;
    private static final int CALENDARS = 5;
    private static final int CALENDARS_ID = 6;
    private static final int ATTENDEES = 7;
    private static final int ATTENDEES_ID = 8;
    private static final int REMINDERS = 9;
    private static final int REMINDERS_ID = 10;
    private static final int EXTENDED_PROPERTIES = 11;
    private static final int EXTENDED_PROPERTIES_ID = 12;
    private static final int CALENDAR_ALERTS = 13;
    private static final int CALENDAR_ALERTS_ID = 14;
    private static final int CALENDAR_ALERTS_BY_INSTANCE = 15;
    private static final int BUSYBITS = 16;
    private static final int INSTANCES_BY_DAY = 17;
    private static final int SYNCSTATE = 18;
    private static final int SYNCSTATE_ID = 19;

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
    private static final HashMap<String, String> sInstancesProjectionMap;
    private static final HashMap<String, String> sEventsProjectionMap;
    private static final HashMap<String, String> sAttendeesProjectionMap;
    private static final HashMap<String, String> sRemindersProjectionMap;
    private static final HashMap<String, String> sCalendarAlertsProjectionMap;
    private static final HashMap<String, String> sBusyBitsProjectionMap;

    static {
        sUriMatcher.addURI("calendar", "instances/when/*/*", INSTANCES);
        sUriMatcher.addURI("calendar", "instances/whenbyday/*/*", INSTANCES_BY_DAY);
        sUriMatcher.addURI("calendar", "events", EVENTS);
        sUriMatcher.addURI("calendar", "events/#", EVENTS_ID);
        sUriMatcher.addURI("calendar", "calendars", CALENDARS);
        sUriMatcher.addURI("calendar", "calendars/#", CALENDARS_ID);
        sUriMatcher.addURI("calendar", "deleted_events", DELETED_EVENTS);
        sUriMatcher.addURI("calendar", "attendees", ATTENDEES);
        sUriMatcher.addURI("calendar", "attendees/#", ATTENDEES_ID);
        sUriMatcher.addURI("calendar", "reminders", REMINDERS);
        sUriMatcher.addURI("calendar", "reminders/#", REMINDERS_ID);
        sUriMatcher.addURI("calendar", "extendedproperties", EXTENDED_PROPERTIES);
        sUriMatcher.addURI("calendar", "extendedproperties/#", EXTENDED_PROPERTIES_ID);
        sUriMatcher.addURI("calendar", "calendar_alerts", CALENDAR_ALERTS);
        sUriMatcher.addURI("calendar", "calendar_alerts/#", CALENDAR_ALERTS_ID);
        sUriMatcher.addURI("calendar", "calendar_alerts/by_instance", CALENDAR_ALERTS_BY_INSTANCE);
        sUriMatcher.addURI("calendar", "busybits/when/*/*", BUSYBITS);
        sUriMatcher.addURI("calendar", SyncStateContentProviderHelper.PATH, SYNCSTATE);
        sUriMatcher.addURI("calendar", SyncStateContentProviderHelper.PATH + "/#", SYNCSTATE_ID);

        sEventsProjectionMap = new HashMap<String, String>();
        // Events columns
        sEventsProjectionMap.put(Events.HTML_URI, "htmlUri");
        sEventsProjectionMap.put(Events.TITLE, "title");
        sEventsProjectionMap.put(Events.EVENT_LOCATION, "eventLocation");
        sEventsProjectionMap.put(Events.DESCRIPTION, "description");
        sEventsProjectionMap.put(Events.STATUS, "eventStatus");
        sEventsProjectionMap.put(Events.SELF_ATTENDEE_STATUS, "selfAttendeeStatus");
        sEventsProjectionMap.put(Events.COMMENTS_URI, "commentsUri");
        sEventsProjectionMap.put(Events.DTSTART, "dtstart");
        sEventsProjectionMap.put(Events.DTEND, "dtend");
        sEventsProjectionMap.put(Events.EVENT_TIMEZONE, "eventTimezone");
        sEventsProjectionMap.put(Events.DURATION, "duration");
        sEventsProjectionMap.put(Events.ALL_DAY, "allDay");
        sEventsProjectionMap.put(Events.VISIBILITY, "visibility");
        sEventsProjectionMap.put(Events.TRANSPARENCY, "transparency");
        sEventsProjectionMap.put(Events.HAS_ALARM, "hasAlarm");
        sEventsProjectionMap.put(Events.HAS_EXTENDED_PROPERTIES, "hasExtendedProperties");
        sEventsProjectionMap.put(Events.RRULE, "rrule");
        sEventsProjectionMap.put(Events.RDATE, "rdate");
        sEventsProjectionMap.put(Events.EXRULE, "exrule");
        sEventsProjectionMap.put(Events.EXDATE, "exdate");
        sEventsProjectionMap.put(Events.ORIGINAL_EVENT, "originalEvent");
        sEventsProjectionMap.put(Events.ORIGINAL_INSTANCE_TIME, "originalInstanceTime");
        sEventsProjectionMap.put(Events.ORIGINAL_ALL_DAY, "originalAllDay");
        sEventsProjectionMap.put(Events.LAST_DATE, "lastDate");
        sEventsProjectionMap.put(Events.HAS_ATTENDEE_DATA, "hasAttendeeData");
        sEventsProjectionMap.put(Events.CALENDAR_ID, "calendar_id");
        sEventsProjectionMap.put(Events.GUESTS_CAN_INVITE_OTHERS, "guestsCanInviteOthers");
        sEventsProjectionMap.put(Events.GUESTS_CAN_MODIFY, "guestsCanModify");
        sEventsProjectionMap.put(Events.GUESTS_CAN_SEE_GUESTS, "guestsCanSeeGuests");
        sEventsProjectionMap.put(Events.ORGANIZER, "organizer");
        sEventsProjectionMap.put(Events.DELETED, "deleted");

        // Calendar columns
        sEventsProjectionMap.put(Events.COLOR, "color");
        sEventsProjectionMap.put(Events.ACCESS_LEVEL, "access_level");
        sEventsProjectionMap.put(Events.SELECTED, "selected");
        sEventsProjectionMap.put(Calendars.URL, "url");
        sEventsProjectionMap.put(Calendars.TIMEZONE, "timezone");
        sEventsProjectionMap.put(Calendars.OWNER_ACCOUNT, "ownerAccount");

        // Put the shared items into the Instances projection map
        sInstancesProjectionMap = new HashMap<String, String>(sEventsProjectionMap);
        sAttendeesProjectionMap = new HashMap<String, String>(sEventsProjectionMap);
        sRemindersProjectionMap = new HashMap<String, String>(sEventsProjectionMap);
        sCalendarAlertsProjectionMap = new HashMap<String, String>(sEventsProjectionMap);

        sEventsProjectionMap.put(Events._ID, "Events._id AS _id");
        sEventsProjectionMap.put(Events._SYNC_ID, "Events._sync_id AS _sync_id");
        sEventsProjectionMap.put(Events._SYNC_VERSION, "Events._sync_version AS _sync_version");
        sEventsProjectionMap.put(Events._SYNC_TIME, "Events._sync_time AS _sync_time");
        sEventsProjectionMap.put(Events._SYNC_LOCAL_ID, "Events._sync_local_id AS _sync_local_id");
        sEventsProjectionMap.put(Events._SYNC_DIRTY, "Events._sync_dirty AS _sync_dirty");
        sEventsProjectionMap.put(Events._SYNC_ACCOUNT, "Events._sync_account AS _sync_account");
        sEventsProjectionMap.put(Events._SYNC_ACCOUNT_TYPE,
                "Events._sync_account_type AS _sync_account_type");

        // Instances columns
        sInstancesProjectionMap.put(Instances.BEGIN, "begin");
        sInstancesProjectionMap.put(Instances.END, "end");
        sInstancesProjectionMap.put(Instances.EVENT_ID, "Instances.event_id AS event_id");
        sInstancesProjectionMap.put(Instances._ID, "Instances._id AS _id");
        sInstancesProjectionMap.put(Instances.START_DAY, "startDay");
        sInstancesProjectionMap.put(Instances.END_DAY, "endDay");
        sInstancesProjectionMap.put(Instances.START_MINUTE, "startMinute");
        sInstancesProjectionMap.put(Instances.END_MINUTE, "endMinute");

        // BusyBits columns
        sBusyBitsProjectionMap = new HashMap<String, String>();
        sBusyBitsProjectionMap.put(BusyBits.DAY, "day");
        sBusyBitsProjectionMap.put(BusyBits.BUSYBITS, "busyBits");
        sBusyBitsProjectionMap.put(BusyBits.ALL_DAY_COUNT, "allDayCount");

        // Attendees columns
        sAttendeesProjectionMap.put(Attendees.EVENT_ID, "event_id");
        sAttendeesProjectionMap.put(Attendees._ID, "Attendees._id AS _id");
        sAttendeesProjectionMap.put(Attendees.ATTENDEE_NAME, "attendeeName");
        sAttendeesProjectionMap.put(Attendees.ATTENDEE_EMAIL, "attendeeEmail");
        sAttendeesProjectionMap.put(Attendees.ATTENDEE_STATUS, "attendeeStatus");
        sAttendeesProjectionMap.put(Attendees.ATTENDEE_RELATIONSHIP, "attendeeRelationship");
        sAttendeesProjectionMap.put(Attendees.ATTENDEE_TYPE, "attendeeType");

        // Reminders columns
        sRemindersProjectionMap.put(Reminders.EVENT_ID, "event_id");
        sRemindersProjectionMap.put(Reminders._ID, "Reminders._id AS _id");
        sRemindersProjectionMap.put(Reminders.MINUTES, "minutes");
        sRemindersProjectionMap.put(Reminders.METHOD, "method");

        // CalendarAlerts columns
        sCalendarAlertsProjectionMap.put(CalendarAlerts.EVENT_ID, "event_id");
        sCalendarAlertsProjectionMap.put(CalendarAlerts._ID, "CalendarAlerts._id AS _id");
        sCalendarAlertsProjectionMap.put(CalendarAlerts.BEGIN, "begin");
        sCalendarAlertsProjectionMap.put(CalendarAlerts.END, "end");
        sCalendarAlertsProjectionMap.put(CalendarAlerts.ALARM_TIME, "alarmTime");
        sCalendarAlertsProjectionMap.put(CalendarAlerts.STATE, "state");
        sCalendarAlertsProjectionMap.put(CalendarAlerts.MINUTES, "minutes");
    }

    /**
     * An implementation of EntityIterator that builds the Entity for a calendar event.
     */
    private static class CalendarEntityIterator implements EntityIterator {
        private final Cursor mEntityCursor;
        private volatile boolean mIsClosed;
        private final SQLiteDatabase mDb;

        private static final String[] EVENTS_PROJECTION = new String[]{
                Calendar.Events._ID,
                Calendar.Events.HTML_URI,
                Calendar.Events.TITLE,
                Calendar.Events.DESCRIPTION,
                Calendar.Events.EVENT_LOCATION,
                Calendar.Events.STATUS,
                Calendar.Events.SELF_ATTENDEE_STATUS,
                Calendar.Events.COMMENTS_URI,
                Calendar.Events.DTSTART,
                Calendar.Events.DTEND,
                Calendar.Events.DURATION,
                Calendar.Events.EVENT_TIMEZONE,
                Calendar.Events.ALL_DAY,
                Calendar.Events.VISIBILITY,
                Calendar.Events.TRANSPARENCY,
                Calendar.Events.HAS_ALARM,
                Calendar.Events.HAS_EXTENDED_PROPERTIES,
                Calendar.Events.RRULE,
                Calendar.Events.RDATE,
                Calendar.Events.EXRULE,
                Calendar.Events.EXDATE,
                Calendar.Events.ORIGINAL_EVENT,
                Calendar.Events.ORIGINAL_INSTANCE_TIME,
                Calendar.Events.ORIGINAL_ALL_DAY,
                Calendar.Events.LAST_DATE,
                Calendar.Events.HAS_ATTENDEE_DATA,
                Calendar.Events.CALENDAR_ID,
                Calendar.Events.GUESTS_CAN_INVITE_OTHERS,
                Calendar.Events.GUESTS_CAN_MODIFY,
                Calendar.Events.GUESTS_CAN_SEE_GUESTS,
                Calendar.Events.ORGANIZER,
                Calendar.Events.DELETED,
                Calendar.Events._SYNC_ID,
                Calendar.Events._SYNC_VERSION,
                Calendar.Calendars.URL,
        };
        private static final int COLUMN_ID = 0;
        private static final int COLUMN_HTML_URI = 1;
        private static final int COLUMN_TITLE = 2;
        private static final int COLUMN_DESCRIPTION = 3;
        private static final int COLUMN_EVENT_LOCATION = 4;
        private static final int COLUMN_STATUS = 5;
        private static final int COLUMN_SELF_ATTENDEE_STATUS = 6;
        private static final int COLUMN_COMMENTS_URI = 7;
        private static final int COLUMN_DTSTART = 8;
        private static final int COLUMN_DTEND = 9;
        private static final int COLUMN_DURATION = 10;
        private static final int COLUMN_EVENT_TIMEZONE = 11;
        private static final int COLUMN_ALL_DAY = 12;
        private static final int COLUMN_VISIBILITY = 13;
        private static final int COLUMN_TRANSPARENCY = 14;
        private static final int COLUMN_HAS_ALARM = 15;
        private static final int COLUMN_HAS_EXTENDED_PROPERTIES = 16;
        private static final int COLUMN_RRULE = 17;
        private static final int COLUMN_RDATE = 18;
        private static final int COLUMN_EXRULE = 19;
        private static final int COLUMN_EXDATE = 20;
        private static final int COLUMN_ORIGINAL_EVENT = 21;
        private static final int COLUMN_ORIGINAL_INSTANCE_TIME = 22;
        private static final int COLUMN_ORIGINAL_ALL_DAY = 23;
        private static final int COLUMN_LAST_DATE = 24;
        private static final int COLUMN_HAS_ATTENDEE_DATA = 25;
        private static final int COLUMN_CALENDAR_ID = 26;
        private static final int COLUMN_GUESTS_CAN_INVITE_OTHERS = 27;
        private static final int COLUMN_GUESTS_CAN_MODIFY = 28;
        private static final int COLUMN_GUESTS_CAN_SEE_GUESTS = 29;
        private static final int COLUMN_ORGANIZER = 30;
        private static final int COLUMN_DELETED = 31;
        private static final int COLUMN_SYNC_ID = 32;
        private static final int COLUMN_SYNC_VERSION = 33;
        private static final int COLUMN_CALENDAR_URL = 34;

        private static final String[] REMINDERS_PROJECTION = new String[] {
                Calendar.Reminders.MINUTES,
                Calendar.Reminders.METHOD,
        };
        private static final int COLUMN_MINUTES = 0;
        private static final int COLUMN_METHOD = 1;

        private static final String[] ATTENDEES_PROJECTION = new String[] {
                Calendar.Attendees.ATTENDEE_NAME,
                Calendar.Attendees.ATTENDEE_EMAIL,
                Calendar.Attendees.ATTENDEE_RELATIONSHIP,
                Calendar.Attendees.ATTENDEE_TYPE,
                Calendar.Attendees.ATTENDEE_STATUS,
        };
        private static final int COLUMN_ATTENDEE_NAME = 0;
        private static final int COLUMN_ATTENDEE_EMAIL = 1;
        private static final int COLUMN_ATTENDEE_RELATIONSHIP = 2;
        private static final int COLUMN_ATTENDEE_TYPE = 3;
        private static final int COLUMN_ATTENDEE_STATUS = 4;
        private static final String[] EXTENDED_PROJECTION = new String[] {
                Calendar.ExtendedProperties.NAME,
                Calendar.ExtendedProperties.VALUE,
        };
        private static final int COLUMN_NAME = 0;
        private static final int COLUMN_VALUE = 1;

        public CalendarEntityIterator(CalendarProvider2 provider, String eventIdString, Uri uri,
                String selection, String[] selectionArgs, String sortOrder) {
            mIsClosed = false;
            mDb = provider.mDbHelper.getReadableDatabase();
            final SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
            qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
            if (eventIdString != null) {
                qb.appendWhere("_id=" + eventIdString);
            }
            mEntityCursor = qb.query(mDb, EVENTS_PROJECTION, selection, selectionArgs,
                    null /* groupBy */, null /* having */, sortOrder);
            mEntityCursor.moveToFirst();
        }

        public void close() {
            if (mIsClosed) {
                throw new IllegalStateException("closing when already closed");
            }
            mIsClosed = true;
            mEntityCursor.close();
        }

        public boolean hasNext() throws RemoteException {

            if (mIsClosed) {
                throw new IllegalStateException("calling hasNext() when the iterator is closed");
            }

            return !mEntityCursor.isAfterLast();
        }

        public void reset() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling next() when the iterator is closed");
            }
            mEntityCursor.moveToFirst();
        }

        public Entity next() throws RemoteException {
            if (mIsClosed) {
                throw new IllegalStateException("calling next() when the iterator is closed");
            }
            if (!hasNext()) {
                throw new IllegalStateException("you may only call next() if hasNext() is true");
            }

            final SQLiteCursor c = (SQLiteCursor) mEntityCursor;
            final long eventId = c.getLong(COLUMN_ID);

            // we expect the cursor is already at the row we need to read from
            ContentValues entityValues = new ContentValues();
            entityValues.put(Calendar.Events._ID, eventId);
            entityValues.put(Calendar.Events.CALENDAR_ID, c.getInt(COLUMN_CALENDAR_ID));
            entityValues.put(Calendar.Events.HTML_URI, c.getString(COLUMN_HTML_URI));
            entityValues.put(Calendar.Events.TITLE, c.getString(COLUMN_TITLE));
            entityValues.put(Calendar.Events.DESCRIPTION, c.getString(COLUMN_DESCRIPTION));
            entityValues.put(Calendar.Events.EVENT_LOCATION, c.getString(COLUMN_EVENT_LOCATION));
            entityValues.put(Calendar.Events.STATUS, c.getInt(COLUMN_STATUS));
            entityValues.put(Calendar.Events.SELF_ATTENDEE_STATUS,
                    c.getInt(COLUMN_SELF_ATTENDEE_STATUS));
            entityValues.put(Calendar.Events.COMMENTS_URI, c.getString(COLUMN_COMMENTS_URI));
            entityValues.put(Calendar.Events.DTSTART, c.getLong(COLUMN_DTSTART));
            entityValues.put(Calendar.Events.DTEND, c.getLong(COLUMN_DTEND));
            entityValues.put(Calendar.Events.DURATION, c.getString(COLUMN_DURATION));
            entityValues.put(Calendar.Events.EVENT_TIMEZONE, c.getString(COLUMN_EVENT_TIMEZONE));
            entityValues.put(Calendar.Events.ALL_DAY, c.getString(COLUMN_ALL_DAY));
            entityValues.put(Calendar.Events.VISIBILITY, c.getInt(COLUMN_VISIBILITY));
            entityValues.put(Calendar.Events.TRANSPARENCY, c.getInt(COLUMN_TRANSPARENCY));
            entityValues.put(Calendar.Events.HAS_ALARM, c.getString(COLUMN_HAS_ALARM));
            entityValues.put(Calendar.Events.HAS_EXTENDED_PROPERTIES,
                    c.getString(COLUMN_HAS_EXTENDED_PROPERTIES));
            entityValues.put(Calendar.Events.RRULE, c.getString(COLUMN_RRULE));
            entityValues.put(Calendar.Events.RDATE, c.getString(COLUMN_RDATE));
            entityValues.put(Calendar.Events.EXRULE, c.getString(COLUMN_EXRULE));
            entityValues.put(Calendar.Events.EXDATE, c.getString(COLUMN_EXDATE));
            entityValues.put(Calendar.Events.ORIGINAL_EVENT, c.getString(COLUMN_ORIGINAL_EVENT));
            entityValues.put(Calendar.Events.ORIGINAL_INSTANCE_TIME,
                    c.getLong(COLUMN_ORIGINAL_INSTANCE_TIME));
            entityValues.put(Calendar.Events.ORIGINAL_ALL_DAY, c.getInt(COLUMN_ORIGINAL_ALL_DAY));
            entityValues.put(Calendar.Events.LAST_DATE, c.getLong(COLUMN_LAST_DATE));
            entityValues.put(Calendar.Events.HAS_ATTENDEE_DATA,
                             c.getInt(COLUMN_HAS_ATTENDEE_DATA));
            entityValues.put(Calendar.Events.GUESTS_CAN_INVITE_OTHERS,
                    c.getInt(COLUMN_GUESTS_CAN_INVITE_OTHERS));
            entityValues.put(Calendar.Events.GUESTS_CAN_MODIFY,
                    c.getInt(COLUMN_GUESTS_CAN_MODIFY));
            entityValues.put(Calendar.Events.GUESTS_CAN_SEE_GUESTS,
                    c.getInt(COLUMN_GUESTS_CAN_SEE_GUESTS));
            entityValues.put(Calendar.Events.ORGANIZER, c.getString(COLUMN_ORGANIZER));
            entityValues.put(Calendar.Calendars.SOURCE_ID, c.getString(COLUMN_SYNC_ID));
            entityValues.put(Calendar.Calendars._SYNC_ID, c.getString(COLUMN_SYNC_ID));
            entityValues.put(Calendar.Events._SYNC_VERSION, c.getString(COLUMN_SYNC_VERSION));
            entityValues.put(Calendar.Events.DELETED,
                    c.getInt(COLUMN_DELETED));
            entityValues.put(Calendars.URL, c.getString(COLUMN_CALENDAR_URL));

            Entity entity = new Entity(entityValues);
            Cursor cursor = null;
            try {
                cursor = mDb.query(sRemindersTable, REMINDERS_PROJECTION, "event_id=" + eventId,
                        null /* selectionArgs */, null /* groupBy */,
                        null /* having */, null /* sortOrder */);
                while (cursor.moveToNext()) {
                    ContentValues reminderValues = new ContentValues();
                    reminderValues.put(Calendar.Reminders.MINUTES, cursor.getInt(COLUMN_MINUTES));
                    reminderValues.put(Calendar.Reminders.METHOD, cursor.getInt(COLUMN_METHOD));
                    entity.addSubValue(Calendar.Reminders.CONTENT_URI, reminderValues);
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }

            cursor = null;
            try {
                cursor = mDb.query(sAttendeesTable, ATTENDEES_PROJECTION, "event_id=" + eventId,
                        null /* selectionArgs */, null /* groupBy */,
                        null /* having */, null /* sortOrder */);
                while (cursor.moveToNext()) {
                    ContentValues attendeeValues = new ContentValues();
                    attendeeValues.put(Calendar.Attendees.ATTENDEE_NAME,
                            cursor.getString(COLUMN_ATTENDEE_NAME));
                    attendeeValues.put(Calendar.Attendees.ATTENDEE_EMAIL,
                            cursor.getString(COLUMN_ATTENDEE_EMAIL));
                    attendeeValues.put(Calendar.Attendees.ATTENDEE_RELATIONSHIP,
                            cursor.getInt(COLUMN_ATTENDEE_RELATIONSHIP));
                    attendeeValues.put(Calendar.Attendees.ATTENDEE_TYPE,
                            cursor.getInt(COLUMN_ATTENDEE_TYPE));
                    attendeeValues.put(Calendar.Attendees.ATTENDEE_STATUS,
                            cursor.getInt(COLUMN_ATTENDEE_STATUS));
                    entity.addSubValue(Calendar.Attendees.CONTENT_URI, attendeeValues);
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }

            cursor = null;
            try {
                cursor = mDb.query(sExtendedPropertiesTable, EXTENDED_PROJECTION,
                        "event_id=" + eventId,
                        null /* selectionArgs */, null /* groupBy */,
                        null /* having */, null /* sortOrder */);
                while (cursor.moveToNext()) {
                    ContentValues extendedValues = new ContentValues();
                    extendedValues.put(Calendar.ExtendedProperties.NAME, c.getString(COLUMN_NAME));
                    extendedValues.put(Calendar.ExtendedProperties.VALUE,
                            c.getString(COLUMN_VALUE));
                    entity.addSubValue(Calendar.ExtendedProperties.CONTENT_URI, extendedValues);
                }
            } finally {
                if (cursor != null) {
                    cursor.close();
                }
            }

            mEntityCursor.moveToNext();
            // add the data to the contact
            return entity;
        }
    }

    @Override
    public EntityIterator queryEntities(Uri uri, String selection, String[] selectionArgs,
            String sortOrder) {
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case EVENTS:
            case EVENTS_ID:
                String calendarId = null;
                if (match == EVENTS_ID) {
                    calendarId = uri.getPathSegments().get(1);
                }

                return new CalendarEntityIterator(this, calendarId,
                        uri, selection, selectionArgs, sortOrder);
            default:
                throw new UnsupportedOperationException("Unknown uri: " + uri);
        }
    }

    /**
     * Make sure that there are no entries for accounts that no longer
     * exist. We are overriding this since we need to delete from the
     * Calendars table, which is not syncable, which has triggers that
     * will delete from the Events and  tables, which are
     * syncable.  TODO: update comment, make sure deletes don't get synced.
     */
    public void onAccountsUpdated(Account[] accounts) {
        mDb = mDbHelper.getWritableDatabase();
        if (mDb == null) return;

        Map<Account, Boolean> accountHasCalendar = Maps.newHashMap();
        Set<Account> validAccounts = Sets.newHashSet();
        for (Account account : accounts) {
            validAccounts.add(new Account(account.name, account.type));
            accountHasCalendar.put(account, false);
        }
        ArrayList<Account> accountsToDelete = new ArrayList<Account>();

        mDb.beginTransaction();
        try {

            for (String table : new String[]{"Calendars"}) {
                // Find all the accounts the contacts DB knows about, mark the ones that aren't
                // in the valid set for deletion.
                Cursor c = mDb.rawQuery("SELECT DISTINCT " + CalendarDatabaseHelper.ACCOUNT_NAME
                                        + ","
                                        + CalendarDatabaseHelper.ACCOUNT_TYPE + " from "
                        + table, null);
                while (c.moveToNext()) {
                    if (c.getString(0) != null && c.getString(1) != null) {
                        Account currAccount = new Account(c.getString(0), c.getString(1));
                        if (!validAccounts.contains(currAccount)) {
                            accountsToDelete.add(currAccount);
                        }
                    }
                }
                c.close();
            }

            for (Account account : accountsToDelete) {
                Log.d(TAG, "removing data for removed account " + account);
                String[] params = new String[]{account.name, account.type};
                mDb.execSQL("DELETE FROM Calendars"
                        + " WHERE " + CalendarDatabaseHelper.ACCOUNT_NAME + "= ? AND "
                        + CalendarDatabaseHelper.ACCOUNT_TYPE
                        + "= ?", params);
            }
            mDbHelper.getSyncState().onAccountsChanged(mDb, accounts);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }

        if (mCalendarClient == null) {
            return;
        }

        // If there are no calendars at all for a given account, add the
        // default calendar.
        // TODO: move to sync adapter

        Set<Account> handledAccounts = Sets.newHashSet();
        if (Config.LOGV) Log.v(TAG, "querying calendars");
        Cursor c = query(Calendars.CONTENT_URI, ACCOUNTS_PROJECTION, null, null,
                null);
        try {
            while (c.moveToNext()) {
                final String accountName = c.getString(0);
                final String accountType = c.getString(1);
                final Account account = new Account(accountName, accountType);
                if (handledAccounts.contains(account)) {
                    continue;
                }
                handledAccounts.add(account);
                if (accountHasCalendar.containsKey(account)) {
                    if (Config.LOGV) {
                        Log.v(TAG, "calendars for account " + account + " exist");
                    }
                    accountHasCalendar.put(account, true /* hasCalendar */);
                }
            }
        } finally {
            c.close();
            c = null;
        }

        if (Config.LOGV) {
            Log.v(TAG, "scanning over " + accountHasCalendar.size() + " account(s)");
        }
        for (Map.Entry<Account, Boolean> entry : accountHasCalendar.entrySet()) {
            final Account account = entry.getKey();
            boolean hasCalendar = entry.getValue();
            if (hasCalendar) {
                if (Config.LOGV) {
                    Log.v(TAG, "ignoring account " + account +
                            " since it matched an existing calendar");
                }
                continue;
            }
            String feedUrl = mCalendarClient.getDefaultCalendarUrl(account.name,
                    CalendarClient.PROJECTION_PRIVATE_FULL, null/* query params */);
            feedUrl = CalendarSyncAdapter.rewriteUrlforAccount(account, feedUrl);
            if (Config.LOGV) {
                Log.v(TAG, "adding default calendar for account " + account);
            }
            ContentValues values = new ContentValues();
            values.put(Calendars._SYNC_ACCOUNT, account.name);
            values.put(Calendars._SYNC_ACCOUNT_TYPE, account.type);
            values.put(Calendars.URL, feedUrl);
            values.put(Calendars.OWNER_ACCOUNT,
                    CalendarSyncAdapter.calendarEmailAddressFromFeedUrl(feedUrl));
            values.put(Calendars.DISPLAY_NAME,
                    getContext().getString(R.string.calendar_default_name));
            values.put(Calendars.SYNC_EVENTS, 1);
            values.put(Calendars.SELECTED, 1);
            values.put(Calendars.HIDDEN, 0);
            values.put(Calendars.COLOR, -14069085 /* blue */);
            // this is just our best guess.  the real value will get updated
            // when the user does a sync.
            values.put(Calendars.TIMEZONE, Time.getCurrentTimezone());
            values.put(Calendars.ACCESS_LEVEL, Calendars.OWNER_ACCESS);
            insertInTransaction(Calendars.CONTENT_URI, values);

            mDbHelper.scheduleSync(account, false /* do a full sync */, null /* no url */);

        }
    }

    private static boolean readBooleanQueryParameter(Uri uri, String name, boolean defaultValue) {
        final String flag = uri.getQueryParameter(name);
        return flag == null
                ? defaultValue
                : (!"false".equals(flag.toLowerCase()) && !"0".equals(flag.toLowerCase()));
    }

}
