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

import com.android.providers.calendar.CalendarDatabaseHelper.Tables;
import com.android.providers.calendar.CalendarDatabaseHelper.Views;
import com.google.common.annotations.VisibleForTesting;

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
import android.content.Intent;
import android.content.IntentFilter;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.Debug;
import android.os.Handler;
import android.os.Message;
import android.os.Process;
import android.pim.EventRecurrence;
import android.pim.RecurrenceSet;
import android.provider.BaseColumns;
import android.provider.Calendar;
import android.provider.Calendar.Attendees;
import android.provider.Calendar.CalendarAlerts;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Events;
import android.provider.Calendar.Instances;
import android.provider.Calendar.Reminders;
import android.text.TextUtils;
import android.text.format.DateUtils;
import android.text.format.Time;
import android.util.Log;
import android.util.TimeFormatException;
import android.util.TimeUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calendar content provider. The contract between this provider and applications
 * is defined in {@link android.provider.Calendar}.
 */
public class CalendarProvider2 extends SQLiteContentProvider implements OnAccountsUpdateListener {

    private static final String TAG = "CalendarProvider2";

    private static final String TIMEZONE_GMT = "GMT";

    private static final boolean PROFILE = false;
    private static final boolean MULTIPLE_ATTENDEES_PER_EVENT = true;

    private static final String INVALID_CALENDARALERTS_SELECTOR =
            "_id IN (SELECT ca." + CalendarAlerts._ID + " FROM "
                    + Tables.CALENDAR_ALERTS + " AS ca"
                    + " LEFT OUTER JOIN " + Tables.INSTANCES
                    + " USING (" + Instances.EVENT_ID + ","
                    + Instances.BEGIN + "," + Instances.END + ")"
                    + " LEFT OUTER JOIN " + Tables.REMINDERS + " AS r ON"
                    + " (ca." + CalendarAlerts.EVENT_ID + "=r." + Reminders.EVENT_ID
                    + " AND ca." + CalendarAlerts.MINUTES + "=r." + Reminders.MINUTES + ")"
                    + " LEFT OUTER JOIN " + Views.EVENTS + " AS e ON"
                    + " (ca." + CalendarAlerts.EVENT_ID + "=e." + Events._ID + ")"
                    + " WHERE " + Tables.INSTANCES + "." + Instances.BEGIN + " ISNULL"
                    + "   OR ca." + CalendarAlerts.ALARM_TIME + "<?"
                    + "   OR (r." + Reminders.MINUTES + " ISNULL"
                    + "       AND ca." + CalendarAlerts.MINUTES + "<>0)"
                    + "   OR e." + Calendars.SELECTED + "=0)";

    private static final String[] ID_ONLY_PROJECTION =
            new String[] {Events._ID};

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
     * Projection to query for correcting times in allDay events.
     */
    private static final String[] ALLDAY_TIME_PROJECTION = new String[] {
        Events._ID,
        Events.DTSTART,
        Events.DTEND,
        Events.DURATION
    };
    private static final int ALLDAY_ID_INDEX = 0;
    private static final int ALLDAY_DTSTART_INDEX = 1;
    private static final int ALLDAY_DTEND_INDEX = 2;
    private static final int ALLDAY_DURATION_INDEX = 3;

    private static final int DAY_IN_SECONDS = 24 * 60 * 60;

    /**
     * The cached copy of the CalendarMetaData database table.
     * Make this "package private" instead of "private" so that test code
     * can access it.
     */
    MetaData mMetaData;
    CalendarCache mCalendarCache;

    private CalendarDatabaseHelper mDbHelper;

    private static final Uri SYNCSTATE_CONTENT_URI = Uri.parse("content://syncstate/state");
    //
    // SCHEDULE_ALARM_URI runs scheduleNextAlarm(false)
    // SCHEDULE_ALARM_REMOVE_URI runs scheduleNextAlarm(true)
    // TODO: use a service to schedule alarms rather than private URI
    /* package */ static final String SCHEDULE_ALARM_PATH = "schedule_alarms";
    /* package */ static final String SCHEDULE_ALARM_REMOVE_PATH = "schedule_alarms_remove";
    /* package */ static final Uri SCHEDULE_ALARM_URI =
            Uri.withAppendedPath(Calendar.CONTENT_URI, SCHEDULE_ALARM_PATH);
    /* package */ static final Uri SCHEDULE_ALARM_REMOVE_URI =
            Uri.withAppendedPath(Calendar.CONTENT_URI, SCHEDULE_ALARM_REMOVE_PATH);

    // To determine if a recurrence exception originally overlapped the
    // window, we need to assume a maximum duration, since we only know
    // the original start time.
    private static final int MAX_ASSUMED_DURATION = 7*24*60*60*1000;

    // The extended property name for storing an Event original Timezone.
    // Due to an issue in Calendar Server restricting the length of the name we had to strip it down
    // TODO - Better name would be:
    // "com.android.providers.calendar.CalendarSyncAdapter#originalTimezone"
    protected static final String EXT_PROP_ORIGINAL_TIMEZONE =
        "CalendarSyncAdapter#originalTimezone";

    private static final String SQL_SELECT_EVENTSRAWTIMES = "SELECT " +
            Calendar.EventsRawTimesColumns.EVENT_ID + ", " +
            Calendar.EventsRawTimesColumns.DTSTART_2445 + ", " +
            Calendar.EventsRawTimesColumns.DTEND_2445 + ", " +
            Events.EVENT_TIMEZONE +
            " FROM " +
            Tables.EVENTS_RAW_TIMES + ", " +
            Tables.EVENTS +
            " WHERE " +
            Calendar.EventsRawTimesColumns.EVENT_ID + " = " + Tables.EVENTS + "." + Events._ID;

    private static final String SQL_UPDATE_EVENT_SET_DIRTY = "UPDATE " +
            Tables.EVENTS +
            " SET " + Events._SYNC_DIRTY + "=1" +
            " WHERE " + Events._ID + "=?";

    private static final String SQL_WHERE_ID = BaseColumns._ID + "=?";
    private static final String SQL_WHERE_EVENT_ID = "event_id=?";
    private static final String SQL_WHERE_ORIGINAL_EVENT = Events.ORIGINAL_EVENT + "=?";
    private static final String SQL_WHERE_ATTENDEES_ID =
            Tables.ATTENDEES + "." + Attendees._ID + "=? AND " +
            Tables.EVENTS + "." + Events._ID + "=" + Tables.ATTENDEES + "." + Attendees.EVENT_ID;

    private static final String SQL_WHERE_REMINDERS_ID =
            Tables.REMINDERS + "." + Reminders._ID + "=? AND " +
            Tables.EVENTS + "." + Events._ID + "=" + Tables.REMINDERS + "." + Reminders.EVENT_ID;

    private static final String SQL_WHERE_CALENDAR_ALERT =
            Views.EVENTS + "." + BaseColumns._ID + "=" +
                    Tables.CALENDAR_ALERTS + "." + CalendarAlerts.EVENT_ID;

    private static final String SQL_WHERE_CALENDAR_ALERT_ID =
            Views.EVENTS + "." + BaseColumns._ID + "=" +
                    Tables.CALENDAR_ALERTS + "." + CalendarAlerts.EVENT_ID +
            " AND " +
            Tables.CALENDAR_ALERTS + "." + CalendarAlerts._ID + "=?";

    private static final String SQL_WHERE_EXTENDED_PROPERTIES_ID =
            Tables.EXTENDED_PROPERTIES + "." + Calendar.ExtendedProperties._ID + "=?";

    private static final String SQL_WHERE_GET_EVENTS_ENTRIES =
            "((" + Events.DTSTART + " <= ? AND "
                    + "(" + Events.LAST_DATE + " IS NULL OR " + Events.LAST_DATE + " >= ?)) OR " +
            "(" + Events.ORIGINAL_INSTANCE_TIME + " IS NOT NULL AND " +
                    Events.ORIGINAL_INSTANCE_TIME + " <= ? AND " +
                    Events.ORIGINAL_INSTANCE_TIME + " >= ?)) AND " +
            "(" + Calendars.SYNC_EVENTS + " != 0)";

    private static final String SQL_SELECT_EVENTS_SYNC_ID =
            "SELECT " + Events._SYNC_ID +
            " FROM " + Tables.EVENTS +
            " WHERE " + SQL_WHERE_ID;

    private static final String SQL_DELETE_FROM_CALENDARS = "DELETE FROM " + Tables.CALENDARS +
                " WHERE " + Calendar.SyncColumns._SYNC_ACCOUNT + "=? AND " +
                    Calendar.SyncColumns._SYNC_ACCOUNT_TYPE + "=?";

    private static final String SQL_WHERE_ID_FROM_INSTANCES_NOT_SYNCED =
            BaseColumns._ID + " IN " +
            "(SELECT " + Tables.INSTANCES + "." + BaseColumns._ID + " as _id" +
            " FROM " + Tables.INSTANCES +
            " INNER JOIN " + Tables.EVENTS +
            " ON (" +
            Tables.EVENTS + "." + Events._ID + "=" + Tables.INSTANCES + "." + Instances.EVENT_ID +
            ")" +
            " WHERE " + Tables.EVENTS + "." + Events._ID + "=?)";

    private static final String SQL_WHERE_ID_FROM_INSTANCES_SYNCED =
            BaseColumns._ID + " IN " +
            "(SELECT " + Tables.INSTANCES + "." + BaseColumns._ID + " as _id" +
            " FROM " + Tables.INSTANCES +
            " INNER JOIN " + Tables.EVENTS +
            " ON (" +
            Tables.EVENTS + "." + Events._ID + "=" + Tables.INSTANCES + "." + Instances.EVENT_ID +
            ")" +
            " WHERE " + Tables.EVENTS + "." + Events._SYNC_ID + "=?" + " OR " +
                    Tables.EVENTS + "." + Events.ORIGINAL_EVENT + "=?)";

    public static final class InstancesList extends ArrayList<ContentValues> {
    }

    public static final class EventInstancesMap extends HashMap<String, InstancesList> {
        public void add(String syncIdKey, ContentValues values) {
            InstancesList instances = get(syncIdKey);
            if (instances == null) {
                instances = new InstancesList();
                put(syncIdKey, instances);
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

        @Override
        public void run() {
            try {
                Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
                runScheduleNextAlarm(mRemoveAlarms);
            } catch (SQLException e) {
                if (Log.isLoggable(TAG, Log.ERROR)) {
                    Log.e(TAG, "runScheduleNextAlarm() failed", e);
                }
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
    private static final long SCHEDULE_ALARM_SLACK = 2 * DateUtils.HOUR_IN_MILLIS;

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
            7 * DateUtils.DAY_IN_MILLIS + SCHEDULE_ALARM_SLACK;

    // A lock for synchronizing access to fields that are shared
    // with the AlarmScheduler thread.
    private Object mAlarmLock = new Object();

    // Make sure we load at least two months worth of data.
    // Client apps can load more data in a background thread.
    private static final long MINIMUM_EXPANSION_SPAN =
            2L * 31 * 24 * 60 * 60 * 1000;

    private static final String[] sCalendarsIdProjection = new String[] { Calendars._ID };
    private static final int CALENDARS_INDEX_ID = 0;

    private static final String INSTANCE_QUERY_TABLES =
        CalendarDatabaseHelper.Tables.INSTANCES + " INNER JOIN " +
        CalendarDatabaseHelper.Views.EVENTS + " AS " +
        CalendarDatabaseHelper.Tables.EVENTS +
        " ON (" + CalendarDatabaseHelper.Tables.INSTANCES + "."
        + Calendar.Instances.EVENT_ID + "=" +
        CalendarDatabaseHelper.Tables.EVENTS + "."
        + Calendar.Events._ID + ")";

    private static final String INSTANCE_SEARCH_QUERY_TABLES = "(" +
        CalendarDatabaseHelper.Tables.INSTANCES + " INNER JOIN " +
        CalendarDatabaseHelper.Views.EVENTS + " AS " +
        CalendarDatabaseHelper.Tables.EVENTS +
        " ON (" + CalendarDatabaseHelper.Tables.INSTANCES + "."
        + Calendar.Instances.EVENT_ID + "=" +
        CalendarDatabaseHelper.Tables.EVENTS + "."
        + Calendar.Events._ID + ")" + ") LEFT OUTER JOIN " +
        CalendarDatabaseHelper.Tables.ATTENDEES +
        " ON (" + CalendarDatabaseHelper.Tables.ATTENDEES + "."
        + Calendar.Attendees.EVENT_ID + "=" +
        CalendarDatabaseHelper.Tables.EVENTS + "."
        + Calendar.Events._ID + ")";

    private static final String SQL_WHERE_INSTANCES_BETWEEN_DAY =
        Calendar.Instances.START_DAY + "<=? AND " +
        Calendar.Instances.END_DAY + ">=?";

    private static final String SQL_WHERE_INSTANCES_BETWEEN =
        Calendar.Instances.BEGIN + "<=? AND " +
        Calendar.Instances.END + ">=?";

    private static final int INSTANCES_INDEX_START_DAY = 0;
    private static final int INSTANCES_INDEX_END_DAY = 1;
    private static final int INSTANCES_INDEX_START_MINUTE = 2;
    private static final int INSTANCES_INDEX_END_MINUTE = 3;
    private static final int INSTANCES_INDEX_ALL_DAY = 4;

    /**
     * A regex for describing how we split search queries into tokens.
     * Keeps quoted phrases as one token.
     *
     *   "one \"two three\"" ==> ["one" "two three"]
     */
    private static final Pattern SEARCH_TOKEN_PATTERN =
        Pattern.compile("[^\\s\"'.?!,]+|" // first part matches unquoted words
                      + "\"([^\"]*)\"");  // second part matches quoted phrases
    /**
     * A special character that was use to escape potentially problematic
     * characters in search queries.
     *
     * Note: do not use backslash for this, as it interferes with the regex
     * escaping mechanism.
     */
    private static final String SEARCH_ESCAPE_CHAR = "#";

    /**
     * A regex for matching any characters in an incoming search query that we
     * need to escape with {@link #SEARCH_ESCAPE_CHAR}, including the escape
     * character itself.
     */
    private static final Pattern SEARCH_ESCAPE_PATTERN =
        Pattern.compile("([%_" + SEARCH_ESCAPE_CHAR + "])");

    /**
     * Alias used for aggregate concatenation of attendee e-mails when grouping
     * attendees by instance.
     */
    private static final String ATTENDEES_EMAIL_CONCAT =
        "group_concat(" + Calendar.Attendees.ATTENDEE_EMAIL + ")";

    /**
     * Alias used for aggregate concatenation of attendee names when grouping
     * attendees by instance.
     */
    private static final String ATTENDEES_NAME_CONCAT =
        "group_concat(" + Calendar.Attendees.ATTENDEE_NAME + ")";

    private static final String[] SEARCH_COLUMNS = new String[] {
        Calendar.Events.TITLE,
        Calendar.Events.DESCRIPTION,
        Calendar.Events.EVENT_LOCATION,
        ATTENDEES_EMAIL_CONCAT,
        ATTENDEES_NAME_CONCAT
    };

    private AlarmManager mAlarmManager;

    /**
     * Arbitrary integer that we assign to the messages that we send to this
     * thread's handler, indicating that these are requests to send an update
     * notification intent.
     */
    private static final int UPDATE_BROADCAST_MSG = 1;

    /**
     * Any requests to send a PROVIDER_CHANGED intent will be collapsed over
     * this window, to prevent spamming too many intents at once.
     */
    private static final long UPDATE_BROADCAST_TIMEOUT_MILLIS =
        DateUtils.SECOND_IN_MILLIS;

    private static final long SYNC_UPDATE_BROADCAST_TIMEOUT_MILLIS =
        30 * DateUtils.SECOND_IN_MILLIS;

    private Context mContext;

    private final Handler mBroadcastHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            Context context = CalendarProvider2.this.mContext;
            if (msg.what == UPDATE_BROADCAST_MSG) {
                // Broadcast a provider changed intent
                doSendUpdateNotification();
                // Because the handler does not guarantee message delivery in
                // the case that the provider is killed, we need to make sure
                // that the provider stays alive long enough to deliver the
                // notification. This empty service is sufficient to "wedge" the
                // process until we stop it here.
                context.stopService(new Intent(context, EmptyService.class));
            }
        }
    };

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
        try {
            return initialize();
        } catch (RuntimeException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Cannot start provider", e);
            }
            return false;
        }
    }

    private boolean initialize() {
        mContext = getContext();
        mDbHelper = (CalendarDatabaseHelper)getDatabaseHelper();
        mDb = mDbHelper.getWritableDatabase();

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
        mCalendarCache = new CalendarCache(mDbHelper);

        postInitialize();

        return true;
    }

    protected void postInitialize() {
        Thread thread = new PostInitializeThread();
        thread.start();
    }

    private class PostInitializeThread extends Thread {
        @Override
        public void run() {
            Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);

            verifyAccounts();

            doUpdateTimezoneDependentFields();
        }
    }

    /**
     * This creates a background thread to check the timezone and update
     * the timezone dependent fields in the Instances table if the timezone
     * has changed.
     */
    protected void updateTimezoneDependentFields() {
        Thread thread = new TimezoneCheckerThread();
        thread.start();
    }

    private class TimezoneCheckerThread extends Thread {
        @Override
        public void run() {
            Process.setThreadPriority(Process.THREAD_PRIORITY_BACKGROUND);
            doUpdateTimezoneDependentFields();
        }
    }

    /**
     * Check if we are in the same time zone
     */
    private boolean isLocalSameAsInstancesTimezone() {
        String localTimezone = TimeZone.getDefault().getID();
        return TextUtils.equals(mCalendarCache.readTimezoneInstances(), localTimezone);
    }

    /**
     * This method runs in a background thread.  If the timezone has changed
     * then the Instances table will be regenerated.
     */
    protected void doUpdateTimezoneDependentFields() {
        try {
            String timezoneType = mCalendarCache.readTimezoneType();
            // Nothing to do if we have the "home" timezone type (timezone is sticky)
            if (timezoneType != null && timezoneType.equals(CalendarCache.TIMEZONE_TYPE_HOME)) {
                return;
            }
            // We are here in "auto" mode, the timezone is coming from the device
            if (! isSameTimezoneDatabaseVersion()) {
                String localTimezone = TimeZone.getDefault().getID();
                doProcessEventRawTimes(localTimezone, TimeUtils.getTimeZoneDatabaseVersion());
            }
            if (isLocalSameAsInstancesTimezone()) {
                // Even if the timezone hasn't changed, check for missed alarms.
                // This code executes when the CalendarProvider2 is created and
                // helps to catch missed alarms when the Calendar process is
                // killed (because of low-memory conditions) and then restarted.
                rescheduleMissedAlarms();
            }
        } catch (SQLException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "doUpdateTimezoneDependentFields() failed", e);
            }
            try {
                // Clear at least the in-memory data (and if possible the
                // database fields) to force a re-computation of Instances.
                mMetaData.clearInstanceRange();
            } catch (SQLException e2) {
                if (Log.isLoggable(TAG, Log.ERROR)) {
                    Log.e(TAG, "clearInstanceRange() also failed: " + e2);
                }
            }
        }
    }

    protected void doProcessEventRawTimes(String localTimezone, String timeZoneDatabaseVersion) {
        mDb.beginTransaction();
        try {
            updateEventsStartEndFromEventRawTimesLocked();
            updateTimezoneDatabaseVersion(timeZoneDatabaseVersion);
            mCalendarCache.writeTimezoneInstances(localTimezone);
            regenerateInstancesTable();
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }
    }

    private void updateEventsStartEndFromEventRawTimesLocked() {
        Cursor cursor = mDb.rawQuery(SQL_SELECT_EVENTSRAWTIMES, null /* selection args */);
        try {
            while (cursor.moveToNext()) {
                long eventId = cursor.getLong(0);
                String dtStart2445 = cursor.getString(1);
                String dtEnd2445 = cursor.getString(2);
                String eventTimezone = cursor.getString(3);
                if (dtStart2445 == null && dtEnd2445 == null) {
                    if (Log.isLoggable(TAG, Log.ERROR)) {
                        Log.e(TAG, "Event " + eventId + " has dtStart2445 and dtEnd2445 null "
                                + "at the same time in EventsRawTimes!");
                    }
                    continue;
                }
                updateEventsStartEndLocked(eventId,
                        eventTimezone,
                        dtStart2445,
                        dtEnd2445);
            }
        } finally {
            cursor.close();
            cursor = null;
        }
    }

    private long get2445ToMillis(String timezone, String dt2445) {
        if (null == dt2445) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Cannot parse null RFC2445 date");
            }
            return 0;
        }
        Time time = (timezone != null) ? new Time(timezone) : new Time();
        try {
            time.parse(dt2445);
        } catch (TimeFormatException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Cannot parse RFC2445 date " + dt2445);
            }
            return 0;
        }
        return time.toMillis(true /* ignore DST */);
    }

    private void updateEventsStartEndLocked(long eventId,
            String timezone, String dtStart2445, String dtEnd2445) {

        ContentValues values = new ContentValues();
        values.put(Events.DTSTART, get2445ToMillis(timezone, dtStart2445));
        values.put(Events.DTEND, get2445ToMillis(timezone, dtEnd2445));

        int result = mDb.update(Tables.EVENTS, values, SQL_WHERE_ID,
                new String[] {String.valueOf(eventId)});
        if (0 == result) {
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Could not update Events table with values " + values);
            }
        }
    }

    private void updateTimezoneDatabaseVersion(String timeZoneDatabaseVersion) {
        try {
            mCalendarCache.writeTimezoneDatabaseVersion(timeZoneDatabaseVersion);
        } catch (CalendarCache.CacheException e) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Could not write timezone database version in the cache");
            }
        }
    }

    /**
     * Check if the time zone database version is the same as the cached one
     */
    protected boolean isSameTimezoneDatabaseVersion() {
        String timezoneDatabaseVersion = mCalendarCache.readTimezoneDatabaseVersion();
        if (timezoneDatabaseVersion == null) {
            return false;
        }
        return TextUtils.equals(timezoneDatabaseVersion, TimeUtils.getTimeZoneDatabaseVersion());
    }

    @VisibleForTesting
    protected String getTimezoneDatabaseVersion() {
        String timezoneDatabaseVersion = mCalendarCache.readTimezoneDatabaseVersion();
        if (timezoneDatabaseVersion == null) {
            return "";
        }
        if (Log.isLoggable(TAG, Log.INFO)) {
            Log.i(TAG, "timezoneDatabaseVersion = " + timezoneDatabaseVersion);
        }
        return timezoneDatabaseVersion;
    }

    private boolean isHomeTimezone() {
        String type = mCalendarCache.readTimezoneType();
        return type.equals(CalendarCache.TIMEZONE_TYPE_HOME);
    }

    private void regenerateInstancesTable() {
        // The database timezone is different from the current timezone.
        // Regenerate the Instances table for this month.  Include events
        // starting at the beginning of this month.
        long now = System.currentTimeMillis();
        String instancesTimezone = mCalendarCache.readTimezoneInstances();
        Time time = new Time(instancesTimezone);
        time.set(now);
        time.monthDay = 1;
        time.hour = 0;
        time.minute = 0;
        time.second = 0;

        long begin = time.normalize(true);
        long end = begin + MINIMUM_EXPANSION_SPAN;

        Cursor cursor = null;
        try {
            cursor = handleInstanceQuery(new SQLiteQueryBuilder(),
                    begin, end,
                    new String[] { Instances._ID },
                    null /* selection */, null /* sort */,
                    false /* searchByDayInsteadOfMillis */,
                    true /* force Instances deletion and expansion */,
                    instancesTimezone,
                    isHomeTimezone());
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

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
    protected void notifyChange(boolean syncToNetwork) {
        // Note that semantics are changed: notification is for CONTENT_URI, not the specific
        // Uri that was modified.
        getContext().getContentResolver().notifyChange(Calendar.CONTENT_URI, null,
                syncToNetwork);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "query uri - " + uri);
        }

        final SQLiteDatabase db = mDbHelper.getReadableDatabase();

        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        String groupBy = null;
        String limit = null; // Not currently implemented
        String instancesTimezone;

        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().query(db, projection, selection,  selectionArgs,
                        sortOrder);

            case EVENTS:
                qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sEventsProjectionMap);
                appendAccountFromParameter(qb, uri);
                break;
            case EVENTS_ID:
                qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sEventsProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getPathSegments().get(1));
                qb.appendWhere(SQL_WHERE_ID);
                break;

            case EVENT_ENTITIES:
                qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sEventEntitiesProjectionMap);
                appendAccountFromParameter(qb, uri);
                break;
            case EVENT_ENTITIES_ID:
                qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sEventEntitiesProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getPathSegments().get(1));
                qb.appendWhere(SQL_WHERE_ID);
                break;

            case CALENDARS:
            case CALENDAR_ENTITIES:
                qb.setTables(Tables.CALENDARS);
                appendAccountFromParameter(qb, uri);
                break;
            case CALENDARS_ID:
            case CALENDAR_ENTITIES_ID:
                qb.setTables(Tables.CALENDARS);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getPathSegments().get(1));
                qb.appendWhere(SQL_WHERE_ID);
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
                instancesTimezone = mCalendarCache.readTimezoneInstances();
                return handleInstanceQuery(qb, begin, end, projection,
                        selection, sortOrder, match == INSTANCES_BY_DAY,
                        false /* do not force Instances deletion and expansion */,
                        instancesTimezone, isHomeTimezone());
            case INSTANCES_SEARCH:
            case INSTANCES_SEARCH_BY_DAY:
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
                instancesTimezone = mCalendarCache.readTimezoneInstances();
                // this is already decoded
                String query = uri.getPathSegments().get(4);
                return handleInstanceSearchQuery(qb, begin, end, query, projection,
                        selection, sortOrder, match == INSTANCES_SEARCH_BY_DAY,
                        instancesTimezone, isHomeTimezone());
            case EVENT_DAYS:
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
                instancesTimezone = mCalendarCache.readTimezoneInstances();
                return handleEventDayQuery(qb, startDay, endDay, projection, selection,
                        instancesTimezone, isHomeTimezone());
            case ATTENDEES:
                qb.setTables(Tables.ATTENDEES + ", " + Tables.EVENTS);
                qb.setProjectionMap(sAttendeesProjectionMap);
                qb.appendWhere(Tables.EVENTS + "." + Events._ID + "="
                        + Tables.ATTENDEES + "." + Attendees.EVENT_ID);
                break;
            case ATTENDEES_ID:
                qb.setTables(Tables.ATTENDEES + ", " + Tables.EVENTS);
                qb.setProjectionMap(sAttendeesProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getPathSegments().get(1));
                qb.appendWhere(SQL_WHERE_ATTENDEES_ID);
                break;
            case REMINDERS:
                qb.setTables(Tables.REMINDERS);
                break;
            case REMINDERS_ID:
                qb.setTables(Tables.REMINDERS + ", " + Tables.EVENTS);
                qb.setProjectionMap(sRemindersProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(SQL_WHERE_REMINDERS_ID);
                break;
            case CALENDAR_ALERTS:
                qb.setTables(Tables.CALENDAR_ALERTS + ", " + CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                qb.appendWhere(SQL_WHERE_CALENDAR_ALERT);
                break;
            case CALENDAR_ALERTS_BY_INSTANCE:
                qb.setTables(Tables.CALENDAR_ALERTS + ", " + CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                qb.appendWhere(SQL_WHERE_CALENDAR_ALERT);
                groupBy = CalendarAlerts.EVENT_ID + "," + CalendarAlerts.BEGIN;
                break;
            case CALENDAR_ALERTS_ID:
                qb.setTables(Tables.CALENDAR_ALERTS + ", " + CalendarDatabaseHelper.Views.EVENTS);
                qb.setProjectionMap(sCalendarAlertsProjectionMap);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getLastPathSegment());
                qb.appendWhere(SQL_WHERE_CALENDAR_ALERT_ID);
                break;
            case EXTENDED_PROPERTIES:
                qb.setTables(Tables.EXTENDED_PROPERTIES);
                break;
            case EXTENDED_PROPERTIES_ID:
                qb.setTables(Tables.EXTENDED_PROPERTIES);
                selectionArgs = insertSelectionArg(selectionArgs, uri.getPathSegments().get(1));
                qb.appendWhere(SQL_WHERE_EXTENDED_PROPERTIES_ID);
                break;
            case PROVIDER_PROPERTIES:
                qb.setTables(Tables.CALENDAR_CACHE);
                qb.setProjectionMap(sCalendarCacheProjectionMap);
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

        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "query sql - projection: " + Arrays.toString(projection) +
                    " selection: " + selection +
                    " selectionArgs: " + Arrays.toString(selectionArgs) +
                    " sortOrder: " + sortOrder +
                    " groupBy: " + groupBy +
                    " limit: " + limit);
        }
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
     * @param forceExpansion force the Instance deletion and expansion if set to true
     * @param instancesTimezone timezone we need to use for computing the instances
     * @param isHomeTimezone if true, we are in the "home" timezone
     * @return
     */
    private Cursor handleInstanceQuery(SQLiteQueryBuilder qb, long rangeBegin,
            long rangeEnd, String[] projection, String selection, String sort,
            boolean searchByDay, boolean forceExpansion, String instancesTimezone,
            boolean isHomeTimezone) {

        qb.setTables(INSTANCE_QUERY_TABLES);
        qb.setProjectionMap(sInstancesProjectionMap);
        if (searchByDay) {
            // Convert the first and last Julian day range to a range that uses
            // UTC milliseconds.
            Time time = new Time(instancesTimezone);
            long beginMs = time.setJulianDay((int) rangeBegin);
            // We add one to lastDay because the time is set to 12am on the given
            // Julian day and we want to include all the events on the last day.
            long endMs = time.setJulianDay((int) rangeEnd + 1);
            // will lock the database.
            acquireInstanceRange(beginMs, endMs, true /* use minimum expansion window */,
                    forceExpansion, instancesTimezone, isHomeTimezone);
            qb.appendWhere(SQL_WHERE_INSTANCES_BETWEEN_DAY);
        } else {
            // will lock the database.
            acquireInstanceRange(rangeBegin, rangeEnd, true /* use minimum expansion window */,
                    forceExpansion, instancesTimezone, isHomeTimezone);
            qb.appendWhere(SQL_WHERE_INSTANCES_BETWEEN);
        }
        String selectionArgs[] = new String[] {String.valueOf(rangeEnd),
                String.valueOf(rangeBegin)};
        return qb.query(mDb, projection, selection, selectionArgs, null /* groupBy */,
                null /* having */, sort);
    }

    /**
     * Escape any special characters in the search token
     * @param token the token to escape
     * @return the escaped token
     */
    @VisibleForTesting
    String escapeSearchToken(String token) {
        Matcher matcher = SEARCH_ESCAPE_PATTERN.matcher(token);
        return matcher.replaceAll(SEARCH_ESCAPE_CHAR + "$1");
    }

    /**
     * Splits the search query into individual search tokens based on whitespace
     * and punctuation. Leaves both single quoted and double quoted strings
     * intact.
     *
     * @param query the search query
     * @return an array of tokens from the search query
     */
    @VisibleForTesting
    String[] tokenizeSearchQuery(String query) {
        List<String> matchList = new ArrayList<String>();
        Matcher matcher = SEARCH_TOKEN_PATTERN.matcher(query);
        String token;
        while (matcher.find()) {
            if (matcher.group(1) != null) {
                // double quoted string
                token = matcher.group(1);
            } else {
                // unquoted token
                token = matcher.group();
            }
            matchList.add(escapeSearchToken(token));
        }
        return matchList.toArray(new String[matchList.size()]);
    }

    /**
     * In order to support what most people would consider a reasonable
     * search behavior, we have to do some interesting things here. We
     * assume that when a user searches for something like "lunch meeting",
     * they really want any event that matches both "lunch" and "meeting",
     * not events that match the string "lunch meeting" itself. In order to
     * do this across multiple columns, we have to construct a WHERE clause
     * that looks like:
     * <code>
     *   WHERE (title LIKE "%lunch%"
     *      OR description LIKE "%lunch%"
     *      OR eventLocation LIKE "%lunch%")
     *     AND (title LIKE "%meeting%"
     *      OR description LIKE "%meeting%"
     *      OR eventLocation LIKE "%meeting%")
     * </code>
     * This "product of clauses" is a bit ugly, but produced a fairly good
     * approximation of full-text search across multiple columns.
     */
    @VisibleForTesting
    String constructSearchWhere(String[] tokens) {
        if (tokens.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        String column, token;
        for (int j = 0; j < tokens.length; j++) {
            sb.append("(");
            for (int i = 0; i < SEARCH_COLUMNS.length; i++) {
                sb.append(SEARCH_COLUMNS[i]);
                sb.append(" LIKE ? ESCAPE \"");
                sb.append(SEARCH_ESCAPE_CHAR);
                sb.append("\" ");
                if (i < SEARCH_COLUMNS.length - 1) {
                    sb.append("OR ");
                }
            }
            sb.append(")");
            if (j < tokens.length - 1) {
                sb.append(" AND ");
            }
        }
        return sb.toString();
    }

    @VisibleForTesting
    String[] constructSearchArgs(String[] tokens, long rangeBegin, long rangeEnd) {
        int numCols = SEARCH_COLUMNS.length;
        int numArgs = tokens.length * numCols + 2;
        // the additional two elements here are for begin/end time
        String[] selectionArgs = new String[numArgs];
        selectionArgs[0] =  String.valueOf(rangeEnd);
        selectionArgs[1] =  String.valueOf(rangeBegin);
        for (int j = 0; j < tokens.length; j++) {
            int start = 2 + numCols * j;
            for (int i = start; i < start + numCols; i++) {
                selectionArgs[i] = "%" + tokens[j] + "%";
            }
        }
        return selectionArgs;
    }

    private Cursor handleInstanceSearchQuery(SQLiteQueryBuilder qb,
            long rangeBegin, long rangeEnd, String query, String[] projection,
            String selection, String sort, boolean searchByDay, String instancesTimezone,
            boolean isHomeTimezone) {
        qb.setTables(INSTANCE_SEARCH_QUERY_TABLES);
        qb.setProjectionMap(sInstancesProjectionMap);

        String[] tokens = tokenizeSearchQuery(query);
        String[] selectionArgs = constructSearchArgs(tokens, rangeBegin, rangeEnd);
        // we pass this in as a HAVING instead of a WHERE so the filtering
        // happens after the grouping
        String searchWhere = constructSearchWhere(tokens);

        if (searchByDay) {
            // Convert the first and last Julian day range to a range that uses
            // UTC milliseconds.
            Time time = new Time(instancesTimezone);
            long beginMs = time.setJulianDay((int) rangeBegin);
            // We add one to lastDay because the time is set to 12am on the given
            // Julian day and we want to include all the events on the last day.
            long endMs = time.setJulianDay((int) rangeEnd + 1);
            // will lock the database.
            // we expand the instances here because we might be searching over
            // a range where instance expansion has not occurred yet
            acquireInstanceRange(beginMs, endMs,
                    true /* use minimum expansion window */,
                    false /* do not force Instances deletion and expansion */,
                    instancesTimezone,
                    isHomeTimezone
            );
            qb.appendWhere(SQL_WHERE_INSTANCES_BETWEEN_DAY);
        } else {
            // will lock the database.
            // we expand the instances here because we might be searching over
            // a range where instance expansion has not occurred yet
            acquireInstanceRange(rangeBegin, rangeEnd,
                    true /* use minimum expansion window */,
                    false /* do not force Instances deletion and expansion */,
                    instancesTimezone,
                    isHomeTimezone
            );
            qb.appendWhere(SQL_WHERE_INSTANCES_BETWEEN);
        }

        return qb.query(mDb, projection, selection, selectionArgs,
                Instances._ID /* groupBy */, searchWhere /* having */, sort);
    }

    private Cursor handleEventDayQuery(SQLiteQueryBuilder qb, int begin, int end,
            String[] projection, String selection, String instancesTimezone,
            boolean isHomeTimezone) {
        qb.setTables(INSTANCE_QUERY_TABLES);
        qb.setProjectionMap(sInstancesProjectionMap);
        // Convert the first and last Julian day range to a range that uses
        // UTC milliseconds.
        Time time = new Time(instancesTimezone);
        long beginMs = time.setJulianDay(begin);
        // We add one to lastDay because the time is set to 12am on the given
        // Julian day and we want to include all the events on the last day.
        long endMs = time.setJulianDay(end + 1);

        acquireInstanceRange(beginMs, endMs, true,
                false /* do not force Instances expansion */, instancesTimezone, isHomeTimezone);
        qb.appendWhere(SQL_WHERE_INSTANCES_BETWEEN_DAY);
        String selectionArgs[] = new String[] {String.valueOf(end), String.valueOf(begin)};

        return qb.query(mDb, projection, selection, selectionArgs,
                Instances.START_DAY /* groupBy */, null /* having */, null);
    }

    /**
     * Ensure that the date range given has all elements in the instance
     * table.  Acquires the database lock and calls {@link #acquireInstanceRangeLocked}.
     *
     * @param begin start of range (ms)
     * @param end end of range (ms)
     * @param useMinimumExpansionWindow expand by at least MINIMUM_EXPANSION_SPAN
     * @param forceExpansion force the Instance deletion and expansion if set to true
     * @param instancesTimezone timezone we need to use for computing the instances
     * @param isHomeTimezone if true, we are in the "home" timezone
     */
    private void acquireInstanceRange(final long begin, final long end,
            final boolean useMinimumExpansionWindow, final boolean forceExpansion,
            final String instancesTimezone, final boolean isHomeTimezone) {
        mDb.beginTransaction();
        try {
            acquireInstanceRangeLocked(begin, end, useMinimumExpansionWindow,
                    forceExpansion, instancesTimezone, isHomeTimezone);
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
     * @param forceExpansion force the Instance deletion and expansion if set to true
     * @param instancesTimezone timezone we need to use for computing the instances
     * @param isHomeTimezone if true, we are in the "home" timezone
     */
    private void acquireInstanceRangeLocked(long begin, long end, boolean useMinimumExpansionWindow,
            boolean forceExpansion, String instancesTimezone, boolean isHomeTimezone) {
        long expandBegin = begin;
        long expandEnd = end;

        if (instancesTimezone == null) {
            Log.e(TAG, "Cannot run acquireInstanceRangeLocked() because instancesTimezone is null");
            return;
        }

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
        long maxInstance = fields.maxInstance;
        long minInstance = fields.minInstance;
        boolean timezoneChanged;
        if (isHomeTimezone) {
            String previousTimezone = mCalendarCache.readTimezoneInstancesPrevious();
            timezoneChanged = !instancesTimezone.equals(previousTimezone);
        } else {
            String localTimezone = TimeZone.getDefault().getID();
            timezoneChanged = !instancesTimezone.equals(localTimezone);
            // if we're in auto make sure we are using the device time zone
            if (timezoneChanged) {
                instancesTimezone = localTimezone;
            }
        }
        // if "home", then timezoneChanged only if current != previous
        // if "auto", then timezoneChanged, if !instancesTimezone.equals(localTimezone);
        if (maxInstance == 0 || timezoneChanged || forceExpansion) {
            // Empty the Instances table and expand from scratch.
            mDb.execSQL("DELETE FROM " + Tables.INSTANCES + ";");
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "acquireInstanceRangeLocked() deleted Instances,"
                        + " timezone changed: " + timezoneChanged);
            }
            expandInstanceRangeLocked(expandBegin, expandEnd, instancesTimezone);

            mMetaData.writeLocked(instancesTimezone, expandBegin, expandEnd);

            String timezoneType = mCalendarCache.readTimezoneType();
            // This may cause some double writes but guarantees the time zone in
            // the db and the time zone the instances are in is the same, which
            // future changes may affect.
            mCalendarCache.writeTimezoneInstances(instancesTimezone);

            // If we're in auto check if we need to fix the previous tz value
            if (timezoneType.equals(CalendarCache.TIMEZONE_TYPE_AUTO)) {
                String prevTZ = mCalendarCache.readTimezoneInstancesPrevious();
                if (TextUtils.equals(TIMEZONE_GMT, prevTZ)) {
                    mCalendarCache.writeTimezoneInstancesPrevious(instancesTimezone);
                }
            }
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
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Canceled instance query (" + expandBegin + ", " + expandEnd
                        + ") falls within previously expanded range.");
            }
            return;
        }

        // If the requested begin point has not been expanded, then include
        // more events than requested in the expansion (use "expandBegin").
        if (begin < minInstance) {
            expandInstanceRangeLocked(expandBegin, minInstance, instancesTimezone);
            minInstance = expandBegin;
        }

        // If the requested end point has not been expanded, then include
        // more events than requested in the expansion (use "expandEnd").
        if (end > maxInstance) {
            expandInstanceRangeLocked(maxInstance, expandEnd, instancesTimezone);
            maxInstance = expandEnd;
        }

        // Update the bounds on the Instances table.
        mMetaData.writeLocked(instancesTimezone, minInstance, maxInstance);
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
            Events.ORIGINAL_INSTANCE_TIME,
            Events.CALENDAR_ID,
            Events.DELETED
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
        qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
        qb.setProjectionMap(sEventsProjectionMap);

        String beginString = String.valueOf(begin);
        String endString = String.valueOf(end);

        // grab recurrence exceptions that fall outside our expansion window but modify
        // recurrences that do fall within our window.  we won't insert these into the output
        // set of instances, but instead will just add them to our cancellations list, so we
        // can cancel the correct recurrence expansion instances.
        // we don't have originalInstanceDuration or end time.  for now, assume the original
        // instance lasts no longer than 1 week.
        // also filter with syncable state (we dont want the entries from a non syncable account)
        // TODO: compute the originalInstanceEndTime or get this from the server.
        qb.appendWhere(SQL_WHERE_GET_EVENTS_ENTRIES);
        String selectionArgs[] = new String[] {endString, beginString, endString,
                String.valueOf(begin - MAX_ASSUMED_DURATION)};
        Cursor c = qb.query(mDb, EXPAND_COLUMNS, null /* selection */,
                selectionArgs, null /* groupBy */,
                null /* having */, null /* sortOrder */);
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Instance expansion:  got " + c.getCount() + " entries");
        }
        return c;
    }

    /**
     * Generates a unique key from the syncId and calendarId.
     * The purpose of this is to prevent collisions if two different calendars use the
     * same sync id.  This can happen if a Google calendar is accessed by two different accounts,
     * or with Exchange, where ids are not unique between calendars.
     * @param syncId Id for the event
     * @param calendarId Id for the calendar
     * @return key
     */
    private String getSyncIdKey(String syncId, long calendarId) {
        return calendarId + ":" + syncId;
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

        // Key into the instance values to hold the original event concatenated with calendar id.
        final String ORIGINAL_EVENT_AND_CALENDAR = "ORIGINAL_EVENT_AND_CALENDAR";

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
        int calendarIdColumn = entries.getColumnIndex(Events.CALENDAR_ID);
        int deletedColumn = entries.getColumnIndex(Events.DELETED);

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
                        if (Log.isLoggable(TAG, Log.ERROR)) {
                            Log.w(TAG, "error parsing duration for event "
                                    + eventId + "'" + durationStr + "'", e);
                        }
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
                boolean deleted = (entries.getInt(deletedColumn) != 0);

                String rruleStr = entries.getString(rruleColumn);
                String rdateStr = entries.getString(rdateColumn);
                String exruleStr = entries.getString(exruleColumn);
                String exdateStr = entries.getString(exdateColumn);
                long calendarId = entries.getLong(calendarIdColumn);
                String syncIdKey = getSyncIdKey(syncId, calendarId); // key into instancesMap

                RecurrenceSet recur = null;
                try {
                    recur = new RecurrenceSet(rruleStr, rdateStr, exruleStr, exdateStr);
                } catch (EventRecurrence.InvalidFormatException e) {
                    if (Log.isLoggable(TAG, Log.ERROR)) {
                        Log.w(TAG, "Could not parse RRULE recurrence string: " + rruleStr, e);
                    }
                    continue;
                }

                if (null != recur && recur.hasRecurrence()) {
                    // the event is repeating

                    if (status == Events.STATUS_CANCELED) {
                        // should not happen!
                        if (Log.isLoggable(TAG, Log.ERROR)) {
                            Log.e(TAG, "Found canceled recurring event in "
                                    + "Events table.  Ignoring.");
                        }
                        continue;
                    }
                    if (deleted) {
                        // Don't expand deleted recurring events
                        continue;
                    }

                    // need to parse the event into a local calendar.
                    eventTime.timezone = eventTimezone;
                    eventTime.set(dtstartMillis);
                    eventTime.allDay = allDay;

                    if (durationStr == null) {
                        // should not happen.
                        if (Log.isLoggable(TAG, Log.ERROR)) {
                            Log.e(TAG, "Repeating event has no duration -- "
                                    + "should not happen.");
                        }
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
                        instancesMap.add(syncIdKey, initialValues);
                    }
                } else {
                    // the event is not repeating
                    initialValues = new ContentValues();

                    // if this event has an "original" field, then record
                    // that we need to cancel the original event (we can't
                    // do that here because the order of this loop isn't
                    // defined)
                    if (originalEvent != null && originalInstanceTimeMillis != -1) {
                        // The ORIGINAL_EVENT_AND_CALENDAR holds the
                        // calendar id concatenated with the ORIGINAL_EVENT to form
                        // a unique key, matching the keys for instancesMap.
                        initialValues.put(ORIGINAL_EVENT_AND_CALENDAR,
                                getSyncIdKey(originalEvent, calendarId));
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
                            if (Log.isLoggable(TAG, Log.ERROR)) {
                                Log.w(TAG, "Unexpected event outside window: " + syncId);
                            }
                            continue;
                        }
                    }

                    initialValues.put(Instances.EVENT_ID, eventId);

                    initialValues.put(Instances.BEGIN, dtstartMillis);
                    initialValues.put(Instances.END, dtendMillis);

                    // we temporarily store the DELETED status (will be cleaned later)
                    initialValues.put(Events.DELETED, deleted);

                    if (allDay) {
                        eventTime.timezone = Time.TIMEZONE_UTC;
                    } else {
                        eventTime.timezone = localTimezone;
                    }
                    computeTimezoneDependentFields(dtstartMillis, dtendMillis,
                            eventTime, initialValues);

                    instancesMap.add(syncIdKey, initialValues);
                }
            } catch (DateException e) {
                if (Log.isLoggable(TAG, Log.ERROR)) {
                    Log.w(TAG, "RecurrenceProcessor error ", e);
                }
            } catch (TimeFormatException e) {
                if (Log.isLoggable(TAG, Log.ERROR)) {
                    Log.w(TAG, "RecurrenceProcessor error ", e);
                }
            }
        }

        // Invariant: instancesMap contains all instances that affect the
        // window, indexed by original sync id concatenated with calendar id.
        // It consists of:
        // a) Individual events that fall in the window.  They have:
        //   EVENT_ID, BEGIN, END
        // b) Instances of recurrences that fall in the window.  They may
        //   be subject to exceptions.  They have:
        //   EVENT_ID, BEGIN, END
        // c) Exceptions that fall in the window.  They have:
        //   ORIGINAL_EVENT_AND_CALENDAR, ORIGINAL_INSTANCE_TIME, STATUS (since they can
        //   be a modification or cancellation), EVENT_ID, BEGIN, END
        // d) Recurrence exceptions that modify an instance inside the
        //   window but fall outside the window.  They have:
        //   ORIGINAL_EVENT_AND_CALENDAR, ORIGINAL_INSTANCE_TIME, STATUS =
        //   STATUS_CANCELED, EVENT_ID, BEGIN, END

        // First, delete the original instances corresponding to recurrence
        // exceptions.  We do this by iterating over the list and for each
        // recurrence exception, we search the list for an instance with a
        // matching "original instance time".  If we find such an instance,
        // we remove it from the list.  If we don't find such an instance
        // then we cancel the recurrence exception.
        Set<String> keys = instancesMap.keySet();
        for (String syncIdKey : keys) {
            InstancesList list = instancesMap.get(syncIdKey);
            for (ContentValues values : list) {

                // If this instance is not a recurrence exception, then
                // skip it.
                if (!values.containsKey(ORIGINAL_EVENT_AND_CALENDAR)) {
                    continue;
                }

                String originalEventPlusCalendar = values.getAsString(ORIGINAL_EVENT_AND_CALENDAR);
                long originalTime = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME);
                InstancesList originalList = instancesMap.get(originalEventPlusCalendar);
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
        for (String syncIdKey : keys) {
            InstancesList list = instancesMap.get(syncIdKey);
            for (ContentValues values : list) {

                // If this instance was cancelled or deleted then don't create a new
                // instance.
                Integer status = values.getAsInteger(Events.STATUS);
                boolean deleted = values.containsKey(Events.DELETED) ?
                        values.getAsBoolean(Events.DELETED) : false;
                if ((status != null && status == Events.STATUS_CANCELED) || deleted) {
                    continue;
                }

                // We remove this useless key (not valid in the context of Instances table)
                values.remove(Events.DELETED);

                // Remove these fields before inserting a new instance
                values.remove(ORIGINAL_EVENT_AND_CALENDAR);
                values.remove(Events.ORIGINAL_INSTANCE_TIME);
                values.remove(Events.STATUS);

                mDbHelper.instancesReplace(values);
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
            case EVENT_DAYS:
                return "vnd.android.cursor.dir/event-instance";
            case TIME:
                return "time/epoch";
            case PROVIDER_PROPERTIES:
                return "vnd.android.cursor.dir/property";
            default:
                throw new IllegalArgumentException("Unknown URL " + url);
        }
    }

    public static boolean isRecurrenceEvent(ContentValues values) {
        return (!TextUtils.isEmpty(values.getAsString(Events.RRULE))||
                !TextUtils.isEmpty(values.getAsString(Events.RDATE))||
                !TextUtils.isEmpty(values.getAsString(Events.ORIGINAL_EVENT)));
    }

    /**
     * Takes an event and corrects the hrs, mins, secs if it is an allDay event.
     *
     * AllDay events should have hrs, mins, secs set to zero. This checks if this is true and
     * corrects the fields DTSTART, DTEND, and DURATION if necessary. Also checks to ensure that
     * either both DTSTART and DTEND or DTSTART and DURATION are set for each event.
     *
     * @param updatedValues The values to check and correct
     * @return Returns true if a correction was necessary, false otherwise
     */
    private boolean fixAllDayTime(Uri uri, ContentValues updatedValues) {
        boolean neededCorrection = false;
        if (updatedValues.containsKey(Events.ALL_DAY)
                && updatedValues.getAsInteger(Events.ALL_DAY).intValue() == 1) {
            Long dtstart = updatedValues.getAsLong(Events.DTSTART);
            Long dtend = updatedValues.getAsLong(Events.DTEND);
            String duration = updatedValues.getAsString(Events.DURATION);
            Time time = new Time();
            Cursor currentTimesCursor = null;
            String tempValue;
            // If a complete set of time fields doesn't exist query the db for them. A complete set
            // is dtstart and dtend for non-recurring events or dtstart and duration for recurring
            // events.
            if(dtstart == null || (dtend == null && duration == null)) {
                // Make sure we have an id to search for, if not this is probably a new event
                if (uri.getPathSegments().size() == 2) {
                    currentTimesCursor = query(uri,
                            ALLDAY_TIME_PROJECTION,
                            null /* selection */,
                            null /* selectionArgs */,
                            null /* sort */);
                    if (currentTimesCursor != null) {
                        if (!currentTimesCursor.moveToFirst() ||
                                currentTimesCursor.getCount() != 1) {
                            // Either this is a new event or the query is too general to get data
                            // from the db. In either case don't try to use the query and catch
                            // errors when trying to update the time fields.
                            currentTimesCursor.close();
                            currentTimesCursor = null;
                        }
                    }
                }
            }

            // Ensure dtstart exists for this event (always required) and set so h,m,s are 0 if
            // necessary.
            // TODO Move this somewhere to check all events, not just allDay events.
            if (dtstart == null) {
                if (currentTimesCursor != null) {
                    // getLong returns 0 for empty fields, we'd like to know if a field is empty
                    // so getString is used instead.
                    tempValue = currentTimesCursor.getString(ALLDAY_DTSTART_INDEX);
                    try {
                        dtstart = Long.valueOf(tempValue);
                    } catch (NumberFormatException e) {
                        currentTimesCursor.close();
                        throw new IllegalArgumentException("Event has no DTSTART field, the db " +
                            "may be damaged. Set DTSTART for this event to fix.");
                    }
                } else {
                    throw new IllegalArgumentException("DTSTART cannot be empty for new events.");
                }
            }
            time.clear(Time.TIMEZONE_UTC);
            time.set(dtstart.longValue());
            if (time.hour != 0 || time.minute != 0 || time.second != 0) {
                time.hour = 0;
                time.minute = 0;
                time.second = 0;
                updatedValues.put(Events.DTSTART, time.toMillis(true));
                neededCorrection = true;
            }

            // If dtend exists for this event make sure it's h,m,s are 0.
            if (dtend == null && currentTimesCursor != null) {
                // getLong returns 0 for empty fields. We'd like to know if a field is empty
                // so getString is used instead.
                tempValue = currentTimesCursor.getString(ALLDAY_DTEND_INDEX);
                try {
                    dtend = Long.valueOf(tempValue);
                } catch (NumberFormatException e) {
                    dtend = null;
                }
            }
            if (dtend != null) {
                time.clear(Time.TIMEZONE_UTC);
                time.set(dtend.longValue());
                if (time.hour != 0 || time.minute != 0 || time.second != 0) {
                    time.hour = 0;
                    time.minute = 0;
                    time.second = 0;
                    dtend = time.toMillis(true);
                    updatedValues.put(Events.DTEND, dtend);
                    neededCorrection = true;
                }
            }

            if (currentTimesCursor != null) {
                if (duration == null) {
                    duration = currentTimesCursor.getString(ALLDAY_DURATION_INDEX);
                }
                currentTimesCursor.close();
            }

            if (duration != null) {
                int len = duration.length();
                /* duration is stored as either "P<seconds>S" or "P<days>D". This checks if it's
                 * in the seconds format, and if so converts it to days.
                 */
                if (len == 0) {
                    duration = null;
                } else if (duration.charAt(0) == 'P' &&
                        duration.charAt(len - 1) == 'S') {
                    int seconds = Integer.parseInt(duration.substring(1, len - 1));
                    int days = (seconds + DAY_IN_SECONDS - 1) / DAY_IN_SECONDS;
                    duration = "P" + days + "D";
                    updatedValues.put(Events.DURATION, duration);
                    neededCorrection = true;
                }
            }

            if (duration == null && dtend == null) {
                throw new IllegalArgumentException("DTEND and DURATION cannot both be null for " +
                        "an event.");
            }
        }
        return neededCorrection;
    }

    @Override
    protected Uri insertInTransaction(Uri uri, ContentValues values, boolean callerIsSyncAdapter) {
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "insertInTransaction: " + uri);
        }

        long id = 0;

        final int match = sUriMatcher.match(uri);
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
                // TODO: do we really need to make a copy?
                ContentValues updatedValues = new ContentValues(values);
                validateEventData(updatedValues);
                // updateLastDate must be after validation, to ensure proper last date computation
                updatedValues = updateLastDate(updatedValues);
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
                if (fixAllDayTime(uri, updatedValues)) {
                    if (Log.isLoggable(TAG, Log.WARN)) {
                        Log.w(TAG, "insertInTransaction: " +
                                "allDay is true but sec, min, hour were not 0.");
                    }
                }
                id = mDbHelper.eventsInsert(updatedValues);
                if (id != -1) {
                    updateEventRawTimesLocked(id, updatedValues);
                    updateInstancesLocked(updatedValues, id, true /* new event */, mDb);

                    // If we inserted a new event that specified the self-attendee
                    // status, then we need to add an entry to the attendees table.
                    if (values.containsKey(Events.SELF_ATTENDEE_STATUS)) {
                        int status = values.getAsInteger(Events.SELF_ATTENDEE_STATUS);
                        if (owner == null) {
                            owner = getOwner(updatedValues.getAsLong(Events.CALENDAR_ID));
                        }
                        createAttendeeEntry(id, status, owner);
                    }
                    // if the Event Timezone is defined, store it as the original one in the
                    // ExtendedProperties table
                    if (values.containsKey(Events.EVENT_TIMEZONE) && !callerIsSyncAdapter) {
                        String originalTimezone = values.getAsString(Events.EVENT_TIMEZONE);

                        ContentValues expropsValues = new ContentValues();
                        expropsValues.put(Calendar.ExtendedProperties.EVENT_ID, id);
                        expropsValues.put(Calendar.ExtendedProperties.NAME,
                                EXT_PROP_ORIGINAL_TIMEZONE);
                        expropsValues.put(Calendar.ExtendedProperties.VALUE, originalTimezone);

                        // Insert the extended property
                        long exPropId = mDbHelper.extendedPropertiesInsert(expropsValues);
                        if (exPropId == -1) {
                            if (Log.isLoggable(TAG, Log.ERROR)) {
                                Log.e(TAG, "Cannot add the original Timezone in the "
                                        + "ExtendedProperties table for Event: " + id);
                            }
                        } else {
                            // Update the Event for saying it has some extended properties
                            ContentValues eventValues = new ContentValues();
                            eventValues.put(Events.HAS_EXTENDED_PROPERTIES, "1");
                            int result = mDb.update("Events", eventValues, SQL_WHERE_ID,
                                    new String[] {String.valueOf(id)});
                            if (result <= 0) {
                                if (Log.isLoggable(TAG, Log.ERROR)) {
                                    Log.e(TAG, "Cannot update hasExtendedProperties column"
                                            + " for Event: " + id);
                                }
                            }
                        }
                    }
                    sendUpdateNotification(id, callerIsSyncAdapter);
                }
                break;
            case CALENDARS:
                Integer syncEvents = values.getAsInteger(Calendars.SYNC_EVENTS);
                if (syncEvents != null && syncEvents == 1) {
                    String accountName = values.getAsString(Calendars._SYNC_ACCOUNT);
                    String accountType = values.getAsString(
                            Calendars._SYNC_ACCOUNT_TYPE);
                    final Account account = new Account(accountName, accountType);
                    String eventsUrl = values.getAsString(Calendars.SYNC1);
                    mDbHelper.scheduleSync(account, false /* two-way sync */, eventsUrl);
                }
                id = mDbHelper.calendarsInsert(values);
                sendUpdateNotification(id, callerIsSyncAdapter);
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
            case EVENT_DAYS:
            case PROVIDER_PROPERTIES:
                throw new UnsupportedOperationException("Cannot insert into that URL: " + uri);
            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }

        if (id < 0) {
            return null;
        }

        return ContentUris.withAppendedId(uri, id);
    }

    /**
     * Do some validation on event data before inserting.
     * In particular make sure dtend, duration, etc make sense for
     * the type of event (regular, recurrence, exception).  Remove
     * any unexpected fields.
     *
     * @param values the ContentValues to insert
     */
    private void validateEventData(ContentValues values) {
        boolean hasDtend = values.getAsLong(Events.DTEND) != null;
        boolean hasDuration = !TextUtils.isEmpty(values.getAsString(Events.DURATION));
        boolean hasRrule = !TextUtils.isEmpty(values.getAsString(Events.RRULE));
        boolean hasRdate = !TextUtils.isEmpty(values.getAsString(Events.RDATE));
        boolean hasOriginalEvent = !TextUtils.isEmpty(values.getAsString(Events.ORIGINAL_EVENT));
        boolean hasOriginalInstanceTime = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME) != null;
        if (hasRrule || hasRdate) {
            // Recurrence:
            // dtstart is start time of first event
            // dtend is null
            // duration is the duration of the event
            // rrule is the recurrence rule
            // lastDate is the end of the last event or null if it repeats forever
            // originalEvent is null
            // originalInstanceTime is null
            if (hasDtend || !hasDuration || hasOriginalEvent || hasOriginalInstanceTime) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.e(TAG, "Invalid values for recurrence: " + values);
                }
                values.remove(Events.DTEND);
                values.remove(Events.ORIGINAL_EVENT);
                values.remove(Events.ORIGINAL_INSTANCE_TIME);
            }
        } else if (hasOriginalEvent || hasOriginalInstanceTime) {
            // Recurrence exception
            // dtstart is start time of exception event
            // dtend is end time of exception event
            // duration is null
            // rrule is null
            // lastdate is same as dtend
            // originalEvent is the _sync_id of the recurrence
            // originalInstanceTime is the start time of the event being replaced
            if (!hasDtend || hasDuration || !hasOriginalEvent || !hasOriginalInstanceTime) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.e(TAG, "Invalid values for recurrence exception: " + values);
                }
                values.remove(Events.DURATION);
            }
        } else {
            // Regular event
            // dtstart is the start time
            // dtend is the end time
            // duration is null
            // rrule is null
            // lastDate is the same as dtend
            // originalEvent is null
            // originalInstanceTime is null
            if (!hasDtend || hasDuration) {
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.e(TAG, "Invalid values for event: " + values);
                }
                values.remove(Events.DURATION);
            }
        }
    }

    private void setEventDirty(int eventId) {
        mDb.execSQL(SQL_UPDATE_EVENT_SET_DIRTY, new Integer[] {eventId});
    }

    /**
     * Gets the calendar's owner for an event.
     * @param calId
     * @return email of owner or null
     */
    private String getOwner(long calId) {
        if (calId < 0) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Calendar Id is not valid: " + calId);
            }
            return null;
        }
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
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Couldn't find " + calId + " in Calendars table");
                }
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
                    if (Log.isLoggable(TAG, Log.DEBUG)) {
                        Log.d(TAG, "Couldn't find " + eventId + " in Events table");
                    }
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
                    if (Log.isLoggable(TAG, Log.DEBUG)) {
                        Log.d(TAG, "Couldn't find " + calId + " in Calendars table");
                    }
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
        db.update(Tables.EVENTS, values, SQL_WHERE_ID,
                new String[] {String.valueOf(eventId)});
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
            if (Log.isLoggable(TAG, Log.VERBOSE)) {
                Log.v(TAG, "Missing DTSTART.  No need to update instance.");
            }
            return;
        }

        Long lastDateMillis = values.getAsLong(Events.LAST_DATE);
        Long originalInstanceTime = values.getAsLong(Events.ORIGINAL_INSTANCE_TIME);

        if (!newEvent) {
            // Want to do this for regular event, recurrence, or exception.
            // For recurrence or exception, more deletion may happen below if we
            // do an instance expansion.  This deletion will suffice if the exception
            // is moved outside the window, for instance.
            db.delete(Tables.INSTANCES, Instances.EVENT_ID + "=?",
                    new String[] {String.valueOf(rowId)});
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

        qb.setTables(CalendarDatabaseHelper.Views.EVENTS);
        qb.setProjectionMap(sEventsProjectionMap);
        String selectionArgs[];
        if (recurrenceSyncId == null) {
            String where = SQL_WHERE_ID;
            qb.appendWhere(where);
            selectionArgs = new String[] {String.valueOf(rowId)};
        } else {
            String where = Events._SYNC_ID + "=? OR " + Events.ORIGINAL_EVENT + "=?";
            qb.appendWhere(where);
            selectionArgs = new String[] {recurrenceSyncId, recurrenceSyncId};
        }
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "Retrieving events to expand: " + qb.toString());
        }

        return qb.query(mDb, EXPAND_COLUMNS, null /* selection */, selectionArgs,
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
        String instancesTimezone = mCalendarCache.readTimezoneInstances();
        String originalEvent = values.getAsString(Events.ORIGINAL_EVENT);
        String recurrenceSyncId;
        if (originalEvent != null) {
            recurrenceSyncId = originalEvent;
        } else {
            // Get the recurrence's sync id from the database
            recurrenceSyncId = DatabaseUtils.stringForQuery(db,
                    SQL_SELECT_EVENTS_SYNC_ID, new String[] {String.valueOf(rowId)});
        }
        // recurrenceSyncId is the _sync_id of the underlying recurrence
        // If the recurrence hasn't gone to the server, it will be null.

        // Need to clear out old instances
        if (recurrenceSyncId == null) {
            // Creating updating a recurrence that hasn't gone to the server.
            // Need to delete based on row id
            String where = SQL_WHERE_ID_FROM_INSTANCES_NOT_SYNCED;
            db.delete(Tables.INSTANCES, where, new String[]{"" + rowId});
        } else {
            // Creating or modifying a recurrence or exception.
            // Delete instances for recurrence (_sync_id = recurrenceSyncId)
            // and all exceptions (originalEvent = recurrenceSyncId)
            String where = SQL_WHERE_ID_FROM_INSTANCES_SYNCED;
            db.delete(Tables.INSTANCES, where, new String[]{recurrenceSyncId, recurrenceSyncId});
        }

        // Now do instance expansion
        Cursor entries = getRelevantRecurrenceEntries(recurrenceSyncId, rowId);
        try {
            performInstanceExpansion(fields.minInstance, fields.maxInstance, instancesTimezone,
                                     entries);
        } finally {
            if (entries != null) {
                entries.close();
            }
        }
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

            RecurrenceSet recur = null;
            try {
                recur = new RecurrenceSet(values);
            } catch (EventRecurrence.InvalidFormatException e) {
                if (Log.isLoggable(TAG, Log.WARN)) {
                    Log.w(TAG, "Could not parse RRULE recurrence string: " +
                            values.get(Calendar.Events.RRULE), e);
                }
                return lastMillis; // -1
            }

            if (null != recur && recur.hasRecurrence()) {
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

    /**
     * Add LAST_DATE to values.
     * @param values the ContentValues (in/out)
     * @return values on success, null on failure
     */
    private ContentValues updateLastDate(ContentValues values) {
        try {
            long last = calculateLastDate(values);
            if (last != -1) {
                values.put(Events.LAST_DATE, last);
            }

            return values;
        } catch (DateException e) {
            // don't add it if there was an error
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Could not calculate last date.", e);
            }
            return null;
        }
    }

    private void updateEventRawTimesLocked(long eventId, ContentValues values) {
        ContentValues rawValues = new ContentValues();

        rawValues.put(Calendar.EventsRawTimes.EVENT_ID, eventId);

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
            rawValues.put(Calendar.EventsRawTimesColumns.DTSTART_2445, time.format2445());
        }

        Long dtendMillis = values.getAsLong(Events.DTEND);
        if (dtendMillis != null) {
            time.set(dtendMillis);
            rawValues.put(Calendar.EventsRawTimesColumns.DTEND_2445, time.format2445());
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
            rawValues.put(Calendar.EventsRawTimesColumns.ORIGINAL_INSTANCE_TIME_2445,
                    time.format2445());
        }

        Long lastDateMillis = values.getAsLong(Events.LAST_DATE);
        if (lastDateMillis != null) {
            time.allDay = allDay;
            time.set(lastDateMillis);
            rawValues.put(Calendar.EventsRawTimesColumns.LAST_DATE_2445, time.format2445());
        }

        mDbHelper.eventsRawTimesReplace(rawValues);
    }

    @Override
    protected int deleteInTransaction(Uri uri, String selection, String[] selectionArgs,
            boolean callerIsSyncAdapter) {
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "deleteInTransaction: " + uri);
        }
        final int match = sUriMatcher.match(uri);
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().delete(mDb, selection, selectionArgs);

            case SYNCSTATE_ID:
                String selectionWithId = (BaseColumns._ID + "=?")
                        + (selection == null ? "" : " AND (" + selection + ")");
                // Prepend id to selectionArgs
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(ContentUris.parseId(uri)));
                return mDbHelper.getSyncState().delete(mDb, selectionWithId,
                        selectionArgs);

            case EVENTS:
            {
                int result = 0;
                selection = appendAccountToSelection(uri, selection);

                // Query this event to get the ids to delete.
                Cursor cursor = mDb.query(Tables.EVENTS, ID_ONLY_PROJECTION,
                        selection, selectionArgs, null /* groupBy */,
                        null /* having */, null /* sortOrder */);
                try {
                    while (cursor.moveToNext()) {
                        long id = cursor.getLong(0);
                        result += deleteEventInternal(id, callerIsSyncAdapter, true /* isBatch */);
                    }
                    scheduleNextAlarm(false /* do not remove alarms */);
                    sendUpdateNotification(callerIsSyncAdapter);
                } finally {
                    cursor.close();
                    cursor = null;
                }
                return result;
            }
            case EVENTS_ID:
            {
                long id = ContentUris.parseId(uri);
                if (selection != null) {
                    throw new UnsupportedOperationException("CalendarProvider2 "
                            + "doesn't support selection based deletion for type "
                            + match);
                }
                return deleteEventInternal(id, callerIsSyncAdapter, false /* isBatch */);
            }
            case ATTENDEES:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete(Tables.ATTENDEES, selection, selectionArgs);
                } else {
                    return deleteFromTable(Tables.ATTENDEES, uri, selection, selectionArgs);
                }
            }
            case ATTENDEES_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete(Tables.ATTENDEES, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    return deleteFromTable(Tables.ATTENDEES, uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case REMINDERS:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete(Tables.REMINDERS, selection, selectionArgs);
                } else {
                    return deleteFromTable(Tables.REMINDERS, uri, selection, selectionArgs);
                }
            }
            case REMINDERS_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete(Tables.REMINDERS, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    return deleteFromTable(Tables.REMINDERS, uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case EXTENDED_PROPERTIES:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete(Tables.EXTENDED_PROPERTIES, selection, selectionArgs);
                } else {
                    return deleteFromTable(Tables.EXTENDED_PROPERTIES, uri, selection,
                            selectionArgs);
                }
            }
            case EXTENDED_PROPERTIES_ID:
            {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    return mDb.delete(Tables.EXTENDED_PROPERTIES, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    return deleteFromTable(Tables.EXTENDED_PROPERTIES, uri, null /* selection */,
                                           null /* selectionArgs */);
                }
            }
            case CALENDAR_ALERTS:
            {
                if (callerIsSyncAdapter) {
                    return mDb.delete(Tables.CALENDAR_ALERTS, selection, selectionArgs);
                } else {
                    return deleteFromTable(Tables.CALENDAR_ALERTS, uri, selection, selectionArgs);
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
                return mDb.delete(Tables.CALENDAR_ALERTS, SQL_WHERE_ID,
                        new String[] {String.valueOf(id)});
            }
            case DELETED_EVENTS:
                throw new UnsupportedOperationException("Cannot delete that URL: " + uri);
            case CALENDARS_ID:
                StringBuilder selectionSb = new StringBuilder(BaseColumns._ID + "=");
                selectionSb.append(uri.getPathSegments().get(1));
                if (!TextUtils.isEmpty(selection)) {
                    selectionSb.append(" AND (");
                    selectionSb.append(selection);
                    selectionSb.append(')');
                }
                selection = selectionSb.toString();
                // fall through to CALENDARS for the actual delete
            case CALENDARS:
                selection = appendAccountToSelection(uri, selection);
                return deleteMatchingCalendars(selection); // TODO: handle in sync adapter
            case INSTANCES:
            case INSTANCES_BY_DAY:
            case EVENT_DAYS:
            case PROVIDER_PROPERTIES:
                throw new UnsupportedOperationException("Cannot delete that URL");
            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }
    }

    private int deleteEventInternal(long id, boolean callerIsSyncAdapter, boolean isBatch) {
        int result = 0;
        String selectionArgs[] = new String[] {String.valueOf(id)};

        // Query this event to get the fields needed for deleting.
        Cursor cursor = mDb.query(Tables.EVENTS, EVENTS_PROJECTION,
                SQL_WHERE_ID, selectionArgs,
                null /* groupBy */,
                null /* having */, null /* sortOrder */);
        try {
            if (cursor.moveToNext()) {
                result = 1;
                String syncId = cursor.getString(EVENTS_SYNC_ID_INDEX);
                String rRule = cursor.getString(EVENTS_RRULE_INDEX);
                boolean emptyRRule = TextUtils.isEmpty(rRule);
                boolean emptySyncId = TextUtils.isEmpty(syncId);
                if (!emptySyncId && !emptyRRule) {
                    // Delete exceptions to this event as well.
                    mDb.delete(Tables.EVENTS, SQL_WHERE_ORIGINAL_EVENT,
                            new String[] {syncId});
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

                // we clean the Events and Attendees table if the caller is CalendarSyncAdapter
                // or if the event is local (no syncId)
                if (callerIsSyncAdapter || emptySyncId) {
                    mDb.delete(Tables.EVENTS, SQL_WHERE_ID, selectionArgs);
                } else {
                    ContentValues values = new ContentValues();
                    values.put(Events.DELETED, 1);
                    values.put(Events._SYNC_DIRTY, 1);
                    mDb.update(Tables.EVENTS, values, SQL_WHERE_ID, selectionArgs);

                    // Delete associated data; attendees, however, are deleted with the actual event
                    //  so that the sync adapter is able to notify attendees of the cancellation.
                    mDb.delete(Tables.INSTANCES, SQL_WHERE_EVENT_ID, selectionArgs);
                    mDb.delete(Tables.EVENTS_RAW_TIMES, SQL_WHERE_EVENT_ID, selectionArgs);
                    mDb.delete(Tables.REMINDERS, SQL_WHERE_EVENT_ID, selectionArgs);
                    mDb.delete(Tables.CALENDAR_ALERTS, SQL_WHERE_EVENT_ID, selectionArgs);
                    mDb.delete(Tables.EXTENDED_PROPERTIES, SQL_WHERE_EVENT_ID,
                            selectionArgs);
                }
            }
        } finally {
            cursor.close();
            cursor = null;
        }

        if (!isBatch) {
            scheduleNextAlarm(false /* do not remove alarms */);
            sendUpdateNotification(callerIsSyncAdapter);
        }
        return result;
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
                mDb.delete(table, SQL_WHERE_ID, new String[] {String.valueOf(id)});
                mDb.update(Tables.EVENTS, values, SQL_WHERE_ID,
                        new String[] {String.valueOf(event_id)});
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
                mDb.update(table, values, SQL_WHERE_ID, new String[] {String.valueOf(id)});
                mDb.update(Tables.EVENTS, dirtyValues, SQL_WHERE_ID,
                        new String[] {String.valueOf(event_id)});
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

        Cursor c = mDb.query(Tables.CALENDARS, sCalendarsIdProjection, where,
                null /* selectionArgs */, null /* groupBy */,
                null /* having */, null /* sortOrder */);
        if (c == null) {
            return 0;
        }
        try {
            while (c.moveToNext()) {
                long id = c.getLong(CALENDARS_INDEX_ID);
                modifyCalendarSubscription(id, false /* not selected */);
            }
        } finally {
            c.close();
        }
        return mDb.delete(Tables.CALENDARS, where, null /* whereArgs */);
    }

    // TODO: call calculateLastDate()!
    @Override
    protected int updateInTransaction(Uri uri, ContentValues values, String selection,
            String[] selectionArgs, boolean callerIsSyncAdapter) {
        if (Log.isLoggable(TAG, Log.VERBOSE)) {
            Log.v(TAG, "updateInTransaction: " + uri);
        }

        int count = 0;
        final int match = sUriMatcher.match(uri);

        // TODO: remove this restriction
        if (!TextUtils.isEmpty(selection) && match != CALENDAR_ALERTS
                && match != EVENTS && match != CALENDARS && match != PROVIDER_PROPERTIES) {
            throw new IllegalArgumentException("WHERE based updates not supported");
        }
        switch (match) {
            case SYNCSTATE:
                return mDbHelper.getSyncState().update(mDb, values,
                        appendAccountToSelection(uri, selection), selectionArgs);

            case SYNCSTATE_ID: {
                selection = appendAccountToSelection(uri, selection);
                String selectionWithId = (BaseColumns._ID + "=?")
                        + (selection == null ? "" : " AND (" + selection + ")");
                // Prepend id to selectionArgs
                selectionArgs = insertSelectionArg(selectionArgs,
                        String.valueOf(ContentUris.parseId(uri)));
                return mDbHelper.getSyncState().update(mDb, values, selectionWithId, selectionArgs);
            }

            case CALENDARS:
            case CALENDARS_ID:
            {
                long id;
                if (match == CALENDARS_ID) {
                    if (selection != null) {
                        throw new UnsupportedOperationException("Selection not permitted for "
                                + uri);
                    }
                    id = ContentUris.parseId(uri);
                } else {
                    // TODO: for supporting other sync adapters, we will need to
                    // be able to deal with the following cases:
                    // 1) selection to "_id=?" and pass in a selectionArgs
                    // 2) selection to "_id IN (1, 2, 3)"
                    // 3) selection to "delete=0 AND _id=1"
                    if (selection != null && selection.startsWith("_id=")) {
                        // The ContentProviderOperation generates an _id=n string instead of
                        // adding the id to the URL, so parse that out here.
                        id = Long.parseLong(selection.substring(4));
                    } else {
                        return mDb.update(Tables.CALENDARS, values, selection, selectionArgs);
                    }
                }
                if (!callerIsSyncAdapter) {
                    values.put(Calendars._SYNC_DIRTY, 1);
                }
                Integer syncEvents = values.getAsInteger(Calendars.SYNC_EVENTS);
                if (syncEvents != null) {
                    modifyCalendarSubscription(id, syncEvents == 1);
                }

                int result = mDb.update(Tables.CALENDARS, values, SQL_WHERE_ID,
                        new String[] {String.valueOf(id)});

                if (result > 0) {
                    // if visibility was toggled, we need to update alarms
                    if (values.containsKey(Calendars.SELECTED)) {
                        // pass false for removeAlarms since the call to
                        // scheduleNextAlarmLocked will remove any alarms for
                        // non-visible events anyways. removeScheduledAlarmsLocked
                        // does not actually have the effect we want
                        scheduleNextAlarm(false);
                    }
                    // update the widget
                    sendUpdateNotification(callerIsSyncAdapter);
                }

                return result;
            }
            case EVENTS:
            case EVENTS_ID:
            {
                long id = 0;
                if (match == EVENTS_ID) {
                    id = ContentUris.parseId(uri);
                } else if (callerIsSyncAdapter) {
                    // TODO: same remark as for CALENDARS/CALENDARS_ID case as this is not
                    // sufficient to deal with all the "_id" case in selection
                    if (selection != null && selection.startsWith("_id=")) {
                        // The ContentProviderOperation generates an _id=n string instead of
                        // adding the id to the URL, so parse that out here.
                        id = Long.parseLong(selection.substring(4));
                    } else {
                        // Sync adapter Events operation affects just Events table, not associated
                        // tables.
                        if (fixAllDayTime(uri, values)) {
                            if (Log.isLoggable(TAG, Log.WARN)) {
                                Log.w(TAG, "updateInTransaction: Caller is sync adapter. " +
                                        "allDay is true but sec, min, hour were not 0.");
                            }
                        }
                        return mDb.update("Events", values, selection, selectionArgs);
                    }
                } else {
                    throw new IllegalArgumentException("Unknown URL " + uri);
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
                ContentValues updatedValues = new ContentValues(values);
                // TODO: should extend validateEventData to work with updates and call it here
                updatedValues = updateLastDate(updatedValues);
                if (updatedValues == null) {
                    if (Log.isLoggable(TAG, Log.WARN)) {
                        Log.w(TAG, "Could not update event.");
                    }
                    return 0;
                }
                // Make sure we pass in a uri with the id appended to fixAllDayTime
                Uri allDayUri;
                if (uri.getPathSegments().size() == 1) {
                    allDayUri = ContentUris.withAppendedId(uri, id);
                } else {
                    allDayUri = uri;
                }
                if (fixAllDayTime(allDayUri, updatedValues)) {
                    if (Log.isLoggable(TAG, Log.WARN)) {
                        Log.w(TAG, "updateInTransaction: " +
                                "allDay is true but sec, min, hour were not 0.");
                    }
                }

                int result = mDb.update(Tables.EVENTS, updatedValues, SQL_WHERE_ID,
                        new String[] {String.valueOf(id)});
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
                    }

                    sendUpdateNotification(id, callerIsSyncAdapter);
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
                    return mDb.update(Tables.ATTENDEES, values, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    return updateInTable(Tables.ATTENDEES, values, uri, null /* selection */,
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
                return mDb.update(Tables.CALENDAR_ALERTS, values, SQL_WHERE_ID,
                        new String[] {String.valueOf(id)});
            }
            case CALENDAR_ALERTS: {
                // Note: dirty bit is not set for Alerts because it is not synced.
                // It is generated from Reminders, which is synced.
                return mDb.update(Tables.CALENDAR_ALERTS, values, selection, selectionArgs);
            }
            case REMINDERS_ID: {
                if (selection != null) {
                    throw new UnsupportedOperationException("Selection not permitted for " + uri);
                }
                if (callerIsSyncAdapter) {
                    long id = ContentUris.parseId(uri);
                    count = mDb.update(Tables.REMINDERS, values, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    count = updateInTable(Tables.REMINDERS, values, uri, null /* selection */,
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
                    return mDb.update(Tables.EXTENDED_PROPERTIES, values, SQL_WHERE_ID,
                            new String[] {String.valueOf(id)});
                } else {
                    return updateInTable(Tables.EXTENDED_PROPERTIES, values, uri,
                            null /* selection */, null /* selectionArgs */);
                }
            }
            // TODO: replace the SCHEDULE_ALARM private URIs with a
            // service
            case SCHEDULE_ALARM: {
                scheduleNextAlarm(false);
                return 0;
            }
            case SCHEDULE_ALARM_REMOVE: {
                scheduleNextAlarm(true);
                return 0;
            }

            case PROVIDER_PROPERTIES: {
                if (selection == null) {
                    throw new UnsupportedOperationException("Selection cannot be null for " + uri);
                }
                if (!selection.equals("key=?")) {
                    throw new UnsupportedOperationException("Selection should be key=? for " + uri);
                }

                List<String> list = Arrays.asList(selectionArgs);

                if (list.contains(CalendarCache.KEY_TIMEZONE_INSTANCES_PREVIOUS)) {
                    throw new UnsupportedOperationException("Invalid selection key: " +
                            CalendarCache.KEY_TIMEZONE_INSTANCES_PREVIOUS + " for " + uri);
                }

                // Before it may be changed, save current Instances timezone for later use
                String timezoneInstancesBeforeUpdate = mCalendarCache.readTimezoneInstances();

                // Update the database with the provided values (this call may change the value
                // of timezone Instances)
                int result = mDb.update(Tables.CALENDAR_CACHE, values, selection, selectionArgs);

                // if successful, do some house cleaning:
                // if the timezone type is set to "home", set the Instances timezone to the previous
                // if the timezone type is set to "auto", set the Instances timezone to the current
                //      device one
                // if the timezone Instances is set AND if we are in "home" timezone type, then
                //      save the timezone Instance into "previous" too
                if (result > 0) {
                    // If we are changing timezone type...
                    if (list.contains(CalendarCache.KEY_TIMEZONE_TYPE)) {
                        String value = values.getAsString(CalendarCache.COLUMN_NAME_VALUE);
                        if (value != null) {
                            // if we are setting timezone type to "home"
                            if (value.equals(CalendarCache.TIMEZONE_TYPE_HOME)) {
                                String previousTimezone =
                                        mCalendarCache.readTimezoneInstancesPrevious();
                                if (previousTimezone != null) {
                                    mCalendarCache.writeTimezoneInstances(previousTimezone);
                                }
                                // Regenerate Instances if the "home" timezone has changed
                                // and notify widgets
                                if (!timezoneInstancesBeforeUpdate.equals(previousTimezone) ) {
                                    regenerateInstancesTable();
                                    sendUpdateNotification(callerIsSyncAdapter);
                                }
                            }
                            // if we are setting timezone type to "auto"
                            else if (value.equals(CalendarCache.TIMEZONE_TYPE_AUTO)) {
                                String localTimezone = TimeZone.getDefault().getID();
                                mCalendarCache.writeTimezoneInstances(localTimezone);
                                if (!timezoneInstancesBeforeUpdate.equals(localTimezone)) {
                                    regenerateInstancesTable();
                                    sendUpdateNotification(callerIsSyncAdapter);
                                }
                            }
                        }
                    }
                    // If we are changing timezone Instances...
                    else if (list.contains(CalendarCache.KEY_TIMEZONE_INSTANCES)) {
                        // if we are in "home" timezone type...
                        if (isHomeTimezone()) {
                            String timezoneInstances = mCalendarCache.readTimezoneInstances();
                            // Update the previous value
                            mCalendarCache.writeTimezoneInstancesPrevious(timezoneInstances);
                            // Recompute Instances if the "home" timezone has changed
                            // and send notifications to any widgets
                            if (timezoneInstancesBeforeUpdate != null &&
                                    !timezoneInstancesBeforeUpdate.equals(timezoneInstances)) {
                                regenerateInstancesTable();
                                sendUpdateNotification(callerIsSyncAdapter);
                            }
                        }
                    }
                }
                return result;
            }

            default:
                throw new IllegalArgumentException("Unknown URL " + uri);
        }
    }

    private void appendAccountFromParameter(SQLiteQueryBuilder qb, Uri uri) {
        final String accountName = QueryParameterUtils.getQueryParameter(uri,
                Calendar.EventsEntity.ACCOUNT_NAME);
        final String accountType = QueryParameterUtils.getQueryParameter(uri,
                Calendar.EventsEntity.ACCOUNT_TYPE);
        if (!TextUtils.isEmpty(accountName)) {
            qb.appendWhere(Calendar.Calendars._SYNC_ACCOUNT + "="
                    + DatabaseUtils.sqlEscapeString(accountName) + " AND "
                    + Calendar.Calendars._SYNC_ACCOUNT_TYPE + "="
                    + DatabaseUtils.sqlEscapeString(accountType));
        } else {
            qb.appendWhere("1"); // I.e. always true
        }
    }

    private String appendAccountToSelection(Uri uri, String selection) {
        final String accountName = QueryParameterUtils.getQueryParameter(uri,
                Calendar.EventsEntity.ACCOUNT_NAME);
        final String accountType = QueryParameterUtils.getQueryParameter(uri,
                Calendar.EventsEntity.ACCOUNT_TYPE);
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
                        Calendars.SYNC1, Calendars.SYNC_EVENTS},
                null /* selection */,
                null /* selectionArgs */,
                null /* sort */);

        Account account = null;
        String calendarUrl = null;
        boolean oldSyncEvents = false;
        if (cursor != null) {
            try {
                if (cursor.moveToFirst()) {
                    final String accountName = cursor.getString(0);
                    final String accountType = cursor.getString(1);
                    account = new Account(accountName, accountType);
                    calendarUrl = cursor.getString(2);
                    oldSyncEvents = (cursor.getInt(3) != 0);
                }
            } finally {
                cursor.close();
            }
        }

        if (account == null) {
            // should not happen?
            if (Log.isLoggable(TAG, Log.WARN)) {
                Log.w(TAG, "Cannot update subscription because account "
                        + "is empty -- should not happen.");
            }
            return;
        }

        if (TextUtils.isEmpty(calendarUrl)) {
            // Passing in a null Url will cause it to not add any extras
            // Should only happen for non-google calendars.
            calendarUrl = null;
        }

        if (oldSyncEvents == syncEvents) {
            // nothing to do
            return;
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

    /* Retrieve and cache the alarm manager */
    private AlarmManager getAlarmManager() {
        synchronized(mAlarmLock) {
            if (mAlarmManager == null) {
                Context context = getContext();
                if (context == null) {
                    if (Log.isLoggable(TAG, Log.ERROR)) {
                        Log.e(TAG, "getAlarmManager() cannot get Context");
                    }
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
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "scheduleNextAlarmCheck() cannot get AlarmManager");
            }
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
        Time time = new Time();
        AlarmManager alarmManager = getAlarmManager();
        if (alarmManager == null) {
            if (Log.isLoggable(TAG, Log.ERROR)) {
                Log.e(TAG, "Failed to find the AlarmManager. Could not schedule the next alarm!");
            }
            return;
        }

        final long currentMillis = System.currentTimeMillis();
        final long start = currentMillis - SCHEDULE_ALARM_SLACK;
        final long end = start + (24 * 60 * 60 * 1000);
        ContentResolver cr = getContext().getContentResolver();
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            time.set(start);
            String startTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
            Log.d(TAG, "runScheduleNextAlarm() start search: " + startTimeStr);
        }

        // Delete rows in CalendarAlert where the corresponding Instance or
        // Reminder no longer exist.
        // Also clear old alarms but keep alarms around for a while to prevent
        // multiple alerts for the same reminder.  The "clearUpToTime'
        // should be further in the past than the point in time where
        // we start searching for events (the "start" variable defined above).
        String selectArg[] = new String[] {
            Long.toString(currentMillis - CLEAR_OLD_ALARM_THRESHOLD)
        };

        int rowsDeleted =
            db.delete(CalendarAlerts.TABLE_NAME, INVALID_CALENDARALERTS_SELECTOR, selectArg);

        long nextAlarmTime = end;
        final long tmpAlarmTime = CalendarAlerts.findNextAlarmTime(cr, currentMillis);
        if (tmpAlarmTime != -1 && tmpAlarmTime < nextAlarmTime) {
            nextAlarmTime = tmpAlarmTime;
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
        //
        // The CAST is needed in the query because otherwise the expression
        // will be untyped and sqlite3's manifest typing will not convert the
        // string query parameter to an int in myAlarmtime>=?, so the comparison
        // will fail.  This could be simplified if bug 2464440 is resolved.

        time.setToNow();
        time.normalize(false);
        long localOffset = time.gmtoff * 1000;

        String allDayOffset = " -(" + localOffset + ") ";
        String subQueryPrefix = "SELECT " + Instances.BEGIN;
        String subQuerySuffix = " -(" + Reminders.MINUTES + "*" +
                + DateUtils.MINUTE_IN_MILLIS + ")"
                + " AS myAlarmTime" + "," + Tables.INSTANCES
                + "." + Instances.EVENT_ID + " AS eventId"
                + "," + Instances.BEGIN + "," + Instances.END
                + "," + Instances.TITLE + "," + Instances.ALL_DAY
                + "," + Reminders.METHOD + "," + Reminders.MINUTES
                + " FROM " + Tables.INSTANCES
                + " INNER JOIN " + Views.EVENTS
                + " ON (" + Views.EVENTS + "." + Events._ID
                + "=" + Tables.INSTANCES + "." + Instances.EVENT_ID + ")"
                + " INNER JOIN " + Tables.REMINDERS
                + " ON (" + Tables.INSTANCES + "." + Instances.EVENT_ID
                + "=" + Tables.REMINDERS + "." + Reminders.EVENT_ID + ")"
                + " WHERE " + Calendars.SELECTED + "=1"
                + " AND myAlarmTime>=CAST(? AS INT)"
                + " AND myAlarmTime<=CAST(? AS INT)"
                + " AND " + Instances.END + ">=?"
                + " AND " + Reminders.METHOD + "=" + Reminders.METHOD_ALERT;

        // we query separately for all day events to convert to local time from UTC
        // we need to /subtract/ the offset to get the correct resulting local time
        String allDayQuery = subQueryPrefix + allDayOffset + subQuerySuffix
                + " AND " + Instances.ALL_DAY + "=1";
        String nonAllDayQuery = subQueryPrefix + subQuerySuffix
                + " AND " + Instances.ALL_DAY + "=0";

        // we use UNION ALL because we are guaranteed to have no dupes between
        // the two queries, and it is less expensive
        String query = "SELECT *"
                + " FROM (" + allDayQuery + " UNION ALL " + nonAllDayQuery + ")"
                // avoid rescheduling existing alarms
                + " WHERE 0=(SELECT count(*) FROM " + Tables.CALENDAR_ALERTS + " CA"
                         + " WHERE CA." + CalendarAlerts.EVENT_ID + "=eventId"
                         + " AND CA." + CalendarAlerts.BEGIN + "=" + Instances.BEGIN
                         + " AND CA." + CalendarAlerts.ALARM_TIME + "=myAlarmTime)"
                + " ORDER BY myAlarmTime," + Instances.BEGIN + "," + Instances.TITLE;

        String queryParams[] = new String[] { String.valueOf(start), String.valueOf(nextAlarmTime),
                String.valueOf(currentMillis), String.valueOf(start), String.valueOf(nextAlarmTime),
                String.valueOf(currentMillis) };

        String instancesTimezone = mCalendarCache.readTimezoneInstances();
        boolean isHomeTimezone = mCalendarCache.readTimezoneType().equals(
                CalendarCache.TIMEZONE_TYPE_HOME);
        // expand this range by a day on either end to account for all day events
        acquireInstanceRangeLocked(start - DateUtils.DAY_IN_MILLIS,
                end + DateUtils.DAY_IN_MILLIS,
                false /* don't use minimum expansion windows */,
                false /* do not force Instances deletion and expansion */,
                instancesTimezone,
                isHomeTimezone);
        Cursor cursor = null;
        try {
            cursor = db.rawQuery(query, queryParams);

            final int beginIndex = cursor.getColumnIndex(Instances.BEGIN);
            final int endIndex = cursor.getColumnIndex(Instances.END);
            final int eventIdIndex = cursor.getColumnIndex("eventId");
            final int alarmTimeIndex = cursor.getColumnIndex("myAlarmTime");
            final int minutesIndex = cursor.getColumnIndex(Reminders.MINUTES);

            if (Log.isLoggable(TAG, Log.DEBUG)) {
                time.set(nextAlarmTime);
                String alarmTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");
                Log.d(TAG, "cursor results: " + cursor.getCount() + " nextAlarmTime: "
                        + alarmTimeStr);
            }

            while (cursor.moveToNext()) {
                // Schedule all alarms whose alarm time is as early as any
                // scheduled alarm.  For example, if the earliest alarm is at
                // 1pm, then we will schedule all alarms that occur at 1pm
                // but no alarms that occur later than 1pm.
                // Actually, we allow alarms up to a minute later to also
                // be scheduled so that we don't have to check immediately
                // again after an event alarm goes off.
                final long alarmTime = cursor.getLong(alarmTimeIndex);
                final long eventId = cursor.getLong(eventIdIndex);
                final int minutes = cursor.getInt(minutesIndex);
                final long startTime = cursor.getLong(beginIndex);
                final long endTime = cursor.getLong(endIndex);

                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    time.set(alarmTime);
                    String schedTime = time.format(" %a, %b %d, %Y %I:%M%P");
                    time.set(startTime);
                    String startTimeStr = time.format(" %a, %b %d, %Y %I:%M%P");

                    Log.d(TAG, "  looking at id: " + eventId + " " + startTime + startTimeStr
                            + " alarm: " + alarmTime + schedTime);
                }

                if (alarmTime < nextAlarmTime) {
                    nextAlarmTime = alarmTime;
                } else if (alarmTime >
                           nextAlarmTime + DateUtils.MINUTE_IN_MILLIS) {
                    // This event alarm (and all later ones) will be scheduled
                    // later.
                    if (Log.isLoggable(TAG, Log.DEBUG)) {
                        Log.d(TAG, "This event alarm (and all later ones) will be scheduled later");
                    }
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
                Uri uri = CalendarAlerts.insert(cr, eventId, startTime,
                        endTime, alarmTime, minutes);
                if (uri == null) {
                    if (Log.isLoggable(TAG, Log.ERROR)) {
                        Log.e(TAG, "runScheduleNextAlarm() insert into "
                                + "CalendarAlerts table failed");
                    }
                    continue;
                }

                CalendarAlerts.scheduleAlarm(getContext(), alarmManager, alarmTime);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // Refresh notification bar
        if (rowsDeleted > 0) {
            CalendarAlerts.scheduleAlarm(getContext(), alarmManager, currentMillis);
        }

        // If we scheduled an event alarm, then schedule the next alarm check
        // for one minute past that alarm.  Otherwise, if there were no
        // event alarms scheduled, then check again in 24 hours.  If a new
        // event is inserted before the next alarm check, then this method
        // will be run again when the new event is inserted.
        if (nextAlarmTime != Long.MAX_VALUE) {
            scheduleNextAlarmCheck(nextAlarmTime + DateUtils.MINUTE_IN_MILLIS);
        } else {
            scheduleNextAlarmCheck(currentMillis + DateUtils.DAY_IN_MILLIS);
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

    /**
     * Call this to trigger a broadcast of the ACTION_PROVIDER_CHANGED intent.
     * This also provides a timeout, so any calls to this method will be batched
     * over a period of BROADCAST_TIMEOUT_MILLIS defined in this class.
     *
     * @param whether or not the update is being triggered by a sync
     */
    private void sendUpdateNotification(boolean callerIsSyncAdapter) {
        // We use -1 to represent an update to all events
        sendUpdateNotification(-1, callerIsSyncAdapter);
    }

    /**
     * Call this to trigger a broadcast of the ACTION_PROVIDER_CHANGED intent.
     * This also provides a timeout, so any calls to this method will be batched
     * over a period of BROADCAST_TIMEOUT_MILLIS defined in this class.  The
     * actual sending of the intent is done in
     * {@link #doSendUpdateNotification()}.
     *
     * TODO add support for eventId
     *
     * @param the ID of the event that changed, or -1 for no specific event
     * @param whether or not the update is being triggered by a sync
     */
    private void sendUpdateNotification(long eventId,
            boolean callerIsSyncAdapter) {
        // Are there any pending broadcast requests?
        if (mBroadcastHandler.hasMessages(UPDATE_BROADCAST_MSG)) {
            // Delete any pending requests, before requeuing a fresh one
            mBroadcastHandler.removeMessages(UPDATE_BROADCAST_MSG);
        } else {
            // Because the handler does not guarantee message delivery in
            // the case that the provider is killed, we need to make sure
            // that the provider stays alive long enough to deliver the
            // notification. This empty service is sufficient to "wedge" the
            // process until we stop it here.
            mContext.startService(new Intent(mContext, EmptyService.class));
        }
        // We use a much longer delay for sync-related updates, to prevent any
        // receivers from slowing down the sync
        long delay = callerIsSyncAdapter ?
                SYNC_UPDATE_BROADCAST_TIMEOUT_MILLIS :
                UPDATE_BROADCAST_TIMEOUT_MILLIS;
        // Despite the fact that we actually only ever use one message at a time
        // for now, it is really important to call obtainMessage() to get a
        // clean instance.  This avoids potentially infinite loops resulting
        // adding the same instance to the message queue twice, since the
        // message queue implements its linked list using a field from Message.
        Message msg = mBroadcastHandler.obtainMessage(UPDATE_BROADCAST_MSG);
        mBroadcastHandler.sendMessageDelayed(msg, delay);
    }

    /**
     * This method should not ever be called directly, to prevent sending too
     * many potentially expensive broadcasts.  Instead, call
     * {@link #sendUpdateNotification()} instead.
     *
     * @see #sendUpdateNotification()
     */
    private void doSendUpdateNotification() {
        Intent intent = new Intent(Intent.ACTION_PROVIDER_CHANGED,
                Calendar.CONTENT_URI);
        if (Log.isLoggable(TAG, Log.INFO)) {
            Log.i(TAG, "Sending notification intent: " + intent);
        }
        getContext().sendBroadcast(intent, null);
    }

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
    private static final int INSTANCES_BY_DAY = 16;
    private static final int SYNCSTATE = 17;
    private static final int SYNCSTATE_ID = 18;
    private static final int EVENT_ENTITIES = 19;
    private static final int EVENT_ENTITIES_ID = 20;
    private static final int EVENT_DAYS = 21;
    private static final int SCHEDULE_ALARM = 22;
    private static final int SCHEDULE_ALARM_REMOVE = 23;
    private static final int TIME = 24;
    private static final int CALENDAR_ENTITIES = 25;
    private static final int CALENDAR_ENTITIES_ID = 26;
    private static final int INSTANCES_SEARCH = 27;
    private static final int INSTANCES_SEARCH_BY_DAY = 28;
    private static final int PROVIDER_PROPERTIES = 29;

    private static final UriMatcher sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
    private static final HashMap<String, String> sInstancesProjectionMap;
    private static final HashMap<String, String> sEventsProjectionMap;
    private static final HashMap<String, String> sEventEntitiesProjectionMap;
    private static final HashMap<String, String> sAttendeesProjectionMap;
    private static final HashMap<String, String> sRemindersProjectionMap;
    private static final HashMap<String, String> sCalendarAlertsProjectionMap;
    private static final HashMap<String, String> sCalendarCacheProjectionMap;

    static {
        sUriMatcher.addURI(Calendar.AUTHORITY, "instances/when/*/*", INSTANCES);
        sUriMatcher.addURI(Calendar.AUTHORITY, "instances/whenbyday/*/*", INSTANCES_BY_DAY);
        sUriMatcher.addURI(Calendar.AUTHORITY, "instances/search/*/*/*", INSTANCES_SEARCH);
        sUriMatcher.addURI(Calendar.AUTHORITY, "instances/searchbyday/*/*/*",
                INSTANCES_SEARCH_BY_DAY);
        sUriMatcher.addURI(Calendar.AUTHORITY, "instances/groupbyday/*/*", EVENT_DAYS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "events", EVENTS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "events/#", EVENTS_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "event_entities", EVENT_ENTITIES);
        sUriMatcher.addURI(Calendar.AUTHORITY, "event_entities/#", EVENT_ENTITIES_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendars", CALENDARS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendars/#", CALENDARS_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendar_entities", CALENDAR_ENTITIES);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendar_entities/#", CALENDAR_ENTITIES_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "deleted_events", DELETED_EVENTS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "attendees", ATTENDEES);
        sUriMatcher.addURI(Calendar.AUTHORITY, "attendees/#", ATTENDEES_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "reminders", REMINDERS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "reminders/#", REMINDERS_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "extendedproperties", EXTENDED_PROPERTIES);
        sUriMatcher.addURI(Calendar.AUTHORITY, "extendedproperties/#", EXTENDED_PROPERTIES_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendar_alerts", CALENDAR_ALERTS);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendar_alerts/#", CALENDAR_ALERTS_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, "calendar_alerts/by_instance",
                           CALENDAR_ALERTS_BY_INSTANCE);
        sUriMatcher.addURI(Calendar.AUTHORITY, "syncstate", SYNCSTATE);
        sUriMatcher.addURI(Calendar.AUTHORITY, "syncstate/#", SYNCSTATE_ID);
        sUriMatcher.addURI(Calendar.AUTHORITY, SCHEDULE_ALARM_PATH, SCHEDULE_ALARM);
        sUriMatcher.addURI(Calendar.AUTHORITY, SCHEDULE_ALARM_REMOVE_PATH, SCHEDULE_ALARM_REMOVE);
        sUriMatcher.addURI(Calendar.AUTHORITY, "time/#", TIME);
        sUriMatcher.addURI(Calendar.AUTHORITY, "time", TIME);
        sUriMatcher.addURI(Calendar.AUTHORITY, "properties", PROVIDER_PROPERTIES);

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

        // Put the shared items into the Attendees, Reminders projection map
        sAttendeesProjectionMap = new HashMap<String, String>(sEventsProjectionMap);
        sRemindersProjectionMap = new HashMap<String, String>(sEventsProjectionMap);

        // Calendar columns
        sEventsProjectionMap.put(Calendars.COLOR, "color");
        sEventsProjectionMap.put(Calendars.ACCESS_LEVEL, "access_level");
        sEventsProjectionMap.put(Calendars.SELECTED, "selected");
        sEventsProjectionMap.put(Calendars.SYNC1, "sync1");
        sEventsProjectionMap.put(Calendars.TIMEZONE, "timezone");
        sEventsProjectionMap.put(Calendars.OWNER_ACCOUNT, "ownerAccount");

        // Put the shared items into the Instances projection map
        // The Instances and CalendarAlerts are joined with Calendars, so the projections include
        // the above Calendar columns.
        sInstancesProjectionMap = new HashMap<String, String>(sEventsProjectionMap);
        sCalendarAlertsProjectionMap = new HashMap<String, String>(sEventsProjectionMap);

        sEventsProjectionMap.put(Events._ID, "_id");
        sEventsProjectionMap.put(Events._SYNC_ID, "_sync_id");
        sEventsProjectionMap.put(Events._SYNC_VERSION, "_sync_version");
        sEventsProjectionMap.put(Events._SYNC_TIME, "_sync_time");
        sEventsProjectionMap.put(Events._SYNC_DATA, "_sync_local_id");
        sEventsProjectionMap.put(Events._SYNC_DIRTY, "_sync_dirty");
        sEventsProjectionMap.put(Events._SYNC_ACCOUNT, "_sync_account");
        sEventsProjectionMap.put(Events._SYNC_ACCOUNT_TYPE,
                "_sync_account_type");

        sEventEntitiesProjectionMap = new HashMap<String, String>();
        sEventEntitiesProjectionMap.put(Events.HTML_URI, "htmlUri");
        sEventEntitiesProjectionMap.put(Events.TITLE, "title");
        sEventEntitiesProjectionMap.put(Events.DESCRIPTION, "description");
        sEventEntitiesProjectionMap.put(Events.EVENT_LOCATION, "eventLocation");
        sEventEntitiesProjectionMap.put(Events.STATUS, "eventStatus");
        sEventEntitiesProjectionMap.put(Events.SELF_ATTENDEE_STATUS, "selfAttendeeStatus");
        sEventEntitiesProjectionMap.put(Events.COMMENTS_URI, "commentsUri");
        sEventEntitiesProjectionMap.put(Events.DTSTART, "dtstart");
        sEventEntitiesProjectionMap.put(Events.DTEND, "dtend");
        sEventEntitiesProjectionMap.put(Events.DURATION, "duration");
        sEventEntitiesProjectionMap.put(Events.EVENT_TIMEZONE, "eventTimezone");
        sEventEntitiesProjectionMap.put(Events.ALL_DAY, "allDay");
        sEventEntitiesProjectionMap.put(Events.VISIBILITY, "visibility");
        sEventEntitiesProjectionMap.put(Events.TRANSPARENCY, "transparency");
        sEventEntitiesProjectionMap.put(Events.HAS_ALARM, "hasAlarm");
        sEventEntitiesProjectionMap.put(Events.HAS_EXTENDED_PROPERTIES, "hasExtendedProperties");
        sEventEntitiesProjectionMap.put(Events.RRULE, "rrule");
        sEventEntitiesProjectionMap.put(Events.RDATE, "rdate");
        sEventEntitiesProjectionMap.put(Events.EXRULE, "exrule");
        sEventEntitiesProjectionMap.put(Events.EXDATE, "exdate");
        sEventEntitiesProjectionMap.put(Events.ORIGINAL_EVENT, "originalEvent");
        sEventEntitiesProjectionMap.put(Events.ORIGINAL_INSTANCE_TIME, "originalInstanceTime");
        sEventEntitiesProjectionMap.put(Events.ORIGINAL_ALL_DAY, "originalAllDay");
        sEventEntitiesProjectionMap.put(Events.LAST_DATE, "lastDate");
        sEventEntitiesProjectionMap.put(Events.HAS_ATTENDEE_DATA, "hasAttendeeData");
        sEventEntitiesProjectionMap.put(Events.CALENDAR_ID, "calendar_id");
        sEventEntitiesProjectionMap.put(Events.GUESTS_CAN_INVITE_OTHERS, "guestsCanInviteOthers");
        sEventEntitiesProjectionMap.put(Events.GUESTS_CAN_MODIFY, "guestsCanModify");
        sEventEntitiesProjectionMap.put(Events.GUESTS_CAN_SEE_GUESTS, "guestsCanSeeGuests");
        sEventEntitiesProjectionMap.put(Events.ORGANIZER, "organizer");
        sEventEntitiesProjectionMap.put(Events.DELETED, "deleted");
        sEventEntitiesProjectionMap.put(Events._ID, Events._ID);
        sEventEntitiesProjectionMap.put(Events._SYNC_ID, Events._SYNC_ID);
        sEventEntitiesProjectionMap.put(Events._SYNC_DATA, Events._SYNC_DATA);
        sEventEntitiesProjectionMap.put(Events._SYNC_VERSION, Events._SYNC_VERSION);
        sEventEntitiesProjectionMap.put(Events._SYNC_DIRTY, Events._SYNC_DIRTY);
        sEventEntitiesProjectionMap.put(Calendars.SYNC1, Calendars.SYNC1);

        // Instances columns
        sInstancesProjectionMap.put(Events.DELETED, "Events.deleted as deleted");
        sInstancesProjectionMap.put(Instances.BEGIN, "begin");
        sInstancesProjectionMap.put(Instances.END, "end");
        sInstancesProjectionMap.put(Instances.EVENT_ID, "Instances.event_id AS event_id");
        sInstancesProjectionMap.put(Instances._ID, "Instances._id AS _id");
        sInstancesProjectionMap.put(Instances.START_DAY, "startDay");
        sInstancesProjectionMap.put(Instances.END_DAY, "endDay");
        sInstancesProjectionMap.put(Instances.START_MINUTE, "startMinute");
        sInstancesProjectionMap.put(Instances.END_MINUTE, "endMinute");

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

        // CalendarCache columns
        sCalendarCacheProjectionMap = new HashMap<String, String>();
        sCalendarCacheProjectionMap.put(CalendarCache.COLUMN_NAME_KEY, "key");
        sCalendarCacheProjectionMap.put(CalendarCache.COLUMN_NAME_VALUE, "value");
    }

    /**
     * Make sure that there are no entries for accounts that no longer
     * exist. We are overriding this since we need to delete from the
     * Calendars table, which is not syncable, which has triggers that
     * will delete from the Events and  tables, which are
     * syncable.  TODO: update comment, make sure deletes don't get synced.
     */
    public void onAccountsUpdated(Account[] accounts) {
        if (mDb == null) {
            mDb = mDbHelper.getWritableDatabase();
        }
        if (mDb == null) {
            return;
        }

        HashMap<Account, Boolean> accountHasCalendar = new HashMap<Account, Boolean>();
        HashSet<Account> validAccounts = new HashSet<Account>();
        for (Account account : accounts) {
            validAccounts.add(new Account(account.name, account.type));
            accountHasCalendar.put(account, false);
        }
        ArrayList<Account> accountsToDelete = new ArrayList<Account>();

        mDb.beginTransaction();
        try {

            for (String table : new String[]{Tables.CALENDARS}) {
                // Find all the accounts the calendar DB knows about, mark the ones that aren't
                // in the valid set for deletion.
                Cursor c = mDb.rawQuery("SELECT DISTINCT " +
                                            Calendar.SyncColumns._SYNC_ACCOUNT +
                                            "," +
                                            Calendar.SyncColumns._SYNC_ACCOUNT_TYPE +
                                        " FROM " + table, null);
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
                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "removing data for removed account " + account);
                }
                String[] params = new String[]{account.name, account.type};
                mDb.execSQL(SQL_DELETE_FROM_CALENDARS, params);
            }
            mDbHelper.getSyncState().onAccountsChanged(mDb, accounts);
            mDb.setTransactionSuccessful();
        } finally {
            mDb.endTransaction();
        }

        // make sure the widget reflects the account changes
        sendUpdateNotification(false);
    }

    /**
     * Inserts an argument at the beginning of the selection arg list.
     *
     * The {@link android.database.sqlite.SQLiteQueryBuilder}'s where clause is
     * prepended to the user's where clause (combined with 'AND') to generate
     * the final where close, so arguments associated with the QueryBuilder are
     * prepended before any user selection args to keep them in the right order.
     */
    private String[] insertSelectionArg(String[] selectionArgs, String arg) {
        if (selectionArgs == null) {
            return new String[] {arg};
        } else {
            int newLength = selectionArgs.length + 1;
            String[] newSelectionArgs = new String[newLength];
            newSelectionArgs[0] = arg;
            System.arraycopy(selectionArgs, 0, newSelectionArgs, 1, selectionArgs.length);
            return newSelectionArgs;
        }
    }
}
