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

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.Service;
import android.appwidget.AppWidgetManager;
import android.content.ComponentName;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.content.res.Resources;
import android.database.Cursor;
import android.net.Uri;
import android.os.IBinder;
import android.provider.Calendar;
import android.provider.Calendar.Attendees;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Instances;
import android.text.format.DateFormat;
import android.text.format.DateUtils;
import android.text.format.Time;
import android.util.Log;
import android.view.View;
import android.widget.RemoteViews;

import java.util.Set;
import java.util.TimeZone;


public class CalendarAppWidgetService extends Service implements Runnable {
    static final String TAG = "CalendarAppWidgetService";
    static final boolean LOGD = false;

    static final String EVENT_SORT_ORDER = "startDay ASC, allDay DESC, begin ASC";

    static final String[] EVENT_PROJECTION = new String[] {
        Instances.ALL_DAY,
        Instances.BEGIN,
        Instances.END,
        Instances.COLOR,
        Instances.TITLE,
        Instances.RRULE,
        Instances.HAS_ALARM,
        Instances.EVENT_LOCATION,
        Instances.CALENDAR_ID,
        Instances.EVENT_ID,
    };

    static final int INDEX_ALL_DAY = 0;
    static final int INDEX_BEGIN = 1;
    static final int INDEX_END = 2;
    static final int INDEX_COLOR = 3;
    static final int INDEX_TITLE = 4;
    static final int INDEX_RRULE = 5;
    static final int INDEX_HAS_ALARM = 6;
    static final int INDEX_EVENT_LOCATION = 7;
    static final int INDEX_CALENDAR_ID = 8;
    static final int INDEX_EVENT_ID = 9;

    static final long SEARCH_DURATION = DateUtils.WEEK_IN_MILLIS;

    static final long UPDATE_NO_EVENTS = DateUtils.HOUR_IN_MILLIS * 6;

    static final String ACTION_PACKAGE = "com.google.android.calendar";
    static final String ACTION_CLASS = "com.android.calendar.LaunchActivity";
    static final String KEY_DETAIL_VIEW = "DETAIL_VIEW";

    @Override
    public void onStart(Intent intent, int startId) {
        super.onStart(intent, startId);

        // Only start processing thread if not already running
        synchronized (AppWidgetShared.sLock) {
            if (!AppWidgetShared.sUpdateRunning) {
                if (LOGD) Log.d(TAG, "no thread running, so starting new one");
                AppWidgetShared.sUpdateRunning = true;
                new Thread(this).start();
            }
        }
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    /**
     * Thread loop to handle
     */
    public void run() {
        while (true) {
            long now = -1;
            int[] appWidgetIds;
            Set<Long> changedEventIds;

            synchronized (AppWidgetShared.sLock) {
                // Bail out if no remaining updates
                if (!AppWidgetShared.sUpdateRequested) {
                    // Clear current shared state, release wakelock, and stop service
                    if (LOGD) Log.d(TAG, "no requested update or expired wakelock, bailing");
                    AppWidgetShared.clearLocked();
                    stopSelf();
                    return;
                }

                // Clear requested flag and collect latest parameters
                AppWidgetShared.sUpdateRequested = false;

                now = AppWidgetShared.sLastRequest;
                appWidgetIds = AppWidgetShared.collectAppWidgetIdsLocked();
                changedEventIds = AppWidgetShared.collectChangedEventIdsLocked();
            }

            // Process this update
            if (LOGD) Log.d(TAG, "processing requested update now=" + now);
            performUpdate(this, appWidgetIds, changedEventIds, now);
        }
    }

    /**
     * Process and push out an update for the given appWidgetIds.
     *
     * @param context Context to use when updating widget.
     * @param appWidgetIds List of appWidgetIds to update, or null for all.
     * @param changedEventIds Specific events known to be changed, otherwise
     *            null. If present, we use to decide if an update is necessary.
     * @param now System clock time to use during this update.
     */
    private void performUpdate(Context context, int[] appWidgetIds,
            Set<Long> changedEventIds, long now) {
        ContentResolver resolver = context.getContentResolver();

        Cursor cursor = null;
        RemoteViews views = null;
        long triggerTime = -1;

        try {
            cursor = getUpcomingInstancesCursor(resolver, SEARCH_DURATION, now);
            if (cursor != null) {
                MarkedEvents events = buildMarkedEvents(cursor, changedEventIds, now);

                boolean shouldUpdate = true;
                if (changedEventIds.size() > 0) {
                    shouldUpdate = events.watchFound;
                }

                if (events.primaryCount == 0) {
                    views = getAppWidgetNoEvents(context);
                } else if (shouldUpdate) {
                    views = getAppWidgetUpdate(context, cursor, events);
                    triggerTime = calculateUpdateTime(cursor, events);
                }
            } else {
                views = getAppWidgetNoEvents(context);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }

        // Bail out early if no update built
        if (views == null) {
            if (LOGD) Log.d(TAG, "Didn't build update, possibly because changedEventIds=" +
                    changedEventIds.toString());
            return;
        }

        AppWidgetManager gm = AppWidgetManager.getInstance(context);
        if (appWidgetIds != null && appWidgetIds.length > 0) {
            gm.updateAppWidget(appWidgetIds, views);
        } else {
            ComponentName thisWidget = CalendarAppWidgetProvider.getComponentName(context);
            gm.updateAppWidget(thisWidget, views);
        }

        // Schedule an alarm to wake ourselves up for the next update.  We also cancel
        // all existing wake-ups because PendingIntents don't match against extras.

        // If no next-update calculated, or bad trigger time in past, schedule
        // update about six hours from now.
        if (triggerTime == -1 || triggerTime < now) {
            if (LOGD) Log.w(TAG, "Encountered bad trigger time " + formatDebugTime(triggerTime, now));
            triggerTime = now + UPDATE_NO_EVENTS;
        }

        AlarmManager am = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        PendingIntent pendingUpdate = CalendarAppWidgetProvider.getUpdateIntent(context);

        am.cancel(pendingUpdate);
        am.set(AlarmManager.RTC, triggerTime, pendingUpdate);

        if (LOGD) Log.d(TAG, "Scheduled next update at " + formatDebugTime(triggerTime, now));
    }

    /**
     * Format given time for debugging output.
     *
     * @param unixTime Target time to report.
     * @param now Current system time from {@link System#currentTimeMillis()}
     *            for calculating time difference.
     */
    private String formatDebugTime(long unixTime, long now) {
        Time time = new Time();
        time.set(unixTime);

        long delta = unixTime - now;
        if (delta > DateUtils.MINUTE_IN_MILLIS) {
            delta /= DateUtils.MINUTE_IN_MILLIS;
            return String.format("[%d] %s (%+d mins)", unixTime, time.format("%H:%M:%S"), delta);
        } else {
            delta /= DateUtils.SECOND_IN_MILLIS;
            return String.format("[%d] %s (%+d secs)", unixTime, time.format("%H:%M:%S"), delta);
        }
    }

    /**
     * Convert given UTC time into current local time.
     *
     * @param recycle Time object to recycle, otherwise null.
     * @param utcTime Time to convert, in UTC.
     */
    private long convertUtcToLocal(Time recycle, long utcTime) {
        if (recycle == null) {
            recycle = new Time();
        }
        recycle.timezone = Time.TIMEZONE_UTC;
        recycle.set(utcTime);
        recycle.timezone = TimeZone.getDefault().getID();
        return recycle.normalize(true);
    }

    /**
     * Figure out the next time we should push widget updates, usually the time
     * calculated by {@link #getEventFlip(Cursor, long, long, boolean)}.
     *
     * @param cursor Valid cursor on {@link Instances#CONTENT_URI}
     * @param events {@link MarkedEvents} parsed from the cursor
     */
    private long calculateUpdateTime(Cursor cursor, MarkedEvents events) {
        long result = -1;
        if (events.primaryRow != -1) {
            cursor.moveToPosition(events.primaryRow);
            long start = cursor.getLong(INDEX_BEGIN);
            long end = cursor.getLong(INDEX_END);
            boolean allDay = cursor.getInt(INDEX_ALL_DAY) != 0;

            // Adjust all-day times into local timezone
            if (allDay) {
                final Time recycle = new Time();
                start = convertUtcToLocal(recycle, start);
                end = convertUtcToLocal(recycle, end);
            }

            result = getEventFlip(cursor, start, end, allDay);

            // Update at midnight
            long midnight = getNextMidnightTimeMillis();
            result = Math.min(midnight, result);
        }
        return result;
    }

    private long getNextMidnightTimeMillis() {
        Time time = new Time();
        time.setToNow();
        time.monthDay++;
        time.hour = 0;
        time.minute = 0;
        time.second = 0;
        long midnight = time.normalize(true);
        return midnight;
    }

    /**
     * Calculate flipping point for the given event; when we should hide this
     * event and show the next one. This is usually half-way through the event.
     * <p>
     * Events with duration longer than one day as treated as all-day events
     * when computing the flipping point.
     *
     * @param start Event start time in local timezone.
     * @param end Event end time in local timezone.
     */
    private long getEventFlip(Cursor cursor, long start, long end, boolean allDay) {
        long duration = end - start;
        if (allDay || duration > DateUtils.DAY_IN_MILLIS) {
            return start;
        } else {
            return (start + end) / 2;
        }
    }

    /**
     * Set visibility of various widget components if there are events, or if no
     * events were found.
     *
     * @param views Set of {@link RemoteViews} to apply visibility.
     * @param noEvents True if no events found, otherwise false.
     */
    private void setNoEventsVisible(RemoteViews views, boolean noEvents) {
        views.setViewVisibility(R.id.no_events, noEvents ? View.VISIBLE : View.GONE);

        int otherViews = noEvents ? View.GONE : View.VISIBLE;
        views.setViewVisibility(R.id.day_of_month, otherViews);
        views.setViewVisibility(R.id.day_of_week, otherViews);
        views.setViewVisibility(R.id.divider, otherViews);
        views.setViewVisibility(R.id.when, otherViews);
        views.setViewVisibility(R.id.title, otherViews);

        // Don't force-show views that are handled elsewhere
        if (noEvents) {
            views.setViewVisibility(R.id.conflict, otherViews);
            views.setViewVisibility(R.id.where, otherViews);
        }
    }

    /**
     * Build a set of {@link RemoteViews} that describes how to update any
     * widget for a specific event instance.
     *
     * @param cursor Valid cursor on {@link Instances#CONTENT_URI}
     * @param events {@link MarkedEvents} parsed from the cursor
     */
    private RemoteViews getAppWidgetUpdate(Context context, Cursor cursor, MarkedEvents events) {
        Resources res = context.getResources();
        ContentResolver resolver = context.getContentResolver();

        RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.agenda_appwidget);
        setNoEventsVisible(views, false);

        Time time = new Time();
        time.setToNow();
        int yearDay = time.yearDay;
        int dateNumber = time.monthDay;

        // Calendar header
        String dayOfWeek = DateUtils.getDayOfWeekString(time.weekDay + 1,
                DateUtils.LENGTH_MEDIUM).toUpperCase();

        views.setTextViewText(R.id.day_of_week, dayOfWeek);
        views.setTextViewText(R.id.day_of_month, Integer.toString(time.monthDay));

        // Fill primary event details
        cursor.moveToPosition(events.primaryRow);

        // Color stripe
        /*
        int colorFilter = cursor.getInt(INDEX_COLOR);
        views.setTextColor(R.id.when, colorFilter);
        views.setTextColor(R.id.title, colorFilter);
        views.setTextColor(R.id.where, colorFilter);
        */

        // When
        long start = cursor.getLong(INDEX_BEGIN);
        boolean allDay = cursor.getInt(INDEX_ALL_DAY) != 0;

        int flags;
        String whenString;
        if (allDay) {
            flags = DateUtils.FORMAT_ABBREV_ALL | DateUtils.FORMAT_UTC
                    | DateUtils.FORMAT_SHOW_DATE;
        } else {
            flags = DateUtils.FORMAT_ABBREV_ALL | DateUtils.FORMAT_SHOW_TIME;

            // Show date if different from today
            time.set(start);
            if (yearDay != time.yearDay) {
                flags = flags | DateUtils.FORMAT_SHOW_DATE;
            }
        }
        if (DateFormat.is24HourFormat(context)) {
            flags |= DateUtils.FORMAT_24HOUR;
        }
        whenString = DateUtils.formatDateRange(context, start, start, flags);
        views.setTextViewText(R.id.when, whenString);

        // Clicking on the widget launches Calendar
        PendingIntent pendingIntent = getLaunchPendingIntent(context, start);
        views.setOnClickPendingIntent(R.id.agenda_appwidget, pendingIntent);

       // What
        String titleString = cursor.getString(INDEX_TITLE);
        if (titleString == null || titleString.length() == 0) {
            titleString = context.getString(R.string.no_title_label);
        }
        views.setTextViewText(R.id.title, titleString);

        // Conflicts
        int titleLines = 4;
        if (events.primaryCount > 1) {
            int count = events.primaryCount - 1;
            String conflictString = String.format(res.getQuantityString(
                    R.plurals.gadget_more_events, count), count);
            views.setTextViewText(R.id.conflict, conflictString);
            views.setViewVisibility(R.id.conflict, View.VISIBLE);
            titleLines -= 1;
        } else {
            views.setViewVisibility(R.id.conflict, View.GONE);
        }

        // Where
        String whereString = cursor.getString(INDEX_EVENT_LOCATION);
        if (whereString != null && whereString.length() > 0) {
            views.setViewVisibility(R.id.where, View.VISIBLE);
            views.setTextViewText(R.id.where, whereString);
            titleLines -= 1;
        } else {
            views.setViewVisibility(R.id.where, View.GONE);
        }

        // Trim title lines based on details shown. In landscape we're using
        // singleLine which means this value is ignored.
        views.setInt(R.id.title, "setMaxLines", titleLines);

        return views;
    }

    /**
     * Build a set of {@link RemoteViews} that describes an error state.
     */
    private RemoteViews getAppWidgetNoEvents(Context context) {
        RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.agenda_appwidget);
        setNoEventsVisible(views, true);

        // Clicking on widget launches the agenda view in Calendar
        PendingIntent pendingIntent = getLaunchPendingIntent(context, 0);
        views.setOnClickPendingIntent(R.id.agenda_appwidget, pendingIntent);

        return views;
    }

    /**
     * Build a {@link PendingIntent} to launch the Calendar app. This correctly
     * sets action, category, and flags so that we don't duplicate tasks when
     * Calendar was also launched from a normal desktop icon.
     * @param goToTime time that calendar should take the user to
     */
    private PendingIntent getLaunchPendingIntent(Context context, long goToTime) {
        Intent launchIntent = new Intent();
        String dataString = "content://com.android.calendar/time";
        launchIntent.setAction(Intent.ACTION_VIEW);
        launchIntent.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK |
                Intent.FLAG_ACTIVITY_RESET_TASK_IF_NEEDED |
                Intent.FLAG_ACTIVITY_CLEAR_TOP);
        if (goToTime != 0) {
            launchIntent.putExtra(KEY_DETAIL_VIEW, true);
            dataString += "/" + goToTime;
        }
        Uri data = Uri.parse(dataString);
        launchIntent.setData(data);
        return PendingIntent.getActivity(context, 0 /* no requestCode */,
                launchIntent, PendingIntent.FLAG_UPDATE_CURRENT);
    }

    private class MarkedEvents {
        long primaryTime = -1;
        int primaryRow = -1;
        int primaryConflictRow = -1;
        int primaryCount = 0;
        long secondaryTime = -1;
        int secondaryRow = -1;
        int secondaryCount = 0;
        boolean watchFound = false;
    }

    /**
     * Walk the given instances cursor and build a list of marked events to be
     * used when updating the widget. This structure is also used to check if
     * updates are needed.
     *
     * @param cursor Valid cursor across {@link Instances#CONTENT_URI}.
     * @param watchEventIds Specific events to watch for, setting
     *            {@link MarkedEvents#watchFound} if found during marking.
     * @param now Current system time to use for this update, possibly from
     *            {@link System#currentTimeMillis()}
     */
    private MarkedEvents buildMarkedEvents(Cursor cursor, Set<Long> watchEventIds, long now) {
        MarkedEvents events = new MarkedEvents();
        final Time recycle = new Time();

        cursor.moveToPosition(-1);
        while (cursor.moveToNext()) {
            int row = cursor.getPosition();
            long eventId = cursor.getLong(INDEX_EVENT_ID);
            long start = cursor.getLong(INDEX_BEGIN);
            long end = cursor.getLong(INDEX_END);
            boolean allDay = cursor.getInt(INDEX_ALL_DAY) != 0;

            // Adjust all-day times into local timezone
            if (allDay) {
                start = convertUtcToLocal(recycle, start);
                end = convertUtcToLocal(recycle, end);
            }

            // Skip events that have already passed their flip times
            long eventFlip = getEventFlip(cursor, start, end, allDay);
            if (LOGD) Log.d(TAG, "Calculated flip time " + formatDebugTime(eventFlip, now));
            if (eventFlip < now) {
                continue;
            }

            // Mark if we've encountered the watched event
            if (watchEventIds.contains(eventId)) {
                events.watchFound = true;
            }

            if (events.primaryRow == -1) {
                // Found first event
                events.primaryRow = row;
                events.primaryTime = start;
                events.primaryCount = 1;
            } else if (events.primaryTime == start) {
                // Found conflicting primary event
                if (events.primaryConflictRow == -1) {
                    events.primaryConflictRow = row;
                }
                events.primaryCount += 1;
            } else if (events.secondaryRow == -1) {
                // Found second event
                events.secondaryRow = row;
                events.secondaryTime = start;
                events.secondaryCount = 1;
            } else if (events.secondaryTime == start) {
                // Found conflicting secondary event
                events.secondaryCount += 1;
            } else {
                // Nothing interesting about this event, so bail out
                break;
            }
        }
        return events;
    }

    /**
     * Query across all calendars for upcoming event instances from now until
     * some time in the future.
     *
     * @param resolver {@link ContentResolver} to use when querying
     *            {@link Instances#CONTENT_URI}.
     * @param searchDuration Distance into the future to look for event
     *            instances, in milliseconds.
     * @param now Current system time to use for this update, possibly from
     *            {@link System#currentTimeMillis()}.
     */
    private Cursor getUpcomingInstancesCursor(ContentResolver resolver,
            long searchDuration, long now) {
        // Search for events from now until some time in the future
        long end = now + searchDuration;

        Uri uri = Uri.withAppendedPath(Instances.CONTENT_URI,
                String.format("%d/%d", now, end));

        String selection = String.format("%s=1 AND %s!=%d",
                Calendars.SELECTED, Instances.SELF_ATTENDEE_STATUS,
                Attendees.ATTENDEE_STATUS_DECLINED);

        return resolver.query(uri, EVENT_PROJECTION, selection, null,
                EVENT_SORT_ORDER);
    }
}
