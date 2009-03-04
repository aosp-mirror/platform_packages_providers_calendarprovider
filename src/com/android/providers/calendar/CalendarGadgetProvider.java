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
import android.content.ComponentName;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.res.Resources;
import android.database.Cursor;
import android.gadget.GadgetManager;
import android.gadget.GadgetProvider;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Canvas;
import android.graphics.Paint;
import android.graphics.PorterDuff;
import android.net.Uri;
import android.provider.Calendar.Attendees;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Instances;
import android.text.format.DateFormat;
import android.text.format.DateUtils;
import android.text.format.Time;
import android.util.Log;
import android.view.View;
import android.widget.RemoteViews;

import java.util.TimeZone;

/**
 * Simple gadget to show next upcoming calendar event.
 */
public class CalendarGadgetProvider extends GadgetProvider {
    static final String TAG = "CalendarGadgetProvider";
    static final boolean LOGD = true;
    
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

    static final String ACTION_CALENDAR_GADGET_UPDATE = "com.android.calendar.GADGET_UPDATE";
    
    static final String PACKAGE_DETAIL = "com.android.calendar";
    static final String CLASS_DETAIL = "com.android.calendar.AgendaActivity";
    
    static final ComponentName THIS_GADGET =
        new ComponentName("com.android.providers.calendar",
                "com.android.providers.calendar.CalendarGadgetProvider");
    
    private static CalendarGadgetProvider sInstance;
    
    static synchronized CalendarGadgetProvider getInstance() {
        if (sInstance == null) {
            sInstance = new CalendarGadgetProvider();
        }
        return sInstance;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(Context context, Intent intent) {
        // Handle calendar-specific updates ourselves because they might be
        // coming in without extras, which GadgetProvider then blocks.
        final String action = intent.getAction();
        if (ACTION_CALENDAR_GADGET_UPDATE.equals(action)) {
            performUpdate(context, null, -1);
        } else {
            super.onReceive(context, intent);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void onEnabled(Context context) {
        // Enable updates for timezone and date changes
        PackageManager pm = context.getPackageManager();
        pm.setComponentEnabledSetting(
                new ComponentName(context, TimeChangeReceiver.class),
                PackageManager.COMPONENT_ENABLED_STATE_ENABLED,
                PackageManager.DONT_KILL_APP);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onDisabled(Context context) {
        // Unsubscribe from all AlarmManager updates
        AlarmManager am = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        PendingIntent pendingUpdate = getUpdateIntent(context);
        am.cancel(pendingUpdate);

        // Disable updates for timezone and date changes
        PackageManager pm = context.getPackageManager();
        pm.setComponentEnabledSetting(
                new ComponentName(context, TimeChangeReceiver.class),
                PackageManager.COMPONENT_ENABLED_STATE_DISABLED,
                PackageManager.DONT_KILL_APP);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onUpdate(Context context, GadgetManager gadgetManager, int[] gadgetIds) {
        performUpdate(context, gadgetIds, -1);
    }
    
    /**
     * Check against {@link GadgetManager} if there are any instances of this gadget.
     */
    private boolean hasInstances(Context context) {
        GadgetManager gadgetManager = GadgetManager.getInstance(context);
        int[] gadgetIds = gadgetManager.getGadgetIds(THIS_GADGET);
        return (gadgetIds.length > 0);
    }

    /**
     * The {@link CalendarProvider} has been updated, which means we should push
     * updates to any gadgets, if they exist.
     * 
     * @param changedEventId Specific event known to be changed, otherwise -1.
     *            If present, we use it to decide if an update is necessary.
     */
    void providerUpdated(Context context, long changedEventId) {
        if (hasInstances(context)) {
            performUpdate(context, null, changedEventId);
        }
    }
    
    /**
     * {@link TimeChangeReceiver} has triggered that the time changed.
     */
    void timeUpdated(Context context) {
        if (hasInstances(context)) {
            performUpdate(context, null, -1);
        }
    }

    /**
     * Process and push out an update for the given gadgetIds.
     * 
     * @param context Context to use when creating gadget.
     * @param gadgetIds List of specific gadgetIds to update, or null for all.
     * @param changedEventId Specific event known to be changed, otherwise -1.
     *            If present, we use it to decide if an update is necessary.
     */
    private void performUpdate(Context context, int[] gadgetIds, long changedEventId) {
        ContentResolver resolver = context.getContentResolver();
        
        Cursor cursor = null;
        RemoteViews views = null;
        long triggerTime = -1;

        try {
            cursor = getUpcomingInstancesCursor(resolver, SEARCH_DURATION);
            if (cursor != null) {
                MarkedEvents events = buildMarkedEvents(cursor, changedEventId);
                
                boolean shouldUpdate = true;
                if (changedEventId != -1) {
                    shouldUpdate = events.watchFound;
                }
                
                if (events.primaryCount == 0) {
                    views = getGadgetNoEvents(context);
                } else if (shouldUpdate) {
                    views = getGadgetUpdate(context, cursor, events);
                    triggerTime = calculateUpdateTime(context, cursor, events);
                }
            } else {
                views = getGadgetNoEvents(context);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        
        // Bail out early if no update built
        if (views == null) {
            if (LOGD) Log.d(TAG, "Didn't build update, possibly because changedEventId=" + changedEventId);
            return;
        }
        
        GadgetManager gm = GadgetManager.getInstance(context);
        if (gadgetIds != null) {
            gm.updateGadget(gadgetIds, views);
        } else {
            ComponentName thisGadget = new ComponentName(context, CalendarGadgetProvider.class);
            gm.updateGadget(thisGadget, views);
        }

        // Schedule an alarm to wake ourselves up for the next update.  We also cancel
        // all existing wake-ups because PendingIntents don't match against extras.
        
        // If no next-update calculated, or bad trigger time in past, schedule
        // update about six hours from now.
        long now = System.currentTimeMillis();
        if (triggerTime == -1 || triggerTime < now) {
            if (LOGD) Log.w(TAG, "Encountered bad trigger time " + formatDebugTime(triggerTime));
            triggerTime = now + UPDATE_NO_EVENTS;
        }
        
        AlarmManager am = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
        PendingIntent pendingUpdate = getUpdateIntent(context);
        
        am.cancel(pendingUpdate);
        am.set(AlarmManager.RTC, triggerTime, pendingUpdate);

        if (LOGD) Log.d(TAG, "Scheduled next update at " + formatDebugTime(triggerTime));
    }
    
    /**
     * Build the {@link PendingIntent} used to trigger an update of all calendar
     * gadgets. Uses {@link ACTION_CALENDAR_GADGET_UPDATE} to directly target
     * all gadgets instead of using {@link GadgetManager#EXTRA_GADGET_IDS}.
     */
    private PendingIntent getUpdateIntent(Context context) {
        Intent updateIntent = new Intent(ACTION_CALENDAR_GADGET_UPDATE);
        updateIntent.setComponent(new ComponentName(context, CalendarGadgetProvider.class));
        return PendingIntent.getBroadcast(context, 0 /* no requestCode */,
                updateIntent, 0 /* no flags */);
    }
    
    /**
     * Format given time for debugging output.
     */
    private String formatDebugTime(long unixTime) {
        Time time = new Time();
        time.set(unixTime);
        
        long delta = unixTime - System.currentTimeMillis();
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
     * Figure out the next time we should push gadget updates. This is based on
     * the time calculated by {@link #getEventFlip(Cursor, int)}.
     */
    private long calculateUpdateTime(Context context, Cursor cursor, MarkedEvents events) {
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
        }
        return result;
    }
    
    /**
     * Calculate flipping point for the given event; when we should hide this
     * event and show the next one. This is usually half-way through the event.
     * 
     * @param start Event start time in local timezone.
     * @param end Event end time in local timezone.
     */
    private long getEventFlip(Cursor cursor, long start, long end, boolean allDay) {
        if (allDay) {
            return start;
        } else {
            return (start + end) / 2;
        }
    }
    
    /**
     * Build a set of {@link RemoteViews} that describes how to update any
     * gadget for a specific event instance.
     * 
     * @param cursor Valid cursor on {@link Instances#CONTENT_URI}
     * @param events {@link MarkedEvents} parsed from the cursor
     */
    private RemoteViews getGadgetUpdate(Context context, Cursor cursor, MarkedEvents events) {
        Resources res = context.getResources();
        ContentResolver resolver = context.getContentResolver();
        
        RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.gadget_item);
        
        // Clicking on gadget launches the agenda view in Calendar
        Intent agendaIntent = new Intent();
        agendaIntent.setComponent(new ComponentName(PACKAGE_DETAIL, CLASS_DETAIL));
        PendingIntent pendingIntent = PendingIntent.getActivity(context, 0 /* no requestCode */,
                agendaIntent, 0 /* no flags */);
        
        views.setOnClickPendingIntent(R.id.gadget, pendingIntent);
        
        Time time = new Time();
        time.setToNow();
        int yearDay = time.yearDay;
        int dateNumber = time.monthDay;

        // Set calendar icon with actual date
        views.setTextViewText(R.id.icon, Integer.toString(dateNumber));
        views.setViewVisibility(R.id.icon, View.VISIBLE);
        views.setViewVisibility(R.id.no_events, View.GONE);

        // Fill primary event details
        if (events.primaryRow != -1) {
            views.setViewVisibility(R.id.primary_card, View.VISIBLE);
            cursor.moveToPosition(events.primaryRow);
            
            // Color stripe
            int colorFilter = cursor.getInt(INDEX_COLOR);
            views.setDrawableParameters(R.id.when, true, -1, colorFilter,
                    PorterDuff.Mode.SRC_IN, -1);
            views.setTextColor(R.id.title, colorFilter);
            views.setTextColor(R.id.where, colorFilter);
            views.setDrawableParameters(R.id.divider, true, -1, colorFilter,
                    PorterDuff.Mode.SRC_IN, -1);
            views.setTextColor(R.id.title2, colorFilter);

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

            // What
            String titleString = cursor.getString(INDEX_TITLE);
            if (titleString == null || titleString.length() == 0) {
                titleString = context.getString(R.string.no_title_label);
            }
            views.setTextViewText(R.id.title, titleString);
            
            // Where
            String whereString = cursor.getString(INDEX_EVENT_LOCATION);
            if (whereString != null && whereString.length() > 0) {
                views.setViewVisibility(R.id.where, View.VISIBLE);
                views.setViewVisibility(R.id.stub_where, View.INVISIBLE);
                views.setTextViewText(R.id.where, whereString);
            } else {
                views.setViewVisibility(R.id.where, View.GONE);
                views.setViewVisibility(R.id.stub_where, View.GONE);
            }
        }
        
        // Fill other primary events, if present
        if (events.primaryConflictRow != -1) {
            views.setViewVisibility(R.id.divider, View.VISIBLE);
            views.setViewVisibility(R.id.title2, View.VISIBLE);

            if (events.primaryCount > 2) {
                // If more than two primary conflicts, format multiple message
                int count = events.primaryCount - 1;
                String titleString = String.format(res.getQuantityString(
                        R.plurals.gadget_more_events, count), count);
                views.setTextViewText(R.id.title2, titleString);
            } else {
                cursor.moveToPosition(events.primaryConflictRow);

                // What
                String titleString = cursor.getString(INDEX_TITLE);
                if (titleString == null || titleString.length() == 0) {
                    titleString = context.getString(R.string.no_title_label);
                }
                views.setTextViewText(R.id.title2, titleString);
            }
        } else {
            views.setViewVisibility(R.id.divider, View.GONE);
            views.setViewVisibility(R.id.title2, View.GONE);
        }
        
        // Fill secondary event
        if (events.secondaryRow != -1) {
            views.setViewVisibility(R.id.secondary_card, View.VISIBLE);
            views.setViewVisibility(R.id.secondary_when, View.VISIBLE);
            views.setViewVisibility(R.id.secondary_title, View.VISIBLE);
            
            cursor.moveToPosition(events.secondaryRow);
            
            // Color stripe
            int colorFilter = cursor.getInt(INDEX_COLOR);
            views.setDrawableParameters(R.id.secondary_when, true, -1, colorFilter,
                    PorterDuff.Mode.SRC_IN, -1);
            views.setTextColor(R.id.secondary_title, colorFilter);
            
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
            views.setTextViewText(R.id.secondary_when, whenString);
            
            if (events.secondaryCount > 1) {
                // If more than two secondary conflicts, format multiple message
                int count = events.secondaryCount;
                String titleString = String.format(res.getQuantityString(
                        R.plurals.gadget_more_events, count), count);
                views.setTextViewText(R.id.secondary_title, titleString);
            } else {
                // What
                String titleString = cursor.getString(INDEX_TITLE);
                if (titleString == null || titleString.length() == 0) {
                    titleString = context.getString(R.string.no_title_label);
                }
                views.setTextViewText(R.id.secondary_title, titleString);
            }
        } else {
            views.setViewVisibility(R.id.secondary_when, View.GONE);
            views.setViewVisibility(R.id.secondary_title, View.GONE);
        }
        
        return views;
    }
    
    /**
     * Build a set of {@link RemoteViews} that describes an error state.
     */
    private RemoteViews getGadgetNoEvents(Context context) {
        RemoteViews views = new RemoteViews(context.getPackageName(), R.layout.gadget_item);

        views.setViewVisibility(R.id.no_events, View.VISIBLE);
        
        views.setViewVisibility(R.id.primary_card, View.GONE);
        views.setViewVisibility(R.id.secondary_card, View.GONE);
        
        // Clicking on gadget launches the agenda view in Calendar
        Intent agendaIntent = new Intent();
        agendaIntent.setComponent(new ComponentName(PACKAGE_DETAIL, CLASS_DETAIL));
        PendingIntent pendingIntent = PendingIntent.getActivity(context, 0 /* no requestCode */,
                agendaIntent, 0 /* no flags */);
        
        views.setOnClickPendingIntent(R.id.gadget, pendingIntent);

        return views;
    }
    
    private static class MarkedEvents {
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
     * used when updating the gadget. This structure is also used to check if
     * updates are needed.
     * 
     * @param cursor Valid cursor across {@link Instances#CONTENT_URI}.
     * @param watchEventId Specific event to watch for, setting
     *            {@link MarkedEvents#watchFound} if found during marking.
     */
    private MarkedEvents buildMarkedEvents(Cursor cursor, long watchEventId) {
        MarkedEvents events = new MarkedEvents();
        long now = System.currentTimeMillis();
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
            if (LOGD) Log.d(TAG, "Calculated flip time " + formatDebugTime(eventFlip));
            if (eventFlip < now) {
                continue;
            }
            
            // Mark if we've encountered the watched event
            if (eventId == watchEventId) {
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
     * @param searchDuration Distance into the future to look for event
     *            instances, in milliseconds.
     */
    private Cursor getUpcomingInstancesCursor(ContentResolver resolver, long searchDuration) {
        // Search for events from now until some time in the future
        long start = System.currentTimeMillis();
        long end = start + searchDuration;
        
        Uri uri = Uri.withAppendedPath(Instances.CONTENT_URI,
                String.format("%d/%d", start, end));

        String selection = String.format("%s=1 AND %s!=%d",
                Calendars.SELECTED, Instances.SELF_ATTENDEE_STATUS,
                Attendees.ATTENDEE_STATUS_DECLINED);
        
        return resolver.query(uri, EVENT_PROJECTION, selection, null,
                EVENT_SORT_ORDER);
    }
    
}
