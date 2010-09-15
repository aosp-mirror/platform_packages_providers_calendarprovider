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
import android.appwidget.AppWidgetManager;
import android.appwidget.AppWidgetProvider;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.PowerManager;
import android.os.PowerManager.WakeLock;
import android.text.format.DateUtils;
import android.util.Log;

/**
 * Simple widget to show next upcoming calendar event.
 */
public class CalendarAppWidgetProvider extends AppWidgetProvider {
    static final String TAG = "CalendarAppWidgetProvider";
    static final boolean LOGD = false;

    static final String ACTION_CALENDAR_APPWIDGET_UPDATE =
            "com.android.providers.calendar.APPWIDGET_UPDATE";

    /**
     * Threshold to check against when building widget updates. If system clock
     * has changed less than this amount, we consider ignoring the request.
     */
    static final long UPDATE_THRESHOLD = DateUtils.MINUTE_IN_MILLIS;

    /**
     * Maximum time to hold {@link WakeLock} when performing widget updates.
     */
    static final long WAKE_LOCK_TIMEOUT = DateUtils.MINUTE_IN_MILLIS;

    static final String PACKAGE_THIS_APPWIDGET =
        "com.android.providers.calendar";
    static final String CLASS_THIS_APPWIDGET =
        "com.android.providers.calendar.CalendarAppWidgetProvider";

    private static CalendarAppWidgetProvider sInstance;

    static synchronized CalendarAppWidgetProvider getInstance() {
        if (sInstance == null) {
            sInstance = new CalendarAppWidgetProvider();
        }
        return sInstance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(Context context, Intent intent) {
        // Handle calendar-specific updates ourselves because they might be
        // coming in without extras, which AppWidgetProvider then blocks.
        final String action = intent.getAction();
        if (ACTION_CALENDAR_APPWIDGET_UPDATE.equals(action)) {
            performUpdate(context, null /* all widgets */,
                    null /* no eventIds */, false /* don't ignore */);
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
    public void onUpdate(Context context, AppWidgetManager appWidgetManager, int[] appWidgetIds) {
        performUpdate(context, appWidgetIds, null /* no eventIds */, false /* force */);
    }

    /**
     * Check against {@link AppWidgetManager} if there are any instances of this widget.
     */
    private boolean hasInstances(Context context) {
        AppWidgetManager appWidgetManager = AppWidgetManager.getInstance(context);
        ComponentName thisAppWidget = getComponentName(context);
        int[] appWidgetIds = appWidgetManager.getAppWidgetIds(thisAppWidget);
        return (appWidgetIds.length > 0);
    }

    /**
     * Build {@link ComponentName} describing this specific
     * {@link AppWidgetProvider}
     */
    static ComponentName getComponentName(Context context) {
        return new ComponentName(PACKAGE_THIS_APPWIDGET, CLASS_THIS_APPWIDGET);
    }

    /**
     * The {@link CalendarProvider} has been updated, which means we should push
     * updates to any widgets, if they exist.
     *
     * @param context Context to use when creating widget.
     * @param changedEventId Specific event known to be changed, otherwise -1.
     *            If present, we use it to decide if an update is necessary.
     */
    void providerUpdated(Context context, long changedEventId) {
        if (hasInstances(context)) {
            // Only pass along changedEventId if not -1
            long[] changedEventIds = null;
            if (changedEventId != -1) {
                changedEventIds = new long[] { changedEventId };
            }

            performUpdate(context, null /* all widgets */, changedEventIds, false /* force */);
        }
    }

    /**
     * {@link TimeChangeReceiver} has triggered that the time changed.
     *
     * @param context Context to use when creating widget.
     * @param considerIgnore If true, compare
     *            {@link AppWidgetShared#sLastRequest} against
     *            {@link #UPDATE_THRESHOLD} to consider ignoring this update
     *            request.
     */
    void timeUpdated(Context context, boolean considerIgnore) {
        if (hasInstances(context)) {
            performUpdate(context, null /* all widgets */, null /* no events */, considerIgnore);
        }
    }

    /**
     * Process and push out an update for the given appWidgetIds. This call
     * actually fires an intent to start {@link CalendarAppWidgetService} as a
     * background service which handles the actual update, to prevent ANR'ing
     * during database queries.
     * <p>
     * This call will acquire a single {@link WakeLock} and set a flag that an
     * update has been requested.
     *
     * @param context Context to use when acquiring {@link WakeLock} and
     *            starting {@link CalendarAppWidgetService}.
     * @param appWidgetIds List of specific appWidgetIds to update, or null for
     *            all.
     * @param changedEventIds Specific events known to be changed. If present,
     *            we use it to decide if an update is necessary.
     * @param considerIgnore If true, compare
     *            {@link AppWidgetShared#sLastRequest} against
     *            {@link #UPDATE_THRESHOLD} to consider ignoring this update
     *            request.
     */
    private void performUpdate(Context context, int[] appWidgetIds,
            long[] changedEventIds, boolean considerIgnore) {
        synchronized (AppWidgetShared.sLock) {
            // Consider ignoring this update request if inside threshold. This
            // check is inside the lock because we depend on this "now" time.
            long now = System.currentTimeMillis();
            if (considerIgnore && AppWidgetShared.sLastRequest != -1) {
                long delta = Math.abs(now - AppWidgetShared.sLastRequest);
                if (delta < UPDATE_THRESHOLD) {
                    if (LOGD) Log.d(TAG, "Ignoring update request because delta=" + delta);
                    return;
                }
            }

            // We need to update, so make sure we have a valid, held wakelock
            if (AppWidgetShared.sWakeLock == null ||
                    !AppWidgetShared.sWakeLock.isHeld()) {
                if (LOGD) Log.d(TAG, "no held wakelock found, so acquiring new one");
                PowerManager powerManager = (PowerManager)
                        context.getSystemService(Context.POWER_SERVICE);
                AppWidgetShared.sWakeLock =
                        powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, TAG);
                AppWidgetShared.sWakeLock.setReferenceCounted(false);
                AppWidgetShared.sWakeLock.acquire(WAKE_LOCK_TIMEOUT);
            }

            if (LOGD) Log.d(TAG, "setting request now=" + now);
            AppWidgetShared.sLastRequest = now;
            AppWidgetShared.sUpdateRequested = true;

            // Apply filters that would limit the scope of this update, or clear
            // any pending filters if all requested.
            AppWidgetShared.mergeAppWidgetIdsLocked(appWidgetIds);
            AppWidgetShared.mergeChangedEventIdsLocked(changedEventIds);

            // Launch over to service so it can perform update
            final Intent updateIntent = new Intent(context, CalendarAppWidgetService.class);
            context.startService(updateIntent);
        }
    }

    /**
     * Build the {@link PendingIntent} used to trigger an update of all calendar
     * widgets. Uses {@link #ACTION_CALENDAR_APPWIDGET_UPDATE} to directly target
     * all widgets instead of using {@link AppWidgetManager#EXTRA_APPWIDGET_IDS}.
     *
     * @param context Context to use when building broadcast.
     */
    static PendingIntent getUpdateIntent(Context context) {
        Intent updateIntent = new Intent(ACTION_CALENDAR_APPWIDGET_UPDATE);
        updateIntent.setComponent(new ComponentName(context, CalendarAppWidgetProvider.class));
        return PendingIntent.getBroadcast(context, 0 /* no requestCode */,
                updateIntent, 0 /* no flags */);
    }
}
