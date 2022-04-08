/*
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

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.os.PowerManager;
import android.os.SystemProperties;
import android.util.Log;

import java.util.concurrent.TimeUnit;

/**
 * This IntentReceiver executes when the boot completes and ensures that
 * the Calendar provider has started and then initializes the alarm
 * scheduler for the Calendar provider.  This needs to be done after
 * the boot completes because the alarm manager may not have been started
 * yet.
 */
public class CalendarReceiver extends BroadcastReceiver {
    private static final String TAG = CalendarProvider2.TAG;

    private static final long NEXT_EVENT_CHECK_INTERVAL =
            SystemProperties.getLong("debug.calendar.check_interval", TimeUnit.HOURS.toMillis(6));
    private static final int NEXT_EVENT_CHECK_PENDING_CODE = 100;

    private PowerManager.WakeLock mWakeLock;

    @Override
    public void onReceive(Context context, Intent intent) {
        final String action = intent.getAction();

        if (!Intent.ACTION_BOOT_COMPLETED.equals(action)) {
            Log.w(TAG, "Unexpected broadcast: " + action);
            return;
        }
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Log.d(TAG, "BOOT_COMPLETED");
        }

        if (mWakeLock == null) {
            PowerManager pm = (PowerManager) context.getSystemService(Context.POWER_SERVICE);
            mWakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "CalendarReceiver_Provider");
            mWakeLock.setReferenceCounted(true);
        }
        mWakeLock.acquire();

        final ContentResolver cr = context.getContentResolver();
        final PendingResult result = goAsync();

        new Thread(() -> {
            setCalendarCheckAlarm(context);
            removeScheduledAlarms(cr);
            result.finish();
            mWakeLock.release();
        }).start();
    }

    /*
     * Remove alarms from the CalendarAlerts table that have been marked
     * as "scheduled" but not fired yet.  We do this because the
     * AlarmManagerService loses all information about alarms when the
     * power turns off but we store the information in a database table
     * that persists across reboots. See the documentation for
     * scheduleNextAlarmLocked() for more information.
     *
     * We don't expect this to be called more than once.  If it were, we would have to
     * worry about serializing the use of the service.
     */
    private void removeScheduledAlarms(ContentResolver resolver) {
        resolver.update(CalendarAlarmManager.SCHEDULE_ALARM_REMOVE_URI, null /* values */,
                null /* where */, null /* selectionArgs */);
    }

    private static void setCalendarCheckAlarm(Context context) {
        final PendingIntent checkIntent = PendingIntent.getBroadcast(context,
                NEXT_EVENT_CHECK_PENDING_CODE,
                CalendarAlarmManager.getCheckNextAlarmIntentForBroadcast(context),
                PendingIntent.FLAG_UPDATE_CURRENT);

        final AlarmManager am = context.getSystemService(AlarmManager.class);

        am.setRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP,
                NEXT_EVENT_CHECK_INTERVAL, NEXT_EVENT_CHECK_INTERVAL, checkIntent);
    }
}
