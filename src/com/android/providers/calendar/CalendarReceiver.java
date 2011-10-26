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

import android.app.Service;
import android.content.BroadcastReceiver;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;

/**
 * This IntentReceiver executes when the boot completes and ensures that
 * the Calendar provider has started and then initializes the alarm
 * scheduler for the Calendar provider.  This needs to be done after
 * the boot completes because the alarm manager may not have been started
 * yet.
 */
public class CalendarReceiver extends BroadcastReceiver {
    private static final String TAG = "CalendarReceiver";

    static final String SCHEDULE = "com.android.providers.calendar.SCHEDULE_ALARM";

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        ContentResolver cr = context.getContentResolver();
        if (action.equals(SCHEDULE)) {
            cr.update(CalendarAlarmManager.SCHEDULE_ALARM_URI, null /* values */, null /* where */,
                    null /* selectionArgs */);
        } else if (action.equals(Intent.ACTION_BOOT_COMPLETED)) {
            removeScheduledAlarms(context, cr);
        }
    }

    /*
     * Remove alarms from the CalendarAlerts table that have been marked
     * as "scheduled" but not fired yet.  We do this because the
     * AlarmManagerService loses all information about alarms when the
     * power turns off but we store the information in a database table
     * that persists across reboots. See the documentation for
     * scheduleNextAlarmLocked() for more information.
     *
     * Running this on the main thread has caused ANRs, so we run it on a background
     * thread and start an "empty service" to encourage the system to keep us alive.
     *
     * We don't expect this to be called more than once.  If it were, we would have to
     * worry about serializing the use of the service.
     */
    private void removeScheduledAlarms(Context context, ContentResolver resolver) {
        context.startService(new Intent(context, RemoveScheduledAlarmsEmptyService.class));

        RemoveScheduledAlarmsThread thread = new RemoveScheduledAlarmsThread(context, resolver);
        thread.start();
    }

    /**
     * Background thread that handles cleanup of scheduled alarms.
     */
    private static class RemoveScheduledAlarmsThread extends Thread {
        private Context mContext;
        private ContentResolver mResolver;

        RemoveScheduledAlarmsThread(Context context, ContentResolver resolver) {
            mContext = context;
            mResolver = resolver;
        }

        @Override
        public void run() {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "Removing scheduled alarms");
            }
            mResolver.update(CalendarAlarmManager.SCHEDULE_ALARM_REMOVE_URI, null /* values */,
                    null /* where */, null /* selectionArgs */);
            mContext.stopService(new Intent(mContext, RemoveScheduledAlarmsEmptyService.class));
        }
    }

    /**
     * Background {@link Service} that is used to keep our process alive long enough
     * for background threads to finish.  Used for cleanup of scheduled alarms.
     */
    public static class RemoveScheduledAlarmsEmptyService extends Service {
        @Override
        public IBinder onBind(Intent intent) {
            return null;
        }
    }
}
