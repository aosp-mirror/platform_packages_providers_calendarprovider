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

import android.content.BroadcastReceiver;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;

/**
 * This IntentReceiver executes when the boot completes and ensures that
 * the Calendar provider has started and then initializes the alarm
 * scheduler for the Calendar provider.  This needs to be done after
 * the boot completes because the alarm manager may not have been started
 * yet.
 */
public class CalendarReceiver extends BroadcastReceiver {

    static final String SCHEDULE = "com.android.providers.calendar.SCHEDULE_ALARM";

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        ContentResolver cr = context.getContentResolver();
        if (action.equals(SCHEDULE)) {
            cr.update(CalendarProvider2.SCHEDULE_ALARM_URI, null /* values */, null /* where */,
                    null /* selectionArgs */);
        } else if (action.equals(Intent.ACTION_BOOT_COMPLETED)) {
            // Remove alarms from the CalendarAlerts table that have been marked
            // as "scheduled" but not fired yet.  We do this because the
            // AlarmManagerService loses all information about alarms when the
            // power turns off but we store the information in a database table
            // that persists across reboots. See the documentation for
            // scheduleNextAlarmLocked() for more information.
            cr.update(CalendarProvider2.SCHEDULE_ALARM_REMOVE_URI,
                    null /* values */, null /* where */, null /* selectionArgs */);
        }
    }
}
