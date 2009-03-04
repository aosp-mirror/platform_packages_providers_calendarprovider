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
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.Context;
import android.content.IContentProvider;
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
        CalendarProvider provider;
        IContentProvider icp = cr.acquireProvider("calendar");
        provider = (CalendarProvider) ContentProvider.
                coerceToLocalContentProvider(icp);
        if (action.equals(SCHEDULE)) {
            provider.scheduleNextAlarm(false /* do not remove alarms */);
        } else if (action.equals(Intent.ACTION_BOOT_COMPLETED)) {
            provider.bootCompleted();
        }
        cr.releaseProvider(icp);
    }
}
