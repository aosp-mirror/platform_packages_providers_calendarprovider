/*
 * Copyright (C) 2010 The Android Open Source Project
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

import android.app.Activity;
import android.app.job.JobInfo;
import android.app.job.JobScheduler;
import android.app.job.JobWorkItem;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.provider.CalendarContract;
import android.util.Log;
import android.util.Slog;

public class CalendarProviderBroadcastReceiver extends BroadcastReceiver {
    private static final String TAG = CalendarProvider2.TAG;

    @Override
    public void onReceive(Context context, Intent intent) {
        String action = intent.getAction();
        if (action == null ||
                (!CalendarAlarmManager.ACTION_CHECK_NEXT_ALARM.equals(action)
                    && !CalendarContract.ACTION_EVENT_REMINDER.equals(action))) {
            Log.e(TAG, "Received invalid intent: " + intent);
            setResultCode(Activity.RESULT_CANCELED);
            return;
        }
        if (Log.isLoggable(TAG, Log.DEBUG)) {
            Log.d(TAG, "Received intent: " + intent);
        }

        JobWorkItem jwi = new JobWorkItem(intent);
        JobInfo.Builder alarmJobBuilder = new JobInfo.Builder(CalendarProviderJobService.JOB_ID,
                new ComponentName(context, CalendarProviderJobService.class))
                .setExpedited(true);
        JobScheduler jobScheduler = context.getSystemService(JobScheduler.class);
        if (jobScheduler.enqueue(alarmJobBuilder.build(), jwi) == JobScheduler.RESULT_SUCCESS) {
            setResultCode(Activity.RESULT_OK);
        } else {
            Slog.wtf(TAG, "Failed to schedule expedited job");
            // Unable to schedule an expedited job. Fall back to a regular job.
            alarmJobBuilder.setExpedited(false);
            if (jobScheduler.enqueue(alarmJobBuilder.build(), jwi) == JobScheduler.RESULT_SUCCESS) {
                setResultCode(Activity.RESULT_OK);
            } else {
                Slog.wtf(TAG, "Failed to schedule regular job");
            }
        }
    }
}
