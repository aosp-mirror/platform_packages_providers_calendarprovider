/*
 * Copyright (C) 2021 The Android Open Source Project
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

import android.app.job.JobParameters;
import android.app.job.JobService;
import android.app.job.JobWorkItem;
import android.content.ContentProvider;
import android.content.IContentProvider;
import android.provider.CalendarContract;
import android.util.Log;
import android.util.Slog;

public class CalendarProviderJobService extends JobService {
    private static final String TAG = CalendarProvider2.TAG;
    static final int JOB_ID = CalendarProviderJobService.class.hashCode();

    private volatile boolean canRun;

    @Override
    public boolean onStartJob(JobParameters params) {
        if (!params.isExpeditedJob()) {
            Slog.w(TAG, "job not running as expedited");
        }

        final IContentProvider iprovider =
                getContentResolver().acquireProvider(CalendarContract.AUTHORITY);
        final ContentProvider cprovider = ContentProvider.coerceToLocalContentProvider(iprovider);

        if (!(cprovider instanceof CalendarProvider2)) {
            Slog.wtf(TAG, "CalendarProvider2 not found in CalendarProviderJobService.");
            return false;
        }

        canRun = true;
        final CalendarProvider2 provider = (CalendarProvider2) cprovider;

        new Thread(() -> {
            for (JobWorkItem jwi = params.dequeueWork(); jwi != null; jwi = params.dequeueWork()) {
                // Schedule the next alarm. Please be noted that for ACTION_EVENT_REMINDER
                // broadcast, we never remove scheduled alarms.
                final boolean removeAlarms = jwi.getIntent()
                        .getBooleanExtra(CalendarAlarmManager.KEY_REMOVE_ALARMS, false);
                provider.getOrCreateCalendarAlarmManager()
                        .runScheduleNextAlarm(removeAlarms, provider);

                if (Log.isLoggable(TAG, Log.DEBUG)) {
                    Log.d(TAG, "Next alarm set.");
                }
                if (!canRun) {
                    break;
                }
                params.completeWork(jwi);
            }
        }).start();
        return true;
    }

    @Override
    public boolean onStopJob(JobParameters params) {
        // The work should be very quick, so it'll be surprising if JS has to stop us.
        Slog.wtf(TAG, "CalendarProviderJobService was stopped due to "
                + JobParameters.getInternalReasonCodeDescription(params.getInternalStopReasonCode())
                + "(" + params.getStopReason() + ")");
        canRun = false;
        return true;
    }
}
