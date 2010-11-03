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

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.ContentResolver;
import android.content.Context;
import android.provider.Calendar;

/**
 * We are using the CalendarAlertManager to be able to mock the AlarmManager as the AlarmManager
 * cannot be extended.
 *
 * CalendarAlertManager is delegating its calls to the real AlarmService.
 */
public class CalendarAlarmManager {

    protected static final String TAG = "CalendarAlarmManager";

    private Context mContext;
    private AlarmManager mAlarmManager;

    public CalendarAlarmManager(Context context) {
        initializeWithContext(context);
    }

    protected void initializeWithContext(Context context) {
        mContext = context;
        mAlarmManager = (AlarmManager) context.getSystemService(Context.ALARM_SERVICE);
    }

    public void set(int type, long triggerAtTime, PendingIntent operation) {
        mAlarmManager.set(type, triggerAtTime, operation);
    }

    public void cancel(PendingIntent operation) {
        mAlarmManager.cancel(operation);
    }

    public void scheduleAlarm(long alarmTime) {
        Calendar.CalendarAlerts.scheduleAlarm(mContext, mAlarmManager, alarmTime);
    }

    public void rescheduleMissedAlarms(ContentResolver cr) {
        Calendar.CalendarAlerts.rescheduleMissedAlarms(cr, mContext, mAlarmManager);
    }
}
