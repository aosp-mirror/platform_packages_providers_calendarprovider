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
import android.util.Log;

/**
 * We are using the CalendarAlertManager to be able to mock the AlarmManager as the AlarmManager
 * cannot be extended.
 *
 * CalendarAlertManager is delegating its calls to the real AlarmService. When mocking
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

    protected boolean checkAlarmService() {
        if (mAlarmManager == null) {
            if (Log.isLoggable(TAG, Log.DEBUG)) {
                Log.d(TAG, "AlarmService is not set");
            }
            return false;
        }
        return true;
    }

    public void set(int type, long triggerAtTime, PendingIntent operation) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.set(type, triggerAtTime, operation);
    }

    public void setRepeating(int type, long triggerAtTime, long interval,
            PendingIntent operation) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.setRepeating(type, triggerAtTime, interval, operation);
    }

    public void setInexactRepeating(int type, long triggerAtTime, long interval,
            PendingIntent operation) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.setInexactRepeating(type, triggerAtTime, interval, operation);
    }

    public void cancel(PendingIntent operation) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.cancel(operation);
    }

    public void setTime(long millis) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.setTime(millis);
    }

    public void setTimeZone(String timeZone) {
        if (!checkAlarmService()) {
            return;
        }
        mAlarmManager.setTimeZone(timeZone);
    }

    public void scheduleAlarm(long alarmTime) {
        if (!checkAlarmService()) {
            return;
        }
        Calendar.CalendarAlerts.scheduleAlarm(mContext, mAlarmManager, alarmTime);
    }

    public void rescheduleMissedAlarms(ContentResolver cr) {
        if (!checkAlarmService()) {
            return;
        }
        Calendar.CalendarAlerts.rescheduleMissedAlarms(cr, mContext, mAlarmManager);
    }
}
