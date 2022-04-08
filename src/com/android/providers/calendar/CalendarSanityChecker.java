/*
 * Copyright (C) 2017 The Android Open Source Project
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

import android.content.Context;
import android.content.SharedPreferences;
import android.os.SystemClock;
import android.os.UserManager;
import android.provider.Settings;
import android.provider.Settings.Global;
import android.text.format.DateUtils;
import android.util.Log;
import android.util.Slog;

import com.android.internal.annotations.GuardedBy;
import com.android.internal.annotations.VisibleForTesting;

/**
 * We call {@link #checkLastCheckTime} at the provider public entry points to make sure
 * {@link CalendarAlarmManager#scheduleNextAlarmLocked} has been called recently enough.
 *
 * atest tests/src/com/android/providers/calendar/CalendarSanityCheckerTest.java
 */
public class CalendarSanityChecker {
    private static final String TAG = "CalendarSanityChecker";

    private static final boolean DEBUG = false;

    private static final long MAX_ALLOWED_CHECK_INTERVAL_MS =
            CalendarAlarmManager.NEXT_ALARM_CHECK_TIME_MS;

    /**
     * If updateLastCheckTime isn't called after user unlock within this time,
     * we call scheduleNextAlarmCheckRightNow.
     */
    private static final long MAX_ALLOWED_REAL_TIME_AFTER_UNLOCK_MS =
            15 * DateUtils.MINUTE_IN_MILLIS;

    /**
     * Minimum interval between WTFs.
     */
    private static final long WTF_INTERVAL_MS = 60 * DateUtils.MINUTE_IN_MILLIS;

    private static final String PREF_NAME = "sanity";
    private static final String LAST_CHECK_REALTIME_PREF_KEY = "last_check_realtime";
    private static final String LAST_CHECK_BOOT_COUNT_PREF_KEY = "last_check_boot_count";
    private static final String LAST_WTF_REALTIME_PREF_KEY = "last_wtf_realtime";

    private final Context mContext;

    private final Object mLock = new Object();

    @GuardedBy("mLock")
    @VisibleForTesting
    final SharedPreferences mPrefs;

    public CalendarSanityChecker(Context context) {
        mContext = context;
        mPrefs = mContext.getSharedPreferences(PREF_NAME, Context.MODE_PRIVATE);
    }

    @VisibleForTesting
    protected long getRealtimeMillis() {
        return SystemClock.elapsedRealtime();
    }

    @VisibleForTesting
    protected long getBootCount() {
        return Settings.Global.getLong(mContext.getContentResolver(), Global.BOOT_COUNT, 0);
    }

    @VisibleForTesting
    protected long getUserUnlockTime() {
        final UserManager um = (UserManager) mContext.getSystemService(Context.USER_SERVICE);
        final long startTime = um.getUserStartRealtime();
        final long unlockTime = um.getUserUnlockRealtime();
        if (DEBUG) {
            Log.d(TAG, String.format("User start/unlock time=%d/%d", startTime, unlockTime));
        }
        return unlockTime;
    }

    /**
     * Called by {@link CalendarAlarmManager#scheduleNextAlarmLocked}
     */
    public final void updateLastCheckTime() {
        final long now = getRealtimeMillis();
        if (DEBUG) {
            Log.d(TAG, "updateLastCheckTime: now=" + now);
        }
        synchronized (mLock) {
            mPrefs.edit()
                    .putLong(LAST_CHECK_REALTIME_PREF_KEY, now)
                    .putLong(LAST_CHECK_BOOT_COUNT_PREF_KEY, getBootCount())
                    .apply();
        }
    }

    /**
     * Call this at public entry points. This will check if the last check time was recent enough,
     * and otherwise it'll call {@link CalendarAlarmManager#checkNextAlarmCheckRightNow}.
     */
    public final boolean checkLastCheckTime() {
        final long lastBootCount;
        final long lastCheckTime;
        final long lastWtfTime;

        synchronized (mLock) {
            lastBootCount = mPrefs.getLong(LAST_CHECK_BOOT_COUNT_PREF_KEY, -1);
            lastCheckTime = mPrefs.getLong(LAST_CHECK_REALTIME_PREF_KEY, -1);
            lastWtfTime = mPrefs.getLong(LAST_WTF_REALTIME_PREF_KEY, 0);

            final long nowBootCount = getBootCount();
            final long nowRealtime = getRealtimeMillis();

            final long unlockTime = getUserUnlockTime();

            if (DEBUG) {
                Log.d(TAG, String.format("isStateValid: %d/%d %d/%d unlocked=%d lastWtf=%d",
                        lastBootCount, nowBootCount, lastCheckTime, nowRealtime, unlockTime,
                        lastWtfTime));
            }

            if (lastBootCount != nowBootCount) {
                // This branch means updateLastCheckTime() hasn't been called since boot.

                debug("checkLastCheckTime: Last check time not set.");

                if (unlockTime == 0) {
                    debug("checkLastCheckTime: unlockTime=0."); // This shouldn't happen though.
                    return true;
                }

                if ((nowRealtime - unlockTime) <= MAX_ALLOWED_REAL_TIME_AFTER_UNLOCK_MS) {
                    debug("checkLastCheckTime: nowRealtime okay.");
                    return true;
                }
                debug("checkLastCheckTime: nowRealtime too old");
            } else {
                // This branch means updateLastCheckTime() has been called since boot.

                if ((nowRealtime - lastWtfTime) <= WTF_INTERVAL_MS) {
                    debug("checkLastCheckTime: Last WTF recent, skipping check.");
                    return true;
                }

                if ((nowRealtime - lastCheckTime) <= MAX_ALLOWED_CHECK_INTERVAL_MS) {
                    debug("checkLastCheckTime: Last check was recent, okay.");
                    return true;
                }
            }
            Slog.wtf(TAG, String.format("Last check time %d was too old. now=%d (boot count=%d/%d)",
                    lastCheckTime, nowRealtime, lastBootCount, nowBootCount));

            mPrefs.edit()
                    .putLong(LAST_CHECK_REALTIME_PREF_KEY, 0)
                    .putLong(LAST_WTF_REALTIME_PREF_KEY, nowRealtime)
                    .putLong(LAST_CHECK_BOOT_COUNT_PREF_KEY, getBootCount())
                    .apply();

            // Note mCalendarProvider2 really shouldn't be null.
            CalendarAlarmManager.checkNextAlarmCheckRightNow(mContext);
        }
        return false;
    }

    void debug(String message) {
        if (DEBUG) {
            Log.d(TAG, message);
        }
    }
}
