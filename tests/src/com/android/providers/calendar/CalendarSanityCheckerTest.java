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
import android.test.AndroidTestCase;
import android.text.format.DateUtils;

public class CalendarSanityCheckerTest extends AndroidTestCase {
    private class CalendarSanityCheckerTestable extends CalendarSanityChecker {
        protected CalendarSanityCheckerTestable(Context context) {
            super(context);
        }

        @Override
        protected long getRealtimeMillis() {
            return mInjectedRealtimeMillis;
        }

        @Override
        protected long getBootCount() {
            return mInjectedBootCount;
        }

        @Override
        protected long getUserUnlockTime() {
            return mInjectedUnlockTime;
        }
    }

    private long mInjectedRealtimeMillis = 1000000L;
    private long mInjectedBootCount = 1000;
    private long mInjectedUnlockTime = 0;

    public void testWithoutLastCheckTime() {
        CalendarSanityCheckerTestable target = new CalendarSanityCheckerTestable(getContext());
        target.mPrefs.edit().clear().commit();

        assertTrue(target.checkLastCheckTime());

        // Unlock.
        mInjectedUnlockTime = mInjectedRealtimeMillis;

        mInjectedRealtimeMillis += 15 * 60 * 1000;
        assertTrue(target.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(target.checkLastCheckTime());
    }

    public void testWithLastCheckTime() {
        CalendarSanityCheckerTestable target = new CalendarSanityCheckerTestable(getContext());
        target.mPrefs.edit().clear().commit();

        assertTrue(target.checkLastCheckTime());

        mInjectedUnlockTime = mInjectedRealtimeMillis;

        // Update the last check time.
        mInjectedRealtimeMillis += 1 * 60 * 1000;
        target.updateLastCheckTime();

        // Right after, okay.
        assertTrue(target.checkLastCheckTime());

        // Still okay.
        mInjectedRealtimeMillis += DateUtils.DAY_IN_MILLIS - (15 * DateUtils.MINUTE_IN_MILLIS);
        assertTrue(target.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(target.checkLastCheckTime());

        // Repeat the same thing.
        mInjectedRealtimeMillis += 1 * 60 * 1000;
        target.updateLastCheckTime();

        // Right after, okay.
        assertTrue(target.checkLastCheckTime());

        // Still okay.
        mInjectedRealtimeMillis += DateUtils.DAY_IN_MILLIS - (15 * DateUtils.MINUTE_IN_MILLIS);
        assertTrue(target.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(target.checkLastCheckTime());

        // Check again right after. This should pass because of WTF_INTERVAL_MS.
        assertTrue(target.checkLastCheckTime());

        mInjectedRealtimeMillis += 60 * 60 * 1000;

        // Still okay.
        assertTrue(target.checkLastCheckTime());

        // Now WTF again.
        mInjectedRealtimeMillis += 1;
        assertFalse(target.checkLastCheckTime());

        // Reboot.
        mInjectedRealtimeMillis = 1000000L;
        mInjectedBootCount++;

        // Unlock.
        mInjectedUnlockTime = mInjectedRealtimeMillis;

        mInjectedRealtimeMillis += 15 * 60 * 1000;
        assertTrue(target.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(target.checkLastCheckTime());

        // Check again right after. This should pass because of WTF_INTERVAL_MS.
        assertTrue(target.checkLastCheckTime());
    }
}
