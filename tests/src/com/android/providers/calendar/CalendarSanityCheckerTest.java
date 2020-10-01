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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import android.content.Context;
import android.test.suitebuilder.annotation.SmallTest;
import android.text.format.DateUtils;

import androidx.test.InstrumentationRegistry;
import androidx.test.runner.AndroidJUnit4;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(AndroidJUnit4.class)
@SmallTest
public class CalendarSanityCheckerTest {
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

    private Context mContext;
    private CalendarSanityCheckerTestable mSanityChecker;

    private long mInjectedRealtimeMillis = 1000000L;
    private long mInjectedBootCount = 1000;
    private long mInjectedUnlockTime = 0;

    @Before
    public void setUp() {
        mContext = InstrumentationRegistry.getContext();
        mSanityChecker = new CalendarSanityCheckerTestable(mContext);
        mSanityChecker.mPrefs.edit().clear().commit();
    }

    @Test
    public void testWithoutLastCheckTime() {
        assertTrue(mSanityChecker.checkLastCheckTime());

        // Unlock.
        mInjectedUnlockTime = mInjectedRealtimeMillis;

        mInjectedRealtimeMillis += 15 * 60 * 1000;
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(mSanityChecker.checkLastCheckTime());
    }

    @Test
    public void testWithLastCheckTime() {
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedUnlockTime = mInjectedRealtimeMillis;

        // Update the last check time.
        mInjectedRealtimeMillis += 1 * 60 * 1000;
        mSanityChecker.updateLastCheckTime();

        // Right after, okay.
        assertTrue(mSanityChecker.checkLastCheckTime());

        // Still okay.
        mInjectedRealtimeMillis += DateUtils.DAY_IN_MILLIS - (15 * DateUtils.MINUTE_IN_MILLIS);
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(mSanityChecker.checkLastCheckTime());

        // Repeat the same thing.
        mInjectedRealtimeMillis += 1 * 60 * 1000;
        mSanityChecker.updateLastCheckTime();

        // Right after, okay.
        assertTrue(mSanityChecker.checkLastCheckTime());

        // Still okay.
        mInjectedRealtimeMillis += DateUtils.DAY_IN_MILLIS - (15 * DateUtils.MINUTE_IN_MILLIS);
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(mSanityChecker.checkLastCheckTime());

        // Check again right after. This should pass because of WTF_INTERVAL_MS.
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedRealtimeMillis += 60 * 60 * 1000;

        // Still okay.
        assertTrue(mSanityChecker.checkLastCheckTime());

        // Now WTF again.
        mInjectedRealtimeMillis += 1;
        assertFalse(mSanityChecker.checkLastCheckTime());

        // Reboot.
        mInjectedRealtimeMillis = 1000000L;
        mInjectedBootCount++;

        // Unlock.
        mInjectedUnlockTime = mInjectedRealtimeMillis;

        mInjectedRealtimeMillis += 15 * 60 * 1000;
        assertTrue(mSanityChecker.checkLastCheckTime());

        mInjectedRealtimeMillis += 1;
        assertFalse(mSanityChecker.checkLastCheckTime());

        // Check again right after. This should pass because of WTF_INTERVAL_MS.
        assertTrue(mSanityChecker.checkLastCheckTime());
    }
}
