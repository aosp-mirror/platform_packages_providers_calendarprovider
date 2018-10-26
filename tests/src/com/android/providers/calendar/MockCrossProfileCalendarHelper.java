/*
 * Copyright (C) 2018 The Android Open Source Project
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
 * limitations under the License
 */

package com.android.providers.calendar;

import android.content.Context;

import com.android.providers.calendar.enterprise.CrossProfileCalendarHelper;

public class MockCrossProfileCalendarHelper extends CrossProfileCalendarHelper {

    private static boolean mCrossProfileCalendarEnabledInSettings = true;
    private static boolean mCallingPackageWhitelisted = true;

    public MockCrossProfileCalendarHelper (Context context) {
        super(context);
    }

    /**
     * Mock this method in unit test since it depends on Settings provider.
     * It will be tested in integration test.
     */
    @Override
    public boolean isCrossProfileCalendarEnabledInSettings() {
        return mCrossProfileCalendarEnabledInSettings;
    }

    public static void setCrossProfileCalendarEnabledInSettings(boolean enabled) {
        mCrossProfileCalendarEnabledInSettings = enabled;
    }

    /**
     * Mock this method in unit test since it depends on DevicePolicyManager.
     * It will be tested in integration test.
     */
    @Override
    public boolean isPackageWhitelisted(String packageName, int managedProfileUserId) {
        return mCallingPackageWhitelisted;
    }

    public static void setPackageWhitelisted(boolean isWhitelisted) {
        mCallingPackageWhitelisted = isWhitelisted;
    }
}
