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

package com.android.providers.calendar.enterprise;

import android.provider.CalendarContract.Calendars;
import android.provider.CalendarContract.Events;
import android.provider.CalendarContract.Instances;
import android.test.AndroidTestCase;
import android.util.ArraySet;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class CrossProfileCalendarHelperTest extends AndroidTestCase {

    private CrossProfileCalendarHelper mHelper;

    public void setUp() throws Exception {
        super.setUp();
        mHelper = new CrossProfileCalendarHelper(mContext);
    }

    public void testProjectionNotWhitelisted_throwErrorForCalendars() {
        final String[] projection = new String[]{
                Calendars._ID,
                Calendars.OWNER_ACCOUNT
        };
        try {
            mHelper.getCalibratedProjection(projection, Calendars.CONTENT_URI);
            fail(String.format("Exception not found for projection %s", Calendars.OWNER_ACCOUNT));
        } catch (IllegalArgumentException e) {
            // Do nothing.
        }
    }

    public void testProjectionNotWhitelisted_throwErrorForEvents() {
        final String[] projection = new String[] {
                Events._ID,
                Events.DESCRIPTION
        };
        try {
            mHelper.getCalibratedProjection(projection, Events.CONTENT_URI);
            fail(String.format("Exception not found for projection %s", Events.DESCRIPTION));
        } catch (IllegalArgumentException e) {
            // Do nothing.
        }
    }

    public void testProjectionNotWhitelisted_throwErrorForInstances() {
        final String[] projection = new String[] {
                Instances._ID,
                Events.DESCRIPTION
        };
        try {
            mHelper.getCalibratedProjection(projection, Instances.CONTENT_URI);
            fail(String.format("Exception not found for projection %s", Events.DESCRIPTION));
        } catch (IllegalArgumentException e) {
            // Do nothing.
        }
    }

    public void testNoProjection_getFullWhitelistedProjectionForCalendars() {
        final String[] projection = mHelper.getCalibratedProjection(null, Calendars.CONTENT_URI);
        final Set<String> projectionSet = new ArraySet<String>(Arrays.asList(projection));
        assertTrue(Objects.deepEquals(CrossProfileCalendarHelper.CALENDARS_TABLE_WHITELIST,
                projectionSet));
    }

    public void testNoProjection_getFullWhitelistedProjectionForEvents() {
        final String[] projection = mHelper.getCalibratedProjection(null, Events.CONTENT_URI);
        final Set<String> projectionSet = new ArraySet<String>(Arrays.asList(projection));
        assertTrue(CrossProfileCalendarHelper.EVENTS_TABLE_WHITELIST.equals(projectionSet));
    }

    public void testNoProjection_getFullWhitelistedProjectionForInstances() {
        final String[] projection = mHelper.getCalibratedProjection(null, Instances.CONTENT_URI);
        final Set<String> projectionSet = new ArraySet<String>(Arrays.asList(projection));
        assertTrue(CrossProfileCalendarHelper.INSTANCES_TABLE_WHITELIST.equals(projectionSet));
    }
}
