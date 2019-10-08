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

import android.app.admin.DevicePolicyManager;
import android.content.Context;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.UserHandle;
import android.provider.CalendarContract;
import android.provider.Settings;
import android.util.ArraySet;
import android.util.Log;

import java.util.Set;

/**
 * Helper class for cross profile calendar related policies.
 */
public class CrossProfileCalendarHelper {

    private static final String LOG_TAG = "CrossProfileCalendarHelper";

    final private Context mContext;

    public static final Set<String> EVENTS_TABLE_WHITELIST;
    public static final Set<String> CALENDARS_TABLE_WHITELIST;
    public static final Set<String> INSTANCES_TABLE_WHITELIST;

    static {
        EVENTS_TABLE_WHITELIST = new ArraySet<>();
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events._ID);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.CALENDAR_ID);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.TITLE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EVENT_LOCATION);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EVENT_COLOR);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.STATUS);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.DTSTART);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.DTEND);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EVENT_TIMEZONE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EVENT_END_TIMEZONE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.DURATION);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.ALL_DAY);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.AVAILABILITY);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.RRULE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.RDATE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EXRULE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.EXDATE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.LAST_DATE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.SELF_ATTENDEE_STATUS);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Events.DISPLAY_COLOR);

        CALENDARS_TABLE_WHITELIST = new ArraySet<>();
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars._ID);
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars.CALENDAR_COLOR);
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars.VISIBLE);
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars.CALENDAR_LOCATION);
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars.CALENDAR_TIME_ZONE);
        CALENDARS_TABLE_WHITELIST.add(CalendarContract.Calendars.IS_PRIMARY);

        INSTANCES_TABLE_WHITELIST = new ArraySet<>();
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances._ID);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.EVENT_ID);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.BEGIN);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.END);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.START_DAY);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.END_DAY);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.START_MINUTE);
        INSTANCES_TABLE_WHITELIST.add(CalendarContract.Instances.END_MINUTE);

        // Add calendar columns.
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Calendars.CALENDAR_COLOR);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Calendars.VISIBLE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Calendars.CALENDAR_TIME_ZONE);
        EVENTS_TABLE_WHITELIST.add(CalendarContract.Calendars.IS_PRIMARY);

        ((ArraySet<String>) INSTANCES_TABLE_WHITELIST).addAll(EVENTS_TABLE_WHITELIST);
    }

    public CrossProfileCalendarHelper(Context context) {
        mContext = context;
    }

    /**
     * @return a context created from the given context for the given user, or null if it fails.
     */
    private Context createPackageContextAsUser(Context context, int userId) {
        try {
            return context.createPackageContextAsUser(
                    context.getPackageName(), 0 /* flags */, UserHandle.of(userId));
        } catch (PackageManager.NameNotFoundException e) {
            Log.e(LOG_TAG, "Failed to create user context", e);
        }
        return null;
    }

    /**
     * Returns whether a package is allowed to access cross-profile calendar APIs.
     *
     * A package is allowed to access cross-profile calendar APIs if it's allowed by the
     * profile owner of a managed profile to access the managed profile calendar provider,
     * and the setting {@link Settings.Secure#CROSS_PROFILE_CALENDAR_ENABLED} is turned
     * on in the managed profile.
     *
     * @param packageName  the name of the package
     * @param managedProfileUserId the user id of the managed profile
     * @return {@code true} if the package is allowed, {@false} otherwise.
     */
    public boolean isPackageAllowedToAccessCalendar(String packageName, int managedProfileUserId) {
        final Context managedProfileUserContext = createPackageContextAsUser(
                mContext, managedProfileUserId);
        final DevicePolicyManager mDpm = managedProfileUserContext.getSystemService(
                DevicePolicyManager.class);
        return mDpm.isPackageAllowedToAccessCalendar(packageName);
    }

    private static void ensureProjectionAllowed(String[] projection, Set<String> validColumnsSet) {
        for (String column : projection) {
            if (!validColumnsSet.contains(column)) {
                throw new IllegalArgumentException(String.format("Column %s is not "
                        + "allowed to be accessed from cross profile Uris", column));
            }
        }
    }

    /**
     * Returns the calibrated version of projection for a given table.
     *
     * If the input projection is empty, return an array of all the whitelisted columns for a
     * given table. Table is determined by the input uri.
     *
     * @param projection the original projection
     * @param localUri the local uri for the query of the projection
     * @return the original value of the input projection if it's not empty, otherwise an array of
     * all the whitelisted columns.
     * @throws IllegalArgumentException if the input projection contains a column that is not
     * whitelisted for a given table.
     */
    public String[] getCalibratedProjection(String[] projection, Uri localUri) {
        // If projection is not empty, check if it's valid. Otherwise fill it with all
        // allowed columns.
        Set<String> validColumnsSet = new ArraySet<String>();
        if (CalendarContract.Events.CONTENT_URI.equals(localUri)) {
            validColumnsSet = EVENTS_TABLE_WHITELIST;
        } else if (CalendarContract.Calendars.CONTENT_URI.equals(localUri)) {
            validColumnsSet = CALENDARS_TABLE_WHITELIST;
        } else if (CalendarContract.Instances.CONTENT_URI.equals(localUri)
                || CalendarContract.Instances.CONTENT_BY_DAY_URI.equals(localUri)
                || CalendarContract.Instances.CONTENT_SEARCH_URI.equals(localUri)
                || CalendarContract.Instances.CONTENT_SEARCH_BY_DAY_URI.equals(localUri)) {
            validColumnsSet = INSTANCES_TABLE_WHITELIST;
        } else {
            throw new IllegalArgumentException(String.format("Cross profile version of %d is not "
                    + "supported", localUri.toSafeString()));
        }

        if (projection != null && projection.length > 0) {
            // If there exists some columns in original projection, check if these columns are
            // allowed.
            ensureProjectionAllowed(projection, validColumnsSet);
            return projection;
        }
        // Query of content provider will return cursor that contains all columns if projection is
        // null or empty. To be consistent with this behavior, we fill projection with all allowed
        // columns if it's null or empty for cross profile Uris.
        return validColumnsSet.toArray(new String[validColumnsSet.size()]);
    }
}
