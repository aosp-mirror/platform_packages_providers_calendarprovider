/*
 * Copyright (C) 2009 The Android Open Source Project
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

import android.app.Service;
import android.os.PowerManager.WakeLock;
import android.util.Log;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Set of static variables that are shared between
 * {@link CalendarAppWidgetProvider} and {@link CalendarAppWidgetService} and
 * guarded by {@link #sLock}.
 */
public class AppWidgetShared {
    static final String TAG = "AppWidgetShared";
    static final boolean LOGD = false;
    
    static Object sLock = new Object();
    static WakeLock sWakeLock;
    static boolean sUpdateRequested = false;
    static boolean sUpdateRunning = false;
    
    /**
     * {@link System#currentTimeMillis()} at last update request. 
     */
    static long sLastRequest = -1;

    private static HashSet<Integer> sAppWidgetIds = new HashSet<Integer>();
    private static HashSet<Long> sChangedEventIds = new HashSet<Long>();
    
    /**
     * Merge a set of filtering appWidgetIds with those from other pending
     * requests. If null, then reset the filter to match all.
     * <p>
     * Only call this while holding a {@link #sLock} lock.
     */
    static void mergeAppWidgetIdsLocked(int[] appWidgetIds) {
        if (appWidgetIds != null) {
            for (int appWidgetId : appWidgetIds) {
                sAppWidgetIds.add(appWidgetId);
            }
        } else {
            sAppWidgetIds.clear();
        }
    }
    
    /**
     * Merge a set of filtering changedEventIds with those from other pending
     * requests. If null, then reset the filter to match all.
     * <p>
     * Only call this while holding a {@link #sLock} lock.
     */
    static void mergeChangedEventIdsLocked(long[] changedEventIds) {
        if (changedEventIds != null) {
            for (long changedEventId : changedEventIds) {
                sChangedEventIds.add(changedEventId);
            }
        } else {
            sChangedEventIds.clear();
        }
    }

    /**
     * Collect all currently requested appWidgetId filters, returning as single
     * list. This call also clears the internal list.
     * <p>
     * Only call this while holding a {@link #sLock} lock.
     */
    static int[] collectAppWidgetIdsLocked() {
        final int size = sAppWidgetIds.size();
        int[] array = new int[size];
        Iterator<Integer> iterator = sAppWidgetIds.iterator();
        for (int i = 0; i < size; i++) {
            array[i] = iterator.next();
        }
        sAppWidgetIds.clear();
        return array;
    }

    /**
     * Collect all currently requested changedEventId filters, returning as
     * single list. This call also clears the internal list.
     * <p>
     * Only call this while holding a {@link #sLock} lock.
     */
    static Set<Long> collectChangedEventIdsLocked() {
        Set<Long> set = new HashSet<Long>();
        for (Long value : sChangedEventIds) {
            set.add(value);
        }
        sChangedEventIds.clear();
        return set;
    }

    /**
     * Call this at any point to release any {@link WakeLock} and reset to
     * default state. Usually called before {@link Service#stopSelf()}.
     * <p>
     * Only call this while holding a {@link #sLock} lock.
     */
    static void clearLocked() {
        if (sWakeLock != null && sWakeLock.isHeld()) {
            if (LOGD) Log.d(TAG, "found held wakelock, so releasing");
            sWakeLock.release();
        }
        sWakeLock = null;
        
        sUpdateRequested = false;
        sUpdateRunning = false;
        
        sAppWidgetIds.clear();
        sChangedEventIds.clear();
    }
}
