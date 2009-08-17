/* //device/content/providers/pim/TestCalendarSyncAdapter.java
**
** Copyright 2006, The Android Open Source Project
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** See the License for the specific language governing permissions and
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** limitations under the License.
*/

package com.android.providers.calendar;

import android.content.ContentValues;
import android.content.SyncableContentProvider;
import android.content.SyncContext;
import android.content.SyncResult;
import android.content.TempProviderSyncAdapter;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.text.format.DateUtils;
import android.accounts.Account;
import android.accounts.AuthenticatorException;
import android.accounts.OperationCanceledException;

import java.util.Calendar;
import java.util.TimeZone;
import java.io.IOException;


public class TestCalendarSyncAdapter extends TempProviderSyncAdapter {
    private static Uri sEventsURL = Uri.parse("content://calendar/events/");
    private static Uri sDeletedEventsURL = Uri.parse("content://calendar/deleted_events/");

    public TestCalendarSyncAdapter(SyncableContentProvider provider) {
        super(provider);
    }

    @Override
    public void onSyncStarting(SyncContext context, Account account, boolean manualSync,
            SyncResult result)
    {
    }

    @Override
    public void onSyncEnding(SyncContext context, boolean success)
    {
    }
    
    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    public boolean getIsSyncable(Account account) 
            throws IOException, AuthenticatorException, OperationCanceledException {
        return true;
    }

    @Override
    public void getServerDiffs(SyncContext context, SyncData syncData,
            SyncableContentProvider tempProvider,
            Bundle extras, Object syncInfo, SyncResult syncResult) {
        switch(sSyncClock) {
            case 1: {
                ContentValues values = new ContentValues();
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

                values.put("title", "Sync performed");
                values.put("dtstart", DateUtils.writeDateTime(cal, true));
                values.put("duration", "PT1S");
                values.put("_sync_id", "server_event_1");
                values.put("_sync_time", Long.valueOf(System.currentTimeMillis()));

                tempProvider.insert(sEventsURL, values);
                break;
            }

            case 2: {
                ContentValues values = new ContentValues();
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                cal.clear();
                cal.set(1979, Calendar.APRIL, 6, 0, 0);

                values.put("title", "Jeff's b-day");
                values.put("dtstart", DateUtils.writeDateTime(cal, true));
                values.put("duration", "PT1D");
                values.put("_sync_id", "server_event_2");
                values.put("_sync_time", Long.valueOf(System.currentTimeMillis()));

                tempProvider.insert(sEventsURL, values);
                break;
            }

            case 3: {
                ContentValues values = new ContentValues();
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                cal.clear();
                cal.set(1979, Calendar.APRIL, 6, 0, 0);

                values.put("title", "Jeff's Birthday");
                values.put("dtstart", DateUtils.writeDateTime(cal, true));
                values.put("duration", "PT1D");
                values.put("_sync_id", "server_event_2");
                values.put("_sync_time", Long.valueOf(System.currentTimeMillis()));

                tempProvider.insert(sEventsURL, values);
                break;
            }

            case 4: {
                ContentValues values = new ContentValues();
                values.put("_sync_id", "server_event_1");
                tempProvider.insert(sDeletedEventsURL, values);
                break;
            }

            case 5: {
                ContentValues values = new ContentValues();
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                cal.clear();
                cal.set(1979, Calendar.APRIL, 6, 0, 0);

                values.put("title", "Jeff Hamilton's Birthday");
                values.put("dtstart", DateUtils.writeDateTime(cal, true));
                values.put("duration", "PT1D");
                values.put("_sync_id", "server_event_2");
                values.put("_sync_time", Long.valueOf(System.currentTimeMillis()));

                tempProvider.insert(sEventsURL, values);
                break;
            }
        }

        sSyncClock++;
    }

    @Override
    public void sendClientDiffs(SyncContext context,
            SyncableContentProvider clientDiffs,
            SyncableContentProvider serverDiffs, SyncResult syncResult,
            boolean dontActuallySendDeletes) {
        Cursor cursor = clientDiffs.query(sEventsURL, null, null, null, null);
        if (cursor == null || cursor.getCount() == 0) {
            throw new IllegalStateException("Empty client diffs");
        }

        int syncIDColumn = cursor.getColumnIndex("_sync_id");
        int syncTimeColumn = cursor.getColumnIndex("_sync_time");
        while(cursor.moveToNext()) {
            cursor.updateString(syncIDColumn, "client_event_" + mClientEventID++);
            cursor.updateLong(syncTimeColumn, System.currentTimeMillis());
        }
        cursor.commitUpdates();
        cursor.deactivate();
    }

    private int mClientEventID = 1;
    private static int sSyncClock = 1;

    @Override
    public void onSyncCanceled() {
        throw new UnsupportedOperationException("not implemented");
    }

    public void onAccountsChanged(Account[] accounts) {
    }
}


