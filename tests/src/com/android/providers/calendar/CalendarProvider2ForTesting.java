package com.android.providers.calendar;

import android.accounts.Account;
import android.content.Context;
import android.os.PowerManager;

public class CalendarProvider2ForTesting extends CalendarProvider2 {

    private MockCalendarAlarmManager mMockCalendarAlarmManager;

    /**
     * For testing, don't want to start the TimezoneCheckerThread, as it results
     * in race conditions.  Thus updateTimezoneDependentFields is stubbed out.
     */
    @Override
    protected void updateTimezoneDependentFields() {
    }

    /**
     * For testing, don't want onAccountsUpdated asynchronously deleting data.
     */
    @Override
    public void onAccountsUpdated(Account[] accounts) {
    }

    @Override
    protected void doUpdateTimezoneDependentFields() {
    }

    @Override
    protected void postInitialize() {
    }

    @Override
    PowerManager.WakeLock getScheduleNextAlarmWakeLock() {
        return null;
    }

    @Override
    void acquireScheduleNextAlarmWakeLock() {
    }

    @Override
    void releaseScheduleNextAlarmWakeLock() {
    }

    @Override
    protected CalendarAlarmManager createCalendarAlarmManager() {
        return new MockCalendarAlarmManager();
    }

    private static class MockCalendarAlarmManager extends CalendarAlarmManager {

        public MockCalendarAlarmManager() {
            super(null);
        }

        @Override
        protected void initializeWithContext(Context context) {
        }
    }
}
