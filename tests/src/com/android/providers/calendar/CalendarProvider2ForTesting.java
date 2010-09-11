package com.android.providers.calendar;

import android.accounts.Account;

public class CalendarProvider2ForTesting extends CalendarProvider2 {
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
}
