package com.android.providers.calendar;

public class CalendarProviderForTesting extends CalendarProvider {
    /**
     * For testing, don't want to start the TimezoneCheckerThread, as it results
     * in race conditions.  Thus updateTimezoneDependentFields is stubbed out.
     */
    @Override
    protected void updateTimezoneDependentFields() {
    }
}