package com.android.providers.calendar;

public class CalendarProvider2ForTesting extends CalendarProvider2 {
    /**
     * For testing, don't want to start the TimezoneCheckerThread, as it results
     * in race conditions.  Thus updateTimezoneDependentFields is stubbed out.
     */
    @Override
    protected void updateTimezoneDependentFields() {
    }
}
