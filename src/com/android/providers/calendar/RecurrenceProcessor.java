/* //device/content/providers/pim/RecurrenceProcessor.java
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
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
** See the License for the specific language governing permissions and 
** limitations under the License.
*/

package com.android.providers.calendar;

import android.pim.DateException;
import android.pim.EventRecurrence;
import android.pim.RecurrenceSet;
import android.pim.Time;
import android.util.Log;

import java.util.TreeSet;

public class RecurrenceProcessor
{
    // these are created once and reused.
    private Time mIterator = new Time(Time.TIMEZONE_UTC);
    private Time mUntil = new Time(Time.TIMEZONE_UTC);
    private StringBuilder mStringBuilder = new StringBuilder();
    private Time mGenerated = new Time(Time.TIMEZONE_UTC);
    private DaySet mDays = new DaySet(false);

    public RecurrenceProcessor()
    {
    }

    private static final String TAG = "RecurrenceProcessor";

    private static final boolean SPEW = false;

    /**
     * Returns the time (millis since epoch) of the last occurrence, -1 if the event repeats
     * forever.
     */
    public long getLastOccurence(Time dtstart,
                                 RecurrenceSet recur) throws DateException {

        long lastTime = -1;
        boolean hasCount = false;

        // first see if there are any "until"s specified.  if so, use the latest
        // until / rdate.
        if (recur.rrules != null) {
            for (EventRecurrence rrule : recur.rrules) {
                if (rrule.count != 0) {
                    hasCount = true;
                }
                    
                if (rrule.until != null) {
                    // according to RFC 2445, until must be in UTC.
                    mIterator.parse2445(rrule.until);
                    long untilTime = mIterator.toMillis(false /* use isDst */);
                    if (untilTime > lastTime) {
                        lastTime = untilTime;
                    }
                }
            }
            if ((lastTime != -1) && recur.rdates != null) {
                for (long dt : recur.rdates) {
                    if (dt > lastTime) {
                        lastTime = dt;
                    }
                }
            }
            if (lastTime != -1) {
                return lastTime;
            }
        } else if ((recur.rdates != null) &&
                   ((recur.exrules == null) && (recur.exdates == null))) {
            // if there are only rdates, we can just pick the last one.
            for (long dt : recur.rdates) {
                if (dt > lastTime) {
                    lastTime = dt;
                }
            }
            return lastTime;
        }

        // expand the complete recurrence if there were any counts specified,
        // or if there were rdates specified.
        if ((recur.rrules != null && hasCount) || (recur.rdates != null)) {
            // could return 0 if the recurrence only occurs before dtstart?
            // i don't think we would have been called in that case.
            // TODO: check this!
            return expand(dtstart, recur,
                    dtstart.toMillis(false /* use isDst */) /* range start */,
                    -1 /* range end */, null /* output */);
        }
        return -1;
    }

    /**
     * a -- list of values
     * N -- number of values to use in a
     * v -- value to check for
     */
    private static boolean listContains(int[] a, int N, int v)
    {
        for (int i=0; i<N; i++) {
            if (a[i] == v) {
                return true;
            }
        }
        return false;
    }

    /**
     * a -- list of values
     * N -- number of values to use in a
     * v -- value to check for
     * max -- if a value in a is negative, add that negative value
     *        to max and compare that instead; this is how we deal with
     *        negative numbers being offsets from the end value
     */
    private static boolean listContains(int[] a, int N, int v, int max)
    {
        for (int i=0; i<N; i++) {
            int w = a[i];
            if (w > 0) {
                if (w == v) {
                    return true;
                }
            } else {
                max += w; // w is negative
                if (max == v) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Filter out the ones for events whose BYxxx rule is for
     * a period greater than or equal to the period of the FREQ.
     *
     * Returns 0 if the event should not be filtered out
     * Returns something else (a rule number which is useful for debugging)
     * if the event should not be returned
     */
    private static int filter(EventRecurrence r, Time iterator)
    {
        boolean found;
        int freq = r.freq;

        if (EventRecurrence.MONTHLY >= freq) {
            // BYMONTH
            if (r.bymonthCount > 0) {
                found = listContains(r.bymonth, r.bymonthCount,
                        iterator.month + 1);
                if (!found) {
                    return 1;
                }
            }
        }
        if (EventRecurrence.WEEKLY >= freq) {
            // BYWEEK -- this is just a guess.  I wonder how many events
            // acutally use BYWEEKNO.
            if (r.byweeknoCount > 0) {
                found = listContains(r.byweekno, r.byweeknoCount,
                                iterator.getWeekNumber(),
                                iterator.getActualMaximum(Time.WEEK_NUM));
                if (!found) {
                    return 2;
                }
            }
        }
        if (EventRecurrence.DAILY >= freq) {
            // BYYEARDAY
            if (r.byyeardayCount > 0) {
                found = listContains(r.byyearday, r.byyeardayCount,
                                iterator.yearDay, iterator.getActualMaximum(Time.YEAR_DAY));
                if (!found) {
                    return 3;
                }
            }
            // BYMONTHDAY
            if (r.bymonthdayCount > 0 ) {
                found = listContains(r.bymonthday, r.bymonthdayCount,
                                iterator.monthDay,
                                iterator.getActualMaximum(Time.MONTH_DAY));
                if (!found) {
                    return 4;
                }
            }
            // BYDAY -- when filtering, we ignore the number field, because it
            // only is meaningful when creating more events.
byday:
            if (r.bydayCount > 0) {
                int a[] = r.byday;
                int N = r.bydayCount;
                int v = EventRecurrence.timeDay2Day(iterator.weekDay);
                for (int i=0; i<N; i++) {
                    if (a[i] == v) {
                        break byday;
                    }
                }
                return 5;
            }
        }
        if (EventRecurrence.HOURLY >= freq) {
            // BYHOUR
            found = listContains(r.byhour, r.byhourCount,
                            iterator.hour,
                            iterator.getActualMaximum(Time.HOUR));
            if (!found) {
                return 6;
            }
        }
        if (EventRecurrence.MINUTELY >= freq) {
            // BYMINUTE
            found = listContains(r.byminute, r.byminuteCount,
                            iterator.minute,
                            iterator.getActualMaximum(Time.MINUTE));
            if (!found) {
                return 7;
            }
        }
        if (EventRecurrence.SECONDLY >= freq) {
            // BYSECOND
            found = listContains(r.bysecond, r.bysecondCount,
                            iterator.second,
                            iterator.getActualMaximum(Time.SECOND));
            if (!found) {
                return 8;
            }
        }
        // BYSETPOS -- we might have to do this by postprocessing
        // the list

        // if we got to here, we didn't filter it out
        return 0;
    }

    private static final int USE_ITERATOR = 0;
    private static final int USE_BYLIST = 1;

    /**
     * Return whether we should make this list from the BYxxx list or
     * from the component of the iterator.
     */
    int generateByList(int count, int freq, int byFreq)
    {
        if (byFreq >= freq) {
            return USE_ITERATOR;
        } else {
            if (count == 0) {
                return USE_ITERATOR;
            } else {
                return USE_BYLIST;
            }
        }
    }

    private static boolean useBYX(int freq, int freqConstant, int count)
    {
        return freq > freqConstant && count > 0;
    }

    public static class DaySet
    {
        public DaySet(boolean zulu)
        {
            mTime = new Time(Time.TIMEZONE_UTC);
        }

        void setRecurrence(EventRecurrence r)
        {
            mYear = 0;
            mMonth = -1;
            mR = r;
        }

        boolean get(Time iterator, int day)
        {
            int realYear = iterator.year;
            int realMonth = iterator.month;

            Time t = null;

            if (SPEW) {
                Log.i(TAG, "get called with iterator=" + iterator
                        + " " + iterator.month
                        + "/" + iterator.monthDay
                        + "/" + iterator.year + " day=" + day);
            }
            if (day < 1 || day > 28) {
                // if might be past the end of the month, we need to normalize it
                t = mTime;
                t.set(day, realMonth, realYear);
                t.normalize(true /* ignore isDst */);
                realYear = t.year;
                realMonth = t.month;
                day = t.monthDay;
                if (SPEW) {
                    Log.i(TAG, "normalized t=" + t + " " + t.month
                            + "/" + t.monthDay
                            + "/" + t.year);
                }
            }

            /*
            if (true || SPEW) {
                Log.i(TAG, "set t=" + t + " " + realMonth + "/" + day + "/" + realYear);
            }
            */
            if (realYear != mYear || realMonth != mMonth) {
                if (t == null) {
                    t = mTime;
                    t.set(day, realMonth, realYear);
                    t.normalize(true /* ignore isDst */);
                    if (SPEW) {
                        Log.i(TAG, "set t=" + t + " " + t.month
                                + "/" + t.monthDay
                                + "/" + t.year
                                + " realMonth=" + realMonth + " mMonth=" + mMonth);
                    }
                }
                mYear = realYear;
                mMonth = realMonth;
                mDays = generateDaysList(t, mR);
                if (SPEW) {
                    Log.i(TAG, "generated days list");
                }
            }
            return (mDays & (1<<day)) != 0;
        }

        /**
         * Fill in a bit set containing the days of the month on which this
         * will occur.
         *
         * Only call this if the r.freq > DAILY.  Otherwise, we should be
         * processing the BYDAY, BYMONTHDAY, etc. as filters instead.
         *
         * monthOffset may be -1, 0 or 1
         */
        private static int generateDaysList(Time generated, EventRecurrence r)
        {
            int days = 0;

            int i, count, v;
            int[] byday, bydayNum, bymonthday;
            int j, lastDayThisMonth;
            int first; // Time.SUNDAY, etc
            int k;

            lastDayThisMonth = generated.getActualMaximum(Time.MONTH_DAY);

            // BYDAY
            count = r.bydayCount;
            if (count > 0) {
                // calculate the day of week for the first of this month (first)
                j = generated.monthDay;
                while (j >= 8) {
                    j -= 7;
                }
                first = generated.weekDay;
                if (first >= j) {
                    first = first - j + 1;
                } else {
                    first = first - j + 8;
                }

                // What to do if the event is weekly:
                // This isn't ideal, but we'll generate a month's worth of events
                // and the code that calls this will only use the ones that matter
                // for the current week.
                byday = r.byday;
                bydayNum = r.bydayNum;
                for (i=0; i<count; i++) {
                    v = bydayNum[i];
                    j = EventRecurrence.day2TimeDay(byday[i]) - first + 1;
                    if (j <= 0) {
                        j += 7;
                    }
                    if (v == 0) {
                        // v is 0, each day in the month/week
                        for (; j<=lastDayThisMonth; j+=7) {
                            if (SPEW) Log.i(TAG, "setting " + j + " for rule "
                                    + v + "/" + EventRecurrence.day2TimeDay(byday[i]));
                            days |= 1 << j;
                        }
                    }
                    else if (v > 0) {
                        // v is positive, count from the beginning of the month
                        // -1 b/c the first one should add 0
                        j += 7*(v-1);
                        if (j <= lastDayThisMonth) {
                            if (SPEW) Log.i(TAG, "setting " + j + " for rule "
                                    + v + "/" + EventRecurrence.day2TimeDay(byday[i]));
                            // if it's impossible, we drop it
                            days |= 1 << j;
                        }
                    }
                    else {
                        // v is negative, count from the end of the month
                        // find the last one
                        for (; j<=lastDayThisMonth; j+=7) {
                        }
                        // v is negative
                        // should do +1 b/c the last one should add 0, but we also
                        // skipped the j -= 7 b/c the loop to find the last one
                        // overshot by one week
                        j += 7*v;
                        if (j >= 1) {
                            if (SPEW) Log.i(TAG, "setting " + j + " for rule "
                                    + v + "/" + EventRecurrence.day2TimeDay(byday[i]));
                            days |= 1 << j;
                        }
                    }
                }
            }

            // BYMONTHDAY
            // Q: What happens if we have BYMONTHDAY and BYDAY?
            // A: I didn't see it in the spec, so in lieu of that, we'll
            // intersect the two.  That seems reasonable to me.
            if (r.freq > EventRecurrence.WEEKLY) {
                count = r.bymonthdayCount;
                if (count != 0) {
                    bymonthday = r.bymonthday;
                    if (r.bydayCount == 0) {
                        for (i=0; i<count; i++) {
                            v = bymonthday[i];
                            if (v >= 0) {
                                days |= 1 << v;
                            } else {
                                j = lastDayThisMonth + v + 1; // v is negative
                                if (j >= 1 && j <= lastDayThisMonth) {
                                    days |= 1 << j;
                                }
                            }
                        }
                    } else {
                        // This is O(lastDayThisMonth*count), which is really
                        // O(count) with a decent sized constant.
                        for (j=1; j<=lastDayThisMonth; j++) {
                            next_day : { 
                                if ((days&(1<<j)) != 0) {
                                    for (i=0; i<count; i++) {
                                        if (bymonthday[i] == j) {
                                            break next_day;
                                        }
                                    }
                                    days &= ~(1<<j);
                                }
                            }
                        }
                    }
                }
            }
            return days;
        }

        private EventRecurrence mR;
        private int mDays;
        private Time mTime;
        private int mYear;
        private int mMonth;
    }


    // TODO: document, clean up these return codes.  currently, the return value
    // is the last occurrence of the recurrence within the expansion window.
    // 0 is returned if there are no instances within the expansion window.
    public long expand(Time dtstart,
                       RecurrenceSet recur,
                       long rangeStartMillis,
                       long rangeEndMillis,
                       TreeSet<Long> dtSet) throws DateException {
        if (dtSet != null) {
            dtSet.clear();
        } else {
            // create the set locally, for book-keeping.
            dtSet = new TreeSet<Long>();
        }

        if (recur.rrules != null) {
            for (EventRecurrence rrule : recur.rrules) {
                expand(dtstart, rrule, rangeStartMillis,
                       rangeEndMillis, true /* add */, dtSet);
            }
        }
        if (recur.rdates != null) {
            for (long dt : recur.rdates) {
                dtSet.add(dt);
            }
        }
        if (recur.exrules != null) {
            for (EventRecurrence exrule : recur.exrules) {
                expand(dtstart, exrule, rangeStartMillis,
                       rangeEndMillis, false /* remove */, dtSet);
            }
        }
        if (recur.exdates != null) {
            for (long dt : recur.exdates) {
                dtSet.remove(dt);
            }
        }
        if (dtSet.isEmpty()) {
            // this can happen if the recurrence does not occur within the
            // expansion window.
            return 0;
        }
        return dtSet.last();
    }

    /**
     * Run the recurrence algorithm.  Processes events defined in the local
     * timezone of the event.  Return a list of iCalendar DATETIME
     * strings containing the start date/times of the occurrences; the output
     * times are defined in the local timezone of the event.
     *
     * If you want all of the events, pass null for rangeEnd.  If you pass
     * null for rangeEnd, and the event doesn't have a COUNT or UNTIL field,
     * you'll get a DateException.
     *
     * @param dtstart the dtstart date as defined in RFC2445.  This
     * {@link Time} should be in the timezone of the event.
     * @param r the parsed recurrence, as defiend in RFC2445
     * @param rangeStartMillis the first date-time you care about, inclusive
     * @param rangeEndMillis the last date-time you care about, not inclusive (so
     *                  if you care about everything up through and including
     *                  Dec 22 1995, set last to Dec 23, 1995 00:00:00
     * @param add Whether or not we should add to out, or remove from out.
     * @param out the ArrayList you'd like to fill with the events
     */
    public void expand(Time dtstart,
                       EventRecurrence r,
                       long rangeStartMillis,
                       long rangeEndMillis,
                       boolean add,
                       TreeSet<Long> out) throws DateException {
        String timezone = dtstart.timezone;
        long dtstartMillis = dtstart.toMillis(false /* use isDst */);
        int count = 0;

        // add the dtstart instance to the recurrence, if within range.
        if (add) {
            if (dtstartMillis >= rangeStartMillis) {
                if ((rangeEndMillis != -1 && (dtstartMillis <= rangeEndMillis))
                    || r.count > 0) {
                        out.add(dtstartMillis);
                        ++count;
                }
            }
        }

        // reset the Time objects to the new timezone.
        // (we want to avoid creating new objects here)
        // we do NOT clear the until -- untils *must* be specified in UTC.
        mIterator.clear(timezone);
        mGenerated.clear(timezone);
        
        Time iterator = mIterator;
        Time until = mUntil;
        StringBuilder sb = mStringBuilder;
        Time generated = mGenerated;
        DaySet days = mDays;

        try {

            days.setRecurrence(r);

            if (rangeEndMillis == -1 && r.until == null && r.count == 0) {
                throw new DateException(
                        "No range end provided for a recurrence that has no UNTIL or COUNT.");
            }

            // the top-level frequency
            int freqField;
            int freqAmount = r.interval;
            int freq = r.freq;
            switch (freq)
            {
                case EventRecurrence.SECONDLY:
                    freqField = Time.SECOND;
                    break;
                case EventRecurrence.MINUTELY:
                    freqField = Time.MINUTE;
                    break;
                case EventRecurrence.HOURLY:
                    freqField = Time.HOUR;
                    break;
                case EventRecurrence.DAILY:
                    freqField = Time.MONTH_DAY;
                    break;
                case EventRecurrence.WEEKLY:
                    freqField = Time.MONTH_DAY;
                    freqAmount = 7 * r.interval;
                    if (freqAmount <= 0) {
                        freqAmount = 7;
                    }
                    break;
                case EventRecurrence.MONTHLY:
                    freqField = Time.MONTH;
                    break;
                case EventRecurrence.YEARLY:
                    freqField = Time.YEAR;
                    break;
                default:
                    throw new DateException("bad freq=" + freq);
            }
            if (freqAmount <= 0) {
                freqAmount = 1;
            }

            int bymonthCount = r.bymonthCount;
            boolean usebymonth = useBYX(freq, EventRecurrence.MONTHLY, bymonthCount);
            boolean useDays = freq >= EventRecurrence.WEEKLY &&
                                 (r.bydayCount > 0 || r.bymonthdayCount > 0);
            int byhourCount = r.byhourCount;
            boolean usebyhour = useBYX(freq, EventRecurrence.HOURLY, byhourCount);
            int byminuteCount = r.byminuteCount;
            boolean usebyminute = useBYX(freq, EventRecurrence.MINUTELY, byminuteCount);
            int bysecondCount = r.bysecondCount;
            boolean usebysecond = useBYX(freq, EventRecurrence.SECONDLY, bysecondCount);

            // initialize the iterator
            iterator.set(dtstart);
            if (freqField == Time.MONTH) {
                if (useDays) {
                    // if it's monthly, and we're going to be generating
                    // days, set the iterator day field to 1 because sometimes
                    // we'll skip months if it's greater than 28.
                    // XXX Do we generate days for MONTHLY w/ BYHOUR?  If so,
                    // we need to do this then too.
                    iterator.monthDay = 1;
                }
            }

            if (r.until != null) {
                until.parse(r.until);
            }
            long untilMillis = until.toMillis(false /* use isDst */);

            sb.ensureCapacity(15);
            sb.setLength(15); // TODO: pay attention to whether or not the event
            // is an all-day one.

            if (SPEW) {
                Log.i(TAG, "expand called w/ rangeStart=" + rangeStartMillis
                        + " rangeEnd=" + rangeEndMillis);
            }

            // go until the end of the range or we're done with this event
            boolean eventEnded = false;
            int N, i, v;
            int a[];
            events: {
                while (true) {
                    int monthIndex = 0;

                    iterator.normalize(true /* ignore isDst */);

                    int iteratorYear = iterator.year;
                    int iteratorMonth = iterator.month + 1;
                    int iteratorDay = iterator.monthDay;
                    int iteratorHour = iterator.hour;
                    int iteratorMinute = iterator.minute;
                    int iteratorSecond = iterator.second;

                    // year is never expanded -- there is no BYYEAR
                    generated.set(iterator);

                    if (SPEW) Log.i(TAG, "year=" + generated.year);

                    do { // month
                        int month = usebymonth
                                        ? r.bymonth[monthIndex]
                                        : iteratorMonth;
                        month--;
                        if (SPEW) Log.i(TAG, "  month=" + month);

                        int dayIndex = 1;
                        int lastDayToExamine = 0;

                        // Use this to handle weeks that overlap the end of the month.
                        // Keep the year and month that days is for, and generate it
                        // when needed in the loop
                        if (useDays) {
                            // Determine where to start and end, don't worry if this happens
                            // to be before dtstart or after the end, because that will be
                            // filtered in the inner loop
                            if (freq == EventRecurrence.WEEKLY) {
                                int dow = iterator.weekDay;
                                dayIndex = iterator.monthDay - dow;
                                lastDayToExamine = dayIndex + 6;
                            } else {
                                lastDayToExamine = generated
                                    .getActualMaximum(Time.MONTH_DAY);
                            }
                            if (SPEW) Log.i(TAG, "dayIndex=" + dayIndex
                                    + " lastDayToExamine=" + lastDayToExamine
                                    + " days=" + days);
                        }

                        do { // day
                            int day;
                            if (useDays) {
                                if (!days.get(iterator, dayIndex)) {
                                    dayIndex++;
                                    continue;
                                } else {
                                    day = dayIndex;
                                }
                            } else {
                                day = iteratorDay;
                            }
                            if (SPEW) Log.i(TAG, "    day=" + day);

                            // hour
                            int hourIndex = 0;
                            do {
                                int hour = usebyhour
                                                ? r.byhour[hourIndex]
                                                : iteratorHour;
                                if (SPEW) Log.i(TAG, "      hour=" + hour + " usebyhour=" + usebyhour);

                                // minute
                                int minuteIndex = 0;
                                do {
                                    int minute = usebyminute
                                                    ? r.byminute[minuteIndex]
                                                    : iteratorMinute;
                                    if (SPEW) Log.i(TAG, "        minute=" + minute);

                                    // second
                                    int secondIndex = 0;
                                    do {
                                        int second = usebysecond
                                                        ? r.bysecond[secondIndex]
                                                        : iteratorSecond;
                                        if (SPEW) Log.i(TAG, "          second=" + second);

                                        // we do this here each time, because if we distribute it, we find the
                                        // month advancing extra times, as we set the month to the 32nd, 33rd, etc.
                                        // days.
                                        generated.set(second, minute, hour, day, month, iteratorYear);

                                        long genMillis = generated.normalize(true /* ignore DST */);
                                        // sometimes events get generated (BYDAY, BYHOUR, etc.) that
                                        // are before dtstart.  Filter these.  I believe this is correct,
                                        // but Google Calendar doesn't seem to always do this.
                                        if (genMillis >= dtstartMillis) {
                                            // filter and then add
                                            int filtered = filter(r, generated);
                                            if (0 == filtered) {

                                                // increase the count as long
                                                // as this isn't the same
                                                // as the first instance
                                                // specified by the DTSTART
                                                // (for RRULEs -- additive).
                                                if (!add) {
                                                    ++count;
                                                } else if (dtstartMillis !=
                                                               genMillis) {
                                                    ++count;
                                                }
                                                // one reason we can stop is that we're past the until date
                                                if (r.until != null &&
                                                    genMillis > untilMillis) {
                                                    if (SPEW) {
                                                        Log.i(TAG, "stopping b/c until="
                                                            + untilMillis
                                                            + " generated="
                                                            + genMillis);
                                                    }
                                                    break events;
                                                }
                                                // or we're past rangeEnd
                                                if (rangeEndMillis != -1 &&
                                                    genMillis >= rangeEndMillis) {
                                                    if (SPEW) {
                                                        Log.i(TAG, "stopping b/c rangeEnd="
                                                                + rangeEndMillis
                                                                + " generated=" + generated);
                                                    }
                                                    break events;
                                                }

                                                if (out != null &&
                                                    genMillis >= rangeStartMillis) {
                                                    if (SPEW) {
                                                        Log.i(TAG, "adding date=" + generated + " filtered=" + filtered);
                                                    }
                                                    if (add) {
                                                        out.add(genMillis);
                                                    } else {
                                                        out.remove(genMillis);
                                                    }
                                                }
                                                // another is that count is high enough
                                                if (r.count > 0 && r.count == count) {
                                                    //Log.i(TAG, "stopping b/c count=" + count);
                                                    break events;
                                                }
                                            }
                                        }
                                        secondIndex++;
                                    } while (usebysecond && secondIndex < bysecondCount);
                                    minuteIndex++;
                                } while (usebyminute && minuteIndex < byminuteCount);
                                hourIndex++;
                            } while (usebyhour && hourIndex < byhourCount);
                            dayIndex++;
                        } while (useDays && dayIndex <= lastDayToExamine);
                        monthIndex++;
                    } while (usebymonth && monthIndex < bymonthCount);

                    // Add freqAmount to freqField until we get another date that we want.
                    // We don't want to "generate" dates with the iterator.
                    // XXX: We do this for days, because there is a varying number of days
                    // per month
                    int oldDay = iterator.monthDay;
                    generated.set(iterator);  // just using generated as a temporary.
                    int n = 1;
                    while (true) {
                        int value = freqAmount * n;
                        switch (freqField) {
                            case Time.SECOND:
                                iterator.second += value;
                                break;
                            case Time.MINUTE:
                                iterator.minute += value;
                                break;
                            case Time.HOUR:
                                iterator.hour += value;
                                break;
                            case Time.MONTH_DAY:
                                iterator.monthDay += value;
                                break;
                            case Time.MONTH:
                                iterator.month += value;
                                break;
                            case Time.YEAR:
                                iterator.year += value;
                                break;
                            case Time.WEEK_DAY:
                                iterator.monthDay += value;
                                break;
                            case Time.YEAR_DAY:
                                iterator.monthDay += value;
                                break;
                            default:
                                throw new RuntimeException("bad field=" + freqField);
                        }

                        iterator.normalize(true /* ignore isDst */);
                        if (freqField != Time.YEAR && freqField != Time.MONTH) {
                            break;
                        }
                        if (iterator.monthDay == oldDay) {
                            break;
                        }
                        n++;
                        iterator.set(generated);
                    }
                }
            }
        }
        catch (DateException e) {
            Log.w(TAG, "DateException with r=" + r + " rangeStart=" + rangeStartMillis
                    + " rangeEnd=" + rangeEndMillis);
            throw e;
        }
        catch (RuntimeException t) {
            Log.w(TAG, "RuntimeException with r=" + r + " rangeStart=" + rangeStartMillis
                    + " rangeEnd=" + rangeEndMillis);
            throw t;
        }
    }
}

