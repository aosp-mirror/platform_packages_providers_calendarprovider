/*
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

import com.google.android.gdata.client.AndroidGDataClient;
import com.google.android.gdata.client.AndroidXmlParserFactory;
import com.google.android.providers.AbstractGDataSyncAdapter;
import com.google.wireless.gdata.calendar.client.CalendarClient;
import com.google.wireless.gdata.calendar.data.EventEntry;
import com.google.wireless.gdata.calendar.data.EventsFeed;
import com.google.wireless.gdata.calendar.data.Reminder;
import com.google.wireless.gdata.calendar.data.When;
import com.google.wireless.gdata.calendar.data.Who;
import com.google.wireless.gdata.calendar.parser.xml.XmlCalendarGDataParserFactory;
import com.google.wireless.gdata.client.GDataServiceClient;
import com.google.wireless.gdata.client.QueryParams;
import com.google.wireless.gdata.data.Entry;
import com.google.wireless.gdata.data.Feed;
import com.google.wireless.gdata.data.StringUtils;
import com.google.wireless.gdata.parser.ParseException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SyncContext;
import android.content.SyncResult;
import android.content.SyncableContentProvider;
import android.database.Cursor;
import android.database.CursorJoiner;
import android.net.Uri;
import android.os.Bundle;
import android.os.SystemProperties;
import android.pim.ICalendar;
import android.pim.RecurrenceSet;
import android.provider.Calendar;
import android.provider.SubscribedFeeds;
import android.provider.SyncConstValue;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Events;
import android.text.TextUtils;
import android.text.format.Time;
import android.util.Config;
import android.util.Log;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

/**
 * SyncAdapter for Google Calendar.  Fetches the list of the user's calendars,
 * and for each calendar that is marked as &quot;selected&quot; in the web
 * interface, syncs that calendar.
 */
public final class CalendarSyncAdapter extends AbstractGDataSyncAdapter {

    /* package */ static final String USER_AGENT_APP_VERSION = "Android-GData-Calendar/1.1";

    private static final String SELECT_BY_ACCOUNT = Calendars._SYNC_ACCOUNT + "=?";
    private static final String SELECT_BY_ACCOUNT_AND_FEED =
            SELECT_BY_ACCOUNT + " AND " + Calendars.URL + "=?";

    private static final String[] CALENDAR_KEY_COLUMNS =
            new String[]{Calendars._SYNC_ACCOUNT, Calendars.URL};
    private static final String CALENDAR_KEY_SORT_ORDER =
            Calendars._SYNC_ACCOUNT + "," + Calendars.URL;
    private static final String[] FEEDS_KEY_COLUMNS =
            new String[]{SubscribedFeeds.Feeds._SYNC_ACCOUNT, SubscribedFeeds.Feeds.FEED};
    private static final String FEEDS_KEY_SORT_ORDER =
            SubscribedFeeds.Feeds._SYNC_ACCOUNT + ", " + SubscribedFeeds.Feeds.FEED;

    public static class SyncInfo {
        // public String feedUrl;
        public long calendarId;
        public String calendarTimezone;
    }

    private static final String TAG = "Sync";
    private static final Integer sTentativeStatus = Events.STATUS_TENTATIVE;
    private static final Integer sConfirmedStatus = Events.STATUS_CONFIRMED;
    private static final Integer sCanceledStatus = Events.STATUS_CANCELED;

    private final CalendarClient mCalendarClient;

    private ContentResolver mContentResolver;

    private static final String[] CALENDARS_PROJECTION = new String[] {
            Calendars._ID,            // 0
            Calendars.SELECTED,       // 1
            Calendars._SYNC_TIME,     // 2
            Calendars.URL,            // 3
            Calendars.DISPLAY_NAME,   // 4
            Calendars.TIMEZONE,       // 5
            Calendars.SYNC_EVENTS     // 6
    };

    // Counters for sync event logging
    private static int mServerDiffs;
    private static int mRefresh;

    protected CalendarSyncAdapter(Context context, SyncableContentProvider provider) {
        super(context, provider);
        mCalendarClient = new CalendarClient(
                new AndroidGDataClient(context, USER_AGENT_APP_VERSION),
                new XmlCalendarGDataParserFactory(new AndroidXmlParserFactory()));
    }

    @Override
    protected Object createSyncInfo() {
        return new SyncInfo();
    }

    @Override
    protected Entry newEntry() {
        return new EventEntry();
    }

    @Override
    protected Cursor getCursorForTable(ContentProvider cp, Class entryClass) {
        if (entryClass != EventEntry.class) {
            throw new IllegalArgumentException("unexpected entry class, " + entryClass.getName());
        }
        return cp.query(Calendar.Events.CONTENT_URI, null, null, null, null);
    }

    @Override
    protected Cursor getCursorForDeletedTable(ContentProvider cp, Class entryClass) {
        if (entryClass != EventEntry.class) {
            throw new IllegalArgumentException("unexpected entry class, " + entryClass.getName());
        }
        return cp.query(Calendar.Events.DELETED_CONTENT_URI, null, null, null, null);
    }

    @Override
    protected String cursorToEntry(SyncContext context, Cursor c, Entry entry,
            Object info) throws ParseException {
        EventEntry event = (EventEntry) entry;
        SyncInfo syncInfo = (SyncInfo) info;

        String feedUrl = c.getString(c.getColumnIndex(Calendars.URL));

        // update the sync info.  this will be used later when we update the
        // provider with the results of sending this entry to the calendar
        // server.
        syncInfo.calendarId = c.getLong(c.getColumnIndex(Events.CALENDAR_ID));
        syncInfo.calendarTimezone =
                c.getString(c.getColumnIndex(Events.EVENT_TIMEZONE));
        if (TextUtils.isEmpty(syncInfo.calendarTimezone)) {
            // if the event timezone is not set -- e.g., when we're creating an
            // event on the device -- we will use the timezone for the calendar.
            syncInfo.calendarTimezone =
                    c.getString(c.getColumnIndex(Events.TIMEZONE));
        }

        // id
        event.setId(c.getString(c.getColumnIndex(Events._SYNC_ID)));
        event.setEditUri(c.getString(c.getColumnIndex(Events._SYNC_VERSION)));

        // status
        byte status;
        int localStatus = c.getInt(c.getColumnIndex(Events.STATUS));
        switch (localStatus) {
            case Events.STATUS_CANCELED:
                status = EventEntry.STATUS_CANCELED;
                break;
            case Events.STATUS_CONFIRMED:
                status = EventEntry.STATUS_CONFIRMED;
                break;
            case Events.STATUS_TENTATIVE:
                status = EventEntry.STATUS_TENTATIVE;
                break;
            default:
                // should not happen
                status = EventEntry.STATUS_TENTATIVE;
                break;
        }
        event.setStatus(status);

        // visibility
        byte visibility;
        int localVisibility = c.getInt(c.getColumnIndex(Events.VISIBILITY));
        switch (localVisibility) {
            case Events.VISIBILITY_DEFAULT:
                visibility = EventEntry.VISIBILITY_DEFAULT;
                break;
            case Events.VISIBILITY_CONFIDENTIAL:
                visibility = EventEntry.VISIBILITY_CONFIDENTIAL;
                break;
            case Events.VISIBILITY_PRIVATE:
                visibility = EventEntry.VISIBILITY_PRIVATE;
                break;
            case Events.VISIBILITY_PUBLIC:
                visibility = EventEntry.VISIBILITY_PUBLIC;
                break;
            default:
                // should not happen
                Log.e(TAG, "Unexpected value for visibility: " + localVisibility
                        + "; using default visibility.");
                visibility = EventEntry.VISIBILITY_DEFAULT;
                break;
        }
        event.setVisibility(visibility);

        byte transparency;
        int localTransparency = c.getInt(c.getColumnIndex(Events.TRANSPARENCY));
        switch (localTransparency) {
            case Events.TRANSPARENCY_OPAQUE:
                transparency = EventEntry.TRANSPARENCY_OPAQUE;
                break;
            case Events.TRANSPARENCY_TRANSPARENT:
                transparency = EventEntry.TRANSPARENCY_TRANSPARENT;
                break;
            default:
                // should not happen
                Log.e(TAG, "Unexpected value for transparency: " + localTransparency
                        + "; using opaque transparency.");
                transparency = EventEntry.TRANSPARENCY_OPAQUE;
                break;
        }
        event.setTransparency(transparency);

        // could set the html uri, but there's no need to, since it should not be edited.

        // title
        event.setTitle(c.getString(c.getColumnIndex(Events.TITLE)));

        // description
        event.setContent(c.getString(c.getColumnIndex(Events.DESCRIPTION)));

        // where
        event.setWhere(c.getString(c.getColumnIndex(Events.EVENT_LOCATION)));

        // attendees
        long eventId = c.getInt(c.getColumnIndex(Events._SYNC_LOCAL_ID));
        addAttendeesToEntry(eventId, event);

        // comment uri
        event.setCommentsUri(c.getString(c.getColumnIndexOrThrow(Events.COMMENTS_URI)));

        Time utc = new Time(Time.TIMEZONE_UTC);

        boolean allDay = c.getInt(c.getColumnIndex(Events.ALL_DAY)) != 0;

        String startTime = null;
        String endTime = null;
        // start time
        int dtstartColumn = c.getColumnIndex(Events.DTSTART);
        if (!c.isNull(dtstartColumn)) {
            long dtstart = c.getLong(dtstartColumn);
            utc.set(dtstart);
            startTime = utc.format3339(allDay);
        }

        // end time
        int dtendColumn = c.getColumnIndex(Events.DTEND);
        if (!c.isNull(dtendColumn)) {
            long dtend = c.getLong(dtendColumn);
            utc.set(dtend);
            endTime = utc.format3339(allDay);
        }

        When when = new When(startTime, endTime);
        event.addWhen(when);

        // reminders
        Integer hasReminder = c.getInt(c.getColumnIndex(Events.HAS_ALARM));
        if (hasReminder != null && hasReminder.intValue() != 0) {
            addRemindersToEntry(eventId, event);
        }

        // extendedProperties
        Integer hasExtendedProperties = c.getInt(c.getColumnIndex(Events.HAS_EXTENDED_PROPERTIES));
        if (hasExtendedProperties != null && hasExtendedProperties.intValue() != 0) {
            addExtendedPropertiesToEntry(eventId, event);
        }

        long originalStartTime = -1;
        String originalId = c.getString(c.getColumnIndex(Events.ORIGINAL_EVENT));
        int originalStartTimeIndex = c.getColumnIndex(Events.ORIGINAL_INSTANCE_TIME);
        if (!c.isNull(originalStartTimeIndex)) {
            originalStartTime = c.getLong(originalStartTimeIndex);
        }
        if ((originalStartTime != -1) && !TextUtils.isEmpty(originalId)) {
            // We need to use the "originalAllDay" field for the original event
            // in order to format the "originalStartTime" correctly.
            boolean originalAllDay = c.getInt(c.getColumnIndex(Events.ORIGINAL_ALL_DAY)) != 0;

            Time originalTime = new Time(c.getString(c.getColumnIndex(Events.EVENT_TIMEZONE)));
            originalTime.set(originalStartTime);

            utc.set(originalStartTime);
            event.setOriginalEventStartTime(utc.format3339(originalAllDay));
            event.setOriginalEventId(originalId);
        }

        // recurrences.
        ICalendar.Component component = new ICalendar.Component("DUMMY",
                null /* parent */);
        if (RecurrenceSet.populateComponent(c, component)) {
            addRecurrenceToEntry(component, event);
        }

        // if this is a new entry, return the feed url.  otherwise, return null; the edit url is
        // already in the entry.
        if (event.getEditUri() == null) {
            return feedUrl;
        } else {
            return null;
        }
    }

    private void addAttendeesToEntry(long eventId, EventEntry event)
            throws ParseException {
        Cursor c = getContext().getContentResolver().query(
                Calendar.Attendees.CONTENT_URI, null, "event_id=" + eventId, null, null);

        try {
            int nameIndex = c.getColumnIndexOrThrow(Calendar.Attendees.ATTENDEE_NAME);
            int emailIndex = c.getColumnIndexOrThrow(Calendar.Attendees.ATTENDEE_EMAIL);
            int statusIndex = c.getColumnIndexOrThrow(Calendar.Attendees.ATTENDEE_STATUS);
            int typeIndex = c.getColumnIndexOrThrow(Calendar.Attendees.ATTENDEE_TYPE);
            int relIndex = c.getColumnIndexOrThrow(Calendar.Attendees.ATTENDEE_RELATIONSHIP);



            while (c.moveToNext()) {
                Who who = new Who();
                who.setValue(c.getString(nameIndex));
                who.setEmail(c.getString(emailIndex));
                int status = c.getInt(statusIndex);
                switch (status) {
                    case Calendar.Attendees.ATTENDEE_STATUS_NONE:
                        who.setStatus(Who.STATUS_NONE);
                        break;
                    case Calendar.Attendees.ATTENDEE_STATUS_ACCEPTED:
                        who.setStatus(Who.STATUS_ACCEPTED);
                        break;
                    case Calendar.Attendees.ATTENDEE_STATUS_DECLINED:
                        who.setStatus(Who.STATUS_DECLINED);
                        break;
                    case Calendar.Attendees.ATTENDEE_STATUS_INVITED:
                        who.setStatus(Who.STATUS_INVITED);
                        break;
                    case Calendar.Attendees.ATTENDEE_STATUS_TENTATIVE:
                        who.setStatus(Who.STATUS_TENTATIVE);
                        break;
                    default:
                        Log.e(TAG, "Unknown attendee status: " + status);
                        who.setStatus(Who.STATUS_NONE);
                        break;
                }
                int type = c.getInt(typeIndex);
                switch (type) {
                    case Calendar.Attendees.TYPE_NONE:
                        who.setType(Who.TYPE_NONE);
                        break;
                    case Calendar.Attendees.TYPE_REQUIRED:
                        who.setType(Who.TYPE_REQUIRED);
                        break;
                    case Calendar.Attendees.TYPE_OPTIONAL:
                        who.setType(Who.TYPE_OPTIONAL);
                        break;
                    default:
                        Log.e(TAG, "Unknown attendee type: " + type);
                        who.setType(Who.TYPE_NONE);
                        break;
                }
                int rel = c.getInt(relIndex);
                switch (rel) {
                    case Calendar.Attendees.RELATIONSHIP_NONE:
                        who.setRelationship(Who.RELATIONSHIP_NONE);
                        break;
                    case Calendar.Attendees.RELATIONSHIP_ATTENDEE:
                        who.setRelationship(Who.RELATIONSHIP_ATTENDEE);
                        break;
                    case Calendar.Attendees.RELATIONSHIP_ORGANIZER:
                        who.setRelationship(Who.RELATIONSHIP_ORGANIZER);
                        break;
                    case Calendar.Attendees.RELATIONSHIP_SPEAKER:
                        who.setRelationship(Who.RELATIONSHIP_SPEAKER);
                        break;
                    case Calendar.Attendees.RELATIONSHIP_PERFORMER:
                        who.setRelationship(Who.RELATIONSHIP_PERFORMER);
                        break;
                    default:
                        Log.e(TAG, "Unknown attendee relationship: " + rel);
                        who.setRelationship(Who.RELATIONSHIP_NONE);
                        break;
                }
                event.addAttendee(who);
            }
        } finally {
            c.close();
        }
    }

    private void addRemindersToEntry(long eventId, EventEntry event)
            throws ParseException {
        Cursor c = getContext().getContentResolver().query(
                Calendar.Reminders.CONTENT_URI, null,
                "event_id=" + eventId, null, null);

        try {
            int methodIndex = c.getColumnIndex(Calendar.Reminders.METHOD);
            int minutesIndex = c.getColumnIndex(Calendar.Reminders.MINUTES);

            while (c.moveToNext()) {
                Reminder reminder = new Reminder();
                reminder.setMinutes(c.getInt(minutesIndex));
                int method = c.getInt(methodIndex);
                switch(method) {
                    case Calendar.Reminders.METHOD_DEFAULT:
                        reminder.setMethod(Reminder.METHOD_DEFAULT);
                        break;
                    case Calendar.Reminders.METHOD_ALERT:
                        reminder.setMethod(Reminder.METHOD_ALERT);
                        break;
                    case Calendar.Reminders.METHOD_EMAIL:
                        reminder.setMethod(Reminder.METHOD_EMAIL);
                        break;
                    case Calendar.Reminders.METHOD_SMS:
                        reminder.setMethod(Reminder.METHOD_SMS);
                        break;
                    default:
                        throw new ParseException("illegal method, " + method);
                }
                event.addReminder(reminder);
            }
        } finally {
            c.close();
        }
    }

    private void addExtendedPropertiesToEntry(long eventId, EventEntry event)
            throws ParseException {
        Cursor c = getContext().getContentResolver().query(
                Calendar.ExtendedProperties.CONTENT_URI, null,
                "event_id=" + eventId, null, null);

        try {
            int nameIndex = c.getColumnIndex(Calendar.ExtendedProperties.NAME);
            int valueIndex = c.getColumnIndex(Calendar.ExtendedProperties.VALUE);

            while (c.moveToNext()) {
                String name = c.getString(nameIndex);
                String value = c.getString(valueIndex);
                event.addExtendedProperty(name, value);
            }
        } finally {
            c.close();
        }
    }

    private void addRecurrenceToEntry(ICalendar.Component component,
            EventEntry event) {
        // serialize the component into a Google Calendar recurrence string
        // we don't serialize the entire component, since we have a dummy
        // wrapper (BEGIN:DUMMY, END:DUMMY).
        StringBuilder sb = new StringBuilder();

        // append the properties
        boolean first = true;
        for (String propertyName : component.getPropertyNames()) {
            for (ICalendar.Property property :
                    component.getProperties(propertyName)) {
                if (first) {
                    first = false;
                } else {
                    sb.append("\n");
                }
                property.toString(sb);
            }
        }

        // append the sub-components
        List<ICalendar.Component> children = component.getComponents();
        if (children != null) {
            for (ICalendar.Component child : children) {
                if (first) {
                    first = false;
                } else {
                    sb.append("\n");
                }
                child.toString(sb);
            }
        }
        event.setRecurrence(sb.toString());
    }

    @Override
    protected void deletedCursorToEntry(SyncContext context, Cursor c, Entry entry) {
        EventEntry event = (EventEntry) entry;
        event.setId(c.getString(c.getColumnIndex(Events._SYNC_ID)));
        event.setEditUri(c.getString(c.getColumnIndex(Events._SYNC_VERSION)));
        event.setStatus(EventEntry.STATUS_CANCELED);
    }

    protected boolean handleAllDeletedUnavailable(GDataSyncData syncData, String feed) {
        syncData.feedData.remove(feed);
        getContext().getContentResolver().delete(Calendar.Calendars.CONTENT_URI,
                Calendar.Calendars._SYNC_ACCOUNT + "=? AND " + Calendar.Calendars.URL + "=?",
                new String[]{getAccount(), feed});
        return true;
    }

    @Override
    public void onSyncStarting(SyncContext context, String account, boolean forced,
            SyncResult result) {
        mContentResolver = getContext().getContentResolver();
        mServerDiffs = 0;
        mRefresh = 0;
        super.onSyncStarting(context, account, forced, result);
    }

    private void deletedEntryToContentValues(Long syncLocalId, EventEntry event,
            ContentValues values) {
        // see #deletedCursorToEntry.  this deletion cannot be an exception to a recurrence (e.g.,
        // deleting an instance of a repeating event) -- new recurrence exceptions would be
        // insertions.
        values.clear();

        // Base sync info
        values.put(Events._SYNC_LOCAL_ID, syncLocalId);
        values.put(Events._SYNC_ID, event.getId());
        values.put(Events._SYNC_VERSION, event.getEditUri());
    }

    /**
     * Clear out the map and stuff an Entry into it in a format that can
     * be inserted into a content provider.
     *
     * If a date is before 1970 or past 2038, ENTRY_INVALID is returned, and DTSTART
     * is set to -1.  This is due to the current 32-bit time restriction and
     * will be fixed in a future release.
     *
     * @return ENTRY_OK, ENTRY_DELETED, or ENTRY_INVALID
     */
    private int entryToContentValues(EventEntry event, Long syncLocalId,
            ContentValues map, Object info) {
        SyncInfo syncInfo = (SyncInfo) info;

        // There are 3 cases for parsing a date-time string:
        //
        // 1. The date-time string specifies a date and time with a time offset.
        //    (The "normal" format.)
        // 2. The date-time string is just a date, used for all-day events,
        //    with no time or time offset fields. (The "all-day" format.)
        // 3. The date-time string specifies a date and time, but no time
        //    offset.  (The "floating" format, not supported yet.)
        //
        // Case 1: Time.parse3339() converts the date-time string to UTC and
        // sets the Time.timezone to UTC.  It does not matter what the initial
        // Time.timezone field was set to.  The initial timezone is ignored.
        //
        // Case 2: The date-time string doesn't specify the time.
        // Time.parse3339() just sets the date but not the time (hour, minute,
        // second) fields.  (The time fields should be zero, meaning midnight.)
        // This code then sets the timezone to UTC (because this is an all-day
        // event).  It does not matter in this case either what the initial
        // Time.timezone field was set to.
        //
        // Case 3: This is a "floating time" (which we do not support yet).
        // In this case, it will matter what the initial Time.timezone is set
        // to.  It should use UTC.  If I specify a floating time of 1pm then I
        // want that event displayed at 1pm in every timezone.  The easiest way
        // to support this would be store it as 1pm in UTC and mark the event
        // as "isFloating" (with a new database column).  Then when displaying
        // the event, the code checks "isFloating" and just leaves the time at
        // 1pm without doing any conversion to the local timezone.
        //
        // So in all cases, it is correct to set the Time.timezone to UTC.
        Time time = new Time(Time.TIMEZONE_UTC);

        map.clear();

        // Base sync info
        map.put(Events._SYNC_ID, event.getId());
        String version = event.getEditUri();
        if (!StringUtils.isEmpty(version)) {
            // Always rewrite the edit URL to https for dasher account to avoid
            // redirection.
            map.put(Events._SYNC_VERSION, rewriteUrlforAccount(getAccount(), version));
        }

        // see if this is an exception to an existing event/recurrence.
        String originalId = event.getOriginalEventId();
        String originalStartTime = event.getOriginalEventStartTime();
        boolean isRecurrenceException = false;
        if (!StringUtils.isEmpty(originalId) && !StringUtils.isEmpty(originalStartTime)) {
            isRecurrenceException = true;
            time.parse3339(originalStartTime);
            map.put(Events.ORIGINAL_EVENT, originalId);
            map.put(Events.ORIGINAL_INSTANCE_TIME, time.toMillis(false /* use isDst */));
            map.put(Events.ORIGINAL_ALL_DAY, time.allDay ? 1 : 0);
        }

        // Event status
        byte status = event.getStatus();
        switch (status) {
            case EventEntry.STATUS_CANCELED:
                if (!isRecurrenceException) {
                    return ENTRY_DELETED;
                }
                map.put(Events.STATUS, sCanceledStatus);
                break;
            case EventEntry.STATUS_TENTATIVE:
                map.put(Events.STATUS, sTentativeStatus);
                break;
            case EventEntry.STATUS_CONFIRMED:
                map.put(Events.STATUS, sConfirmedStatus);
                break;
            default:
                // should not happen
                return ENTRY_INVALID;
        }

        map.put(Events._SYNC_LOCAL_ID, syncLocalId);

        // Updated time, only needed for non-deleted items
        String updated = event.getUpdateDate();
        map.put(Events._SYNC_TIME, updated);
        map.put(Events._SYNC_DIRTY, 0);

        // visibility
        switch (event.getVisibility()) {
            case EventEntry.VISIBILITY_DEFAULT:
                map.put(Events.VISIBILITY,  Events.VISIBILITY_DEFAULT);
                break;
            case EventEntry.VISIBILITY_CONFIDENTIAL:
                map.put(Events.VISIBILITY,  Events.VISIBILITY_CONFIDENTIAL);
                break;
            case EventEntry.VISIBILITY_PRIVATE:
                map.put(Events.VISIBILITY,  Events.VISIBILITY_PRIVATE);
                break;
            case EventEntry.VISIBILITY_PUBLIC:
                map.put(Events.VISIBILITY,  Events.VISIBILITY_PUBLIC);
                break;
            default:
                // should not happen
                Log.e(TAG, "Unexpected visibility " + event.getVisibility());
                return ENTRY_INVALID;
        }

        // transparency
        switch (event.getTransparency()) {
            case EventEntry.TRANSPARENCY_OPAQUE:
                map.put(Events.TRANSPARENCY,  Events.TRANSPARENCY_OPAQUE);
                break;
            case EventEntry.TRANSPARENCY_TRANSPARENT:
                map.put(Events.TRANSPARENCY,  Events.TRANSPARENCY_TRANSPARENT);
                break;
            default:
                // should not happen
                Log.e(TAG, "Unexpected transparency " + event.getTransparency());
                return ENTRY_INVALID;
        }

        // html uri
        String htmlUri = event.getHtmlUri();
        if (!StringUtils.isEmpty(htmlUri)) {
            // TODO: convert this desktop url into a mobile one?
            // htmlUri = htmlUri.replace("/event?", "/mevent?"); // but a little more robust
            map.put(Events.HTML_URI, htmlUri);
        }

        // title
        String title = event.getTitle();
        if (!StringUtils.isEmpty(title)) {
            map.put(Events.TITLE, title);
        }

        // content
        String content = event.getContent();
        if (!StringUtils.isEmpty(content)) {
            map.put(Events.DESCRIPTION, content);
        }

        // where
        String where = event.getWhere();
        if (!StringUtils.isEmpty(where)) {
            map.put(Events.EVENT_LOCATION, where);
        }

        // Calendar ID
        map.put(Events.CALENDAR_ID, syncInfo.calendarId);

        // comments uri
        String commentsUri = event.getCommentsUri();
        if (commentsUri != null) {
            map.put(Events.COMMENTS_URI, commentsUri);
        }

        boolean timesSet = false;

        // see if there are any reminders for this event
        if (event.getReminders() != null) {
            // just store that we have reminders.  the caller will have
            // to update the reminders table separately.
            map.put(Events.HAS_ALARM, 1);
        }

        // see if there are any extended properties for this event
        if (event.getExtendedProperties() != null) {
            // just store that we have extended properties.  the caller will have
            // to update the extendedproperties table separately.
            map.put(Events.HAS_EXTENDED_PROPERTIES, 1);
        }

        // dtstart & dtend
        When when = event.getFirstWhen();
        if (when != null) {
            String startTime = when.getStartTime();
            if (!StringUtils.isEmpty(startTime)) {
                time.parse3339(startTime);

                // we also stash away the event's timezone.
                // this timezone might get overwritten below, if this event is
                // a recurrence (recurrences are defined in terms of the
                // timezone of the creator of the event).
                // note that we treat all day events as occurring in the UTC timezone, so
                // an event on 05/08/2007 occurs on 05/08/2007, no matter what timezone the device
                // is in.
                // TODO: handle the "floating" timezone.
                if (time.allDay) {
                    map.put(Events.ALL_DAY, 1);
                    map.put(Events.EVENT_TIMEZONE, Time.TIMEZONE_UTC);
                } else {
                    map.put(Events.EVENT_TIMEZONE, syncInfo.calendarTimezone);
                }

                long dtstart = time.toMillis(false /* use isDst */);
                if (dtstart < 0) {
                    if (Config.LOGD) {
                        Log.d(TAG, "dtstart out of range: " + startTime);
                    }
                    map.put(Events.DTSTART, -1);  // Flag to caller that date is out of range
                    return ENTRY_INVALID;
                }
                map.put(Events.DTSTART, dtstart);

                timesSet = true;
            }

            String endTime = when.getEndTime();
            if (!StringUtils.isEmpty(endTime)) {
                time.parse3339(endTime);
                long dtend = time.toMillis(false /* use isDst */);
                if (dtend < 0) {
                    if (Config.LOGD) {
                        Log.d(TAG, "dtend out of range: " + endTime);
                    }
                    map.put(Events.DTSTART, -1);  // Flag to caller that date is out of range
                    return ENTRY_INVALID;
                }
                map.put(Events.DTEND, dtend);
            }
        }

        // rrule
        String recurrence = event.getRecurrence();
        if (!TextUtils.isEmpty(recurrence)) {
            ICalendar.Component recurrenceComponent =
                    new ICalendar.Component("DUMMY", null /* parent */);
            ICalendar ical = null;
            try {
                ICalendar.parseComponent(recurrenceComponent, recurrence);
            } catch (ICalendar.FormatException fe) {
                if (Config.LOGD) {
                    Log.d(TAG, "Unable to parse recurrence: " + recurrence);
                }
                return ENTRY_INVALID;
            }

            if (!RecurrenceSet.populateContentValues(recurrenceComponent, map)) {
                return ENTRY_INVALID;
            }

            timesSet = true;
        }

        if (!timesSet) {
            return ENTRY_INVALID;
        }

        map.put(SyncConstValue._SYNC_ACCOUNT, getAccount());
        return ENTRY_OK;
    }

    public void updateProvider(Feed feed,
            Long syncLocalId, Entry entry,
            ContentProvider provider, Object info) throws ParseException {
        SyncInfo syncInfo = (SyncInfo) info;
        EventEntry event = (EventEntry) entry;

        ContentValues map = new ContentValues();

        // use the calendar's timezone, if provided in the feed.
        // this overwrites whatever was in the db.
        if ((feed != null) && (feed instanceof EventsFeed)) {
            EventsFeed eventsFeed = (EventsFeed) feed;
            syncInfo.calendarTimezone = eventsFeed.getTimezone();
        }

        if (entry.isDeleted()) {
            deletedEntryToContentValues(syncLocalId, event, map);
            if (Config.LOGV) {
                Log.v(TAG, "Deleting entry: " + map);
            }
            provider.insert(Events.DELETED_CONTENT_URI, map);
            return;
        }

        int entryState = entryToContentValues(event, syncLocalId, map, syncInfo);

        if (entryState == ENTRY_DELETED) {
            if (Config.LOGV) {
                Log.v(TAG, "Got deleted entry from server: "
                        + map);
            }
            provider.insert(Events.DELETED_CONTENT_URI, map);
        } else if (entryState == ENTRY_OK) {
            if (Config.LOGV) {
                Log.v(TAG, "Got entry from server: " + map);
            }
            Uri result = provider.insert(Events.CONTENT_URI, map);
            long rowId = ContentUris.parseId(result);
            // handle the reminders for the event
            Integer hasAlarm = map.getAsInteger(Events.HAS_ALARM);
            if (hasAlarm != null && hasAlarm == 1) {
                // reminders should not be null
                Vector alarms = event.getReminders();
                if (alarms == null) {
                    Log.e(TAG, "Have an alarm but do not have any reminders "
                            + "-- should not happen.");
                    throw new IllegalStateException("Have an alarm but do not have any reminders");
                }
                Enumeration reminders = alarms.elements();
                while (reminders.hasMoreElements()) {
                    ContentValues reminderValues = new ContentValues();
                    reminderValues.put(Calendar.Reminders.EVENT_ID, rowId);

                    Reminder reminder = (Reminder) reminders.nextElement();
                    byte method = reminder.getMethod();
                    switch (method) {
                        case Reminder.METHOD_DEFAULT:
                            reminderValues.put(Calendar.Reminders.METHOD,
                                    Calendar.Reminders.METHOD_DEFAULT);
                            break;
                        case Reminder.METHOD_ALERT:
                            reminderValues.put(Calendar.Reminders.METHOD,
                                    Calendar.Reminders.METHOD_ALERT);
                            break;
                        case Reminder.METHOD_EMAIL:
                            reminderValues.put(Calendar.Reminders.METHOD,
                                    Calendar.Reminders.METHOD_EMAIL);
                            break;
                        case Reminder.METHOD_SMS:
                            reminderValues.put(Calendar.Reminders.METHOD,
                                    Calendar.Reminders.METHOD_SMS);
                            break;
                        default:
                            // should not happen.  return false?  we'd have to
                            // roll back the event.
                            Log.e(TAG, "Unknown reminder method: " + method
                                    + " should not happen!");
                    }

                    int minutes = reminder.getMinutes();
                    reminderValues.put(Calendar.Reminders.MINUTES,
                            minutes == Reminder.MINUTES_DEFAULT ?
                                    Calendar.Reminders.MINUTES_DEFAULT :
                                    minutes);

                    if (provider.insert(Calendar.Reminders.CONTENT_URI,
                            reminderValues) == null) {
                        throw new ParseException("Unable to insert reminders.");
                    }
                }
            }

            // handle attendees for the event
            Vector attendees = event.getAttendees();
            Enumeration attendeesEnum = attendees.elements();
            while (attendeesEnum.hasMoreElements()) {
                Who who = (Who) attendeesEnum.nextElement();
                ContentValues attendeesValues = new ContentValues();
                attendeesValues.put(Calendar.Attendees.EVENT_ID, rowId);
                attendeesValues.put(Calendar.Attendees.ATTENDEE_NAME, who.getValue());
                attendeesValues.put(Calendar.Attendees.ATTENDEE_EMAIL, who.getEmail());

                byte status;
                switch (who.getStatus()) {
                    case Who.STATUS_NONE:
                        status = Calendar.Attendees.ATTENDEE_STATUS_NONE;
                        break;
                    case Who.STATUS_INVITED:
                        status = Calendar.Attendees.ATTENDEE_STATUS_INVITED;
                        break;
                    case Who.STATUS_ACCEPTED:
                        status = Calendar.Attendees.ATTENDEE_STATUS_ACCEPTED;
                        break;
                    case Who.STATUS_TENTATIVE:
                        status = Calendar.Attendees.ATTENDEE_STATUS_TENTATIVE;
                        break;
                    case Who.STATUS_DECLINED:
                        status = Calendar.Attendees.ATTENDEE_STATUS_DECLINED;
                        break;
                    default:
                        Log.w(TAG, "Unknown attendee status " + who.getStatus());
                        status = Calendar.Attendees.ATTENDEE_STATUS_NONE;
                }
                attendeesValues.put(Calendar.Attendees.ATTENDEE_STATUS, status);
                byte rel;
                switch (who.getRelationship()) {
                    case Who.RELATIONSHIP_NONE:
                        rel = Calendar.Attendees.RELATIONSHIP_NONE;
                        break;
                    case Who.RELATIONSHIP_ORGANIZER:
                        rel = Calendar.Attendees.RELATIONSHIP_ORGANIZER;
                        break;
                    case Who.RELATIONSHIP_ATTENDEE:
                        rel = Calendar.Attendees.RELATIONSHIP_ATTENDEE;
                        break;
                    case Who.RELATIONSHIP_PERFORMER:
                        rel = Calendar.Attendees.RELATIONSHIP_PERFORMER;
                        break;
                    case Who.RELATIONSHIP_SPEAKER:
                        rel = Calendar.Attendees.RELATIONSHIP_SPEAKER;
                        break;
                    default:
                        Log.w(TAG, "Unknown attendee relationship " + who.getRelationship());
                        rel = Calendar.Attendees.RELATIONSHIP_NONE;
                }

                attendeesValues.put(Calendar.Attendees.ATTENDEE_RELATIONSHIP, rel);

                byte type;
                switch (who.getType()) {
                    case Who.TYPE_NONE:
                        type = Calendar.Attendees.TYPE_NONE;
                        break;
                    case Who.TYPE_REQUIRED:
                        type = Calendar.Attendees.TYPE_REQUIRED;
                        break;
                    case Who.TYPE_OPTIONAL:
                        type = Calendar.Attendees.TYPE_OPTIONAL;
                        break;
                    default:
                        Log.w(TAG, "Unknown attendee type " + who.getType());
                        type = Calendar.Attendees.TYPE_NONE;
                }
                attendeesValues.put(Calendar.Attendees.ATTENDEE_TYPE, type);
                if (provider.insert(Calendar.Attendees.CONTENT_URI, attendeesValues) == null) {
                    throw new ParseException("Unable to insert attendees.");
                }
            }

            // handle the extended properties for the event
            Integer hasExtendedProperties = map.getAsInteger(Events.HAS_EXTENDED_PROPERTIES);
            if (hasExtendedProperties != null && hasExtendedProperties.intValue() != 0) {
                // extended properties should not be null
                // TODO: make the extended properties a bit more OO?
                Hashtable extendedProperties = event.getExtendedProperties();
                if (extendedProperties == null) {
                    Log.e(TAG, "Have extendedProperties but do not have any properties"
                            + "-- should not happen.");
                    throw new IllegalStateException(
                            "Have extendedProperties but do not have any properties");
                }
                Enumeration propertyNames = extendedProperties.keys();
                while (propertyNames.hasMoreElements()) {
                    String propertyName = (String) propertyNames.nextElement();
                    String propertyValue = (String) extendedProperties.get(propertyName);
                    ContentValues extendedPropertyValues = new ContentValues();
                    extendedPropertyValues.put(Calendar.ExtendedProperties.EVENT_ID, rowId);
                    extendedPropertyValues.put(Calendar.ExtendedProperties.NAME,
                            propertyName);
                    extendedPropertyValues.put(Calendar.ExtendedProperties.VALUE,
                            propertyValue);
                    if (provider.insert(Calendar.ExtendedProperties.CONTENT_URI,
                            extendedPropertyValues) == null) {
                        throw new ParseException("Unable to insert extended properties.");
                    }
                }
            }
        } else {
            // If the DTSTART == -1, then the date was out of range.  We don't
            // need to throw a ParseException because the user can create
            // dates on the web that we can't handle on the phone.  For
            // example, events with dates before Dec 13, 1901 can be created
            // on the web but cannot be handled on the phone.
            Long dtstart = map.getAsLong(Events.DTSTART);
            if (dtstart != null && dtstart == -1) {
                return;
            }

            if (Config.LOGV) {
                Log.v(TAG, "Got invalid entry from server: " + map);
            }
            throw new ParseException("Got invalid entry from server: " + map);
        }
    }

    @Override
    public void getServerDiffs(SyncContext context,
            SyncData baseSyncData, SyncableContentProvider tempProvider,
            Bundle extras, Object baseSyncInfo, SyncResult syncResult) {
        final ContentResolver cr = getContext().getContentResolver();
        mServerDiffs++;
        final boolean syncingSingleFeed = (extras != null) && extras.containsKey("feed");
        if (syncingSingleFeed) {
            String feedUrl = extras.getString("feed");
            getServerDiffsForFeed(context, baseSyncData, tempProvider, feedUrl,
                    baseSyncInfo, syncResult);
            return;
        }

        // select the set of calendars for this account.
        Cursor cursor = cr.query(Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION, SELECT_BY_ACCOUNT,
                new String[] { getAccount() }, null /* sort order */);

        Bundle syncExtras = new Bundle();

        boolean refreshCalendars = true;

        try {
            while (cursor.moveToNext()) {
                boolean syncEvents = (cursor.getInt(6) == 1);
                String feedUrl = cursor.getString(3);

                if (!syncEvents) {
                    continue;
                }

                // since this is a poll (no specific feed selected), refresh the list of calendars.
                // we can move away from this when we move to the new allcalendars feed, which is
                // syncable.  until then, we'll rely on the daily poll to keep the list of calendars
                // up to date.
                if (refreshCalendars) {
                    mRefresh++;
                    context.setStatusText("Fetching list of calendars");
                    // get rid of the current cursor and fetch from the server.
                    cursor.close();
                    final String[] accountSelectionArgs = new String[]{getAccount()};
                    cursor = cr.query(
                        Calendar.Calendars.LIVE_CONTENT_URI, CALENDARS_PROJECTION,
                        SELECT_BY_ACCOUNT, accountSelectionArgs, null /* sort order */);
                    // start over with the loop
                    refreshCalendars = false;
                    continue;
                }

                // schedule syncs for each of these feeds.
                syncExtras.clear();
                syncExtras.putAll(extras);
                syncExtras.putString("feed", feedUrl);
                cr.startSync(Calendar.CONTENT_URI, syncExtras);
            }
        } finally {
            cursor.close();
        }
    }


    private void getServerDiffsForFeed(SyncContext context, SyncData baseSyncData,
            SyncableContentProvider tempProvider,
            String feed, Object baseSyncInfo, SyncResult syncResult) {
        final SyncInfo syncInfo = (SyncInfo) baseSyncInfo;
        final GDataSyncData syncData = (GDataSyncData) baseSyncData;

        Cursor cursor = getContext().getContentResolver().query(Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION, SELECT_BY_ACCOUNT_AND_FEED,
                new String[] { getAccount(), feed }, null /* sort order */);

        ContentValues map = new ContentValues();
        int maxResults = getMaxEntriesPerSync();

        try {
            if (!cursor.moveToFirst()) {
                return;
            }
            // TODO: refactor all of this, so we don't have to rely on
            // member variables getting updated here in order for the
            // base class hooks to work.

            syncInfo.calendarId = cursor.getLong(0);
            boolean syncEvents = (cursor.getInt(6) == 1);
            long syncTime = cursor.getLong(2);
            String feedUrl = cursor.getString(3);
            String name = cursor.getString(4);
            String origCalendarTimezone =
                    syncInfo.calendarTimezone = cursor.getString(5);

            if (!syncEvents) {
                // should not happen.  non-syncable feeds should not be scheduled for syncs nor
                // should they get tickled.
                if (Log.isLoggable(TAG, Log.VERBOSE)) {
                    Log.v(TAG, "Ignoring sync request for non-syncable feed.");
                }
                return;
            }

            context.setStatusText("Syncing " + name);

            // call the superclass implementation to sync the current
            // calendar from the server.
            getServerDiffsImpl(context, tempProvider, getFeedEntryClass(), feedUrl, syncInfo,
                    maxResults, syncData, syncResult);
            if (mSyncCanceled || syncResult.hasError()) {
                return;
            }

            // update the timezone for this calendar if it changed
            if (!TextUtils.equals(syncInfo.calendarTimezone,
                    origCalendarTimezone)) {
                map.clear();
                map.put(Calendars.TIMEZONE, syncInfo.calendarTimezone);
                mContentResolver.update(
                        ContentUris.withAppendedId(Calendars.CONTENT_URI, syncInfo.calendarId),
                        map, null, null);
            }
        } finally {
            cursor.close();
        }
    }

    @Override
    protected void initTempProvider(SyncableContentProvider cp) {
        // TODO: don't use the real db's calendar id's.  create new ones locally and translate
        // during CalendarProvider's merge.
        
        // populate temp provider with calendar ids, so joins work.
        ContentValues map = new ContentValues();
        Cursor c = getContext().getContentResolver().query(
                Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION,
                SELECT_BY_ACCOUNT, new String[]{getAccount()}, null /* sort order */);
        final int idIndex = c.getColumnIndexOrThrow(Calendars._ID);
        final int urlIndex = c.getColumnIndexOrThrow(Calendars.URL);
        final int timezoneIndex = c.getColumnIndexOrThrow(Calendars.TIMEZONE);
        while (c.moveToNext()) {
            map.clear();
            map.put(Calendars._ID, c.getLong(idIndex));
            map.put(Calendars.URL, c.getString(urlIndex));
            map.put(Calendars.TIMEZONE, c.getString(timezoneIndex));
            cp.insert(Calendar.Calendars.CONTENT_URI, map);
        }
        c.close();
    }

    public void onAccountsChanged(String[] accountsArray) {
        if (!"yes".equals(SystemProperties.get("ro.config.sync"))) {
            return;
        }

        // - Get a cursor (A) over all selected calendars over all accounts
        // - Get a cursor (B) over all subscribed feeds for calendar
        // - If an item is in A but not B then add a subscription
        // - If an item is in B but not A then remove the subscription

        ContentResolver cr = getContext().getContentResolver();
        Cursor cursorA = null;
        Cursor cursorB = null;
        try {
            cursorA = Calendar.Calendars.query(cr, null /* projection */,
                    Calendar.Calendars.SELECTED + "=1", CALENDAR_KEY_SORT_ORDER);
            int urlIndexA = cursorA.getColumnIndexOrThrow(Calendar.Calendars.URL);
            int accountIndexA = cursorA.getColumnIndexOrThrow(Calendar.Calendars._SYNC_ACCOUNT);
            cursorB = SubscribedFeeds.Feeds.query(cr, FEEDS_KEY_COLUMNS,
                    SubscribedFeeds.Feeds.AUTHORITY + "=?", new String[]{Calendar.AUTHORITY},
                    FEEDS_KEY_SORT_ORDER);
            int urlIndexB = cursorB.getColumnIndexOrThrow(SubscribedFeeds.Feeds.FEED);
            int accountIndexB = cursorB.getColumnIndexOrThrow(SubscribedFeeds.Feeds._SYNC_ACCOUNT);
            for (CursorJoiner.Result joinerResult :
                    new CursorJoiner(cursorA, CALENDAR_KEY_COLUMNS, cursorB, FEEDS_KEY_COLUMNS)) {
                switch (joinerResult) {
                    case LEFT:
                        SubscribedFeeds.addFeed(
                                cr,
                                cursorA.getString(urlIndexA),
                                cursorA.getString(accountIndexA),
                                Calendar.AUTHORITY,
                                CalendarClient.SERVICE);
                        break;
                    case RIGHT:
                        SubscribedFeeds.deleteFeed(
                                cr,
                                cursorB.getString(urlIndexB),
                                cursorB.getString(accountIndexB),
                                Calendar.AUTHORITY);
                        break;
                    case BOTH:
                        // do nothing, since the subscription already exists
                        break;
                }
            }
        } finally {
            // check for null in case an exception occurred before the cursors got created
            if (cursorA != null) cursorA.close();
            if (cursorB != null) cursorB.close();
        }
    }

    /**
     * Should not get called.  The feed url changes depending on which calendar is being sync'd
     * to/from the device, and thus is determined and passed around as a local variable, where
     * appropriate.
     */
    protected String getFeedUrl(String account) {
        throw new UnsupportedOperationException("getFeedUrl() should not get called.");
    }

    protected Class getFeedEntryClass() {
        return EventEntry.class;
    }

    @Override
    protected void updateQueryParameters(QueryParams params) {
        if (params.getUpdatedMin() == null) {
            // if this is the first sync, only bother syncing starting from
            // one month ago.
            // TODO: remove this restriction -- we may want all of
            // historical calendar events.
            Time lastMonth = new Time(Time.TIMEZONE_UTC);
            lastMonth.setToNow();
            --lastMonth.month;
            lastMonth.normalize(true /* ignore isDst */);
            String startMin = lastMonth.format("%Y-%m-%dT%H:%M:%S.000Z");
            // TODO: move start-min to CalendarClient?
            // or create CalendarQueryParams subclass (extra class)?
            params.setParamValue("start-min", startMin);
            // HACK: specify that we want to expand recurrences ijn the past,
            // so the server does not expand any recurrences.  we do this to
            // avoid a large number of gd:when elements that we do not need,
            // since we process gd:recurrence elements instead.
            params.setParamValue("recurrence-expansion-start", "1970-01-01");
            params.setParamValue("recurrence-expansion-end", "1970-01-01");
        }
        // we want to get the events ordered by last modified, so we can
        // recover in case we cannot process the entire feed.
        params.setParamValue("orderby", "lastmodified");
        params.setParamValue("sortorder", "ascending");
    }

    @Override
    protected GDataServiceClient getGDataServiceClient() {
        return mCalendarClient;
    }

    protected void getStatsString(StringBuffer sb, SyncResult result) {
        super.getStatsString(sb, result);
        if (mRefresh > 0) {
            sb.append("F").append(mRefresh);
        }
        if (mServerDiffs > 0) {
            sb.append("s").append(mServerDiffs);
        }
    }
}
