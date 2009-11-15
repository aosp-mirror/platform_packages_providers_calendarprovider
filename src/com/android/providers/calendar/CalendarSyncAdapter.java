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

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AuthenticatorException;
import android.accounts.OperationCanceledException;
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
import android.graphics.Color;
import android.net.Uri;
import android.os.Bundle;
import android.os.SystemProperties;
import android.pim.ICalendar;
import android.pim.RecurrenceSet;
import android.provider.Calendar;
import android.provider.Calendar.Calendars;
import android.provider.Calendar.Events;
import android.provider.SubscribedFeeds;
import android.provider.SyncConstValue;
import android.provider.Settings;
import android.text.TextUtils;
import android.text.format.Time;
import android.util.Config;
import android.util.Log;
import com.google.android.gdata.client.AndroidGDataClient;
import com.google.android.gdata.client.AndroidXmlParserFactory;
import com.google.android.googlelogin.GoogleLoginServiceConstants;
import com.google.android.providers.AbstractGDataSyncAdapter;
import com.google.wireless.gdata.calendar.client.CalendarClient;
import com.google.wireless.gdata.calendar.data.CalendarEntry;
import com.google.wireless.gdata.calendar.data.CalendarsFeed;
import com.google.wireless.gdata.calendar.data.EventEntry;
import com.google.wireless.gdata.calendar.data.EventsFeed;
import com.google.wireless.gdata.calendar.data.Reminder;
import com.google.wireless.gdata.calendar.data.When;
import com.google.wireless.gdata.calendar.data.Who;
import com.google.wireless.gdata.calendar.parser.xml.XmlCalendarGDataParserFactory;
import com.google.wireless.gdata.client.GDataServiceClient;
import com.google.wireless.gdata.client.HttpException;
import com.google.wireless.gdata.client.QueryParams;
import com.google.wireless.gdata.data.Entry;
import com.google.wireless.gdata.data.Feed;
import com.google.wireless.gdata.data.StringUtils;
import com.google.wireless.gdata.parser.GDataParser;
import com.google.wireless.gdata.parser.ParseException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.Vector;
import java.net.URLDecoder;

/**
 * SyncAdapter for Google Calendar.  Fetches the list of the user's calendars,
 * and for each calendar that is marked as &quot;selected&quot; in the web
 * interface, syncs that calendar.
 */
public final class CalendarSyncAdapter extends AbstractGDataSyncAdapter {

    /* package */ static final String USER_AGENT_APP_VERSION = "Android-GData-Calendar/1.2";

    private static final String SELECT_BY_ACCOUNT =
            Calendars._SYNC_ACCOUNT + "=? AND " + Calendars._SYNC_ACCOUNT_TYPE + "=?";
    private static final String SELECT_BY_ACCOUNT_AND_FEED =
            SELECT_BY_ACCOUNT + " AND " + Calendars.URL + "=?";

    private static final String[] CALENDAR_KEY_COLUMNS =
            new String[]{Calendars._SYNC_ACCOUNT, Calendars._SYNC_ACCOUNT_TYPE, Calendars.URL};
    private static final String CALENDAR_KEY_SORT_ORDER =
            Calendars._SYNC_ACCOUNT + "," + Calendars._SYNC_ACCOUNT_TYPE + "," + Calendars.URL;
    private static final String[] FEEDS_KEY_COLUMNS =
            new String[]{SubscribedFeeds.Feeds._SYNC_ACCOUNT,
                    SubscribedFeeds.Feeds._SYNC_ACCOUNT_TYPE, SubscribedFeeds.Feeds.FEED};
    private static final String FEEDS_KEY_SORT_ORDER =
            SubscribedFeeds.Feeds._SYNC_ACCOUNT + ", " + SubscribedFeeds.Feeds._SYNC_ACCOUNT_TYPE
                    + ", " + SubscribedFeeds.Feeds.FEED;

    private static final String PRIVATE_FULL = "/private/full";
    private static final String FEEDS_SUBSTRING = "/feeds/";
    private static final String PRIVATE_FULL_SELFATTENDANCE = "/private/full-selfattendance";

    /** System property to enable sliding window sync **/
    private static final String USE_SLIDING_WINDOW = "sync.slidingwindows";

    private static final String HIDDEN_ATTENDEES_PROP =
        "com.android.providers.calendar.CalendarSyncAdapter#guests";

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
            Calendars.SYNC_EVENTS,    // 6
            Calendars.OWNER_ACCOUNT   // 7
    };

    // Counters for sync event logging
    private static int mServerDiffs;
    private static int mRefresh;

    /** These are temporary until a real policy is implemented. **/
    private static final long DAY_IN_MS = 86400000;
    private static final long MONTH_IN_MS = 2592000000L; // 30 days
    private static final long YEAR_IN_MS = 31600000000L; // approximately

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

        // has attendees data.  this is set to false if the proxy hid all of
        // the guests (see #entryToContentValues).  in that case, we switch
        // to the self attendance feed for updates.
        boolean hasAttendees = c.getInt(c.getColumnIndex(Events.HAS_ATTENDEE_DATA)) != 0;

        // id, edit uri.
        // these may need to get rewritten to a self attendance projection,
        // if our proxy server has removed guests (if there were to many)
        String id = c.getString(c.getColumnIndex(Events._SYNC_ID));
        String editUri = c.getString(c.getColumnIndex(Events._SYNC_VERSION));
        if (!hasAttendees) {
            if (id != null) id = convertProjectionToSelfAttendance(id);
            if (editUri != null) editUri = convertProjectionToSelfAttendance(editUri);
        }
        event.setId(id);
        event.setEditUri(editUri);

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

            String timezone = c.getString(c.getColumnIndex(Events.EVENT_TIMEZONE));
            if (TextUtils.isEmpty(timezone)) {
                timezone = TimeZone.getDefault().getID();
            }
            Time originalTime = new Time(timezone);
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

        // For now, always want to send event notifications
        event.setSendEventNotifications(true);

        event.setGuestsCanInviteOthers(
                c.getInt(c.getColumnIndex(Events.GUESTS_CAN_INVITE_OTHERS)) != 0);
        event.setGuestsCanModify(
                c.getInt(c.getColumnIndex(Events.GUESTS_CAN_MODIFY)) != 0);
        event.setGuestsCanSeeGuests(
                c.getInt(c.getColumnIndex(Events.GUESTS_CAN_SEE_GUESTS)) != 0);
        event.setOrganizer(c.getString(c.getColumnIndex(Events.ORGANIZER)));

        // if this is a new entry, return the feed url.  otherwise, return null; the edit url is
        // already in the entry.
        if (event.getEditUri() == null) {
            // we won't ever rewrite this to self attendance because this is a new event
            // (so if there are attendees, we need to use the full projection).
            return feedUrl;
        } else {
            return null;
        }
    }

    private String convertProjectionToSelfAttendance(String uri) {
        return uri.replace(PRIVATE_FULL, PRIVATE_FULL_SELFATTENDANCE);
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
        final Account account = getAccount();
        getContext().getContentResolver().delete(Calendar.Calendars.CONTENT_URI,
                Calendar.Calendars._SYNC_ACCOUNT + "=? AND "
                        + Calendar.Calendars._SYNC_ACCOUNT_TYPE + "=? AND "
                        + Calendar.Calendars.URL + "=?",
                new String[]{account.name, account.type, feed});
        return true;
    }

    @Override
    public void onSyncStarting(SyncContext context, Account account, boolean manualSync,
            SyncResult result) {
        mContentResolver = getContext().getContentResolver();
        mServerDiffs = 0;
        mRefresh = 0;
        super.onSyncStarting(context, account, manualSync, result);
    }

    public boolean getIsSyncable(Account account)
            throws IOException, AuthenticatorException, OperationCanceledException {
        Account[] accounts = AccountManager.get(getContext()).getAccountsByTypeAndFeatures(
                "com.google", new String[]{"legacy_hosted_or_google"}, null, null).getResult();
        return accounts.length > 0 && accounts[0].equals(account) && super.getIsSyncable(account);
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
        final Account account = getAccount();
        if (!StringUtils.isEmpty(version)) {
            // Always rewrite the edit URL to https for dasher account to avoid
            // redirection.
            map.put(Events._SYNC_VERSION, rewriteUrlforAccount(account, version));
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

        boolean hasAttendeeData = true;
        // see if there are any extended properties for this event
        if (event.getExtendedProperties() != null) {
            // first, intercept the proxy's hint that it has stripped attendees
            Hashtable props = event.getExtendedProperties();
            if (props.containsKey(HIDDEN_ATTENDEES_PROP) &&
                "hidden".equals(props.get(HIDDEN_ATTENDEES_PROP))) {
                props.remove(HIDDEN_ATTENDEES_PROP);
                hasAttendeeData = false;
            }
            // just store that we have extended properties.  the caller will have
            // to update the extendedproperties table separately.
            map.put(Events.HAS_EXTENDED_PROPERTIES, ((props.size() > 0) ? 1 : 0));
        }

        map.put(Events.HAS_ATTENDEE_DATA, hasAttendeeData ? 1 : 0);

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

        map.put(SyncConstValue._SYNC_ACCOUNT, account.name);
        map.put(SyncConstValue._SYNC_ACCOUNT_TYPE, account.type);

        map.put(Events.GUESTS_CAN_INVITE_OTHERS, event.getGuestsCanInviteOthers() ? 1 : 0);
        map.put(Events.GUESTS_CAN_MODIFY, event.getGuestsCanModify() ? 1 : 0);
        map.put(Events.GUESTS_CAN_SEE_GUESTS, event.getGuestsCanSeeGuests() ? 1 : 0);

        // Find the organizer for this event
        String organizer = null;
        Vector attendees = event.getAttendees();
        Enumeration attendeesEnum = attendees.elements();
        while (attendeesEnum.hasMoreElements()) {
            Who who = (Who) attendeesEnum.nextElement();
            if (who.getRelationship() == Who.RELATIONSHIP_ORGANIZER) {
                organizer = who.getEmail();
                break;
            }
        }
        if (organizer != null) {
            map.put(Events.ORGANIZER, organizer);
        }

        return ENTRY_OK;
    }

    public void updateProvider(Feed feed,
            Long syncLocalId, Entry entry,
            ContentProvider provider, Object info,
            GDataSyncData.FeedData feedSyncData) throws ParseException {
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

        // See if event is inside the window
        // feedSyncData will be null if the phone is creating the event
        if (entryState == ENTRY_OK && (feedSyncData == null || feedSyncData.newWindowEnd == 0)) {
            // A regular sync.  Accept the event if it is inside the sync window or
            // it is a recurrence exception for something inside the sync window.

            Long dtstart = map.getAsLong(Events.DTSTART);
            if (dtstart != null && (feedSyncData == null || dtstart < feedSyncData.windowEnd)) {
                //  dstart inside window, keeping event
            } else {
                Long originalInstanceTime = map.getAsLong(Events.ORIGINAL_INSTANCE_TIME);
                if (originalInstanceTime != null &&
                        (feedSyncData == null || originalInstanceTime <= feedSyncData.windowEnd)) {
                    // originalInstanceTime inside the window, keeping event
                } else {
                    // Rejecting event as outside window
                    return;
                }
            }
        }

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

    /**
     * Converts an old non-sliding-windows database to sliding windows
     * @param feedSyncData State of the sync.
     */
    private void upgradeToSlidingWindows(GDataSyncData.FeedData feedSyncData) {
        feedSyncData.windowEnd = getSyncWindowEnd();
        // TODO: Should prune old events
    }

    @Override
    public void getServerDiffs(SyncContext context,
            SyncData baseSyncData, SyncableContentProvider tempProvider,
            Bundle extras, Object baseSyncInfo, SyncResult syncResult) {
        final ContentResolver cr = getContext().getContentResolver();
        mServerDiffs++;
        final boolean syncingSingleFeed = (extras != null) && extras.containsKey("feed");
        final boolean syncingMetafeedOnly = (extras != null) && extras.containsKey("metafeedonly");

        if (syncingSingleFeed) {
            if (syncingMetafeedOnly) {
                Log.d(TAG, "metafeedonly and feed both set.");
                return;
            }
            StringBuilder sb = new StringBuilder();
            extrasToStringBuilder(extras, sb);
            String feedUrl = extras.getString("feed");

            GDataSyncData.FeedData feedSyncData = getFeedData(feedUrl, baseSyncData);
            if (feedSyncData != null && feedSyncData.windowEnd == 0) {
                upgradeToSlidingWindows(feedSyncData);
            } else if (feedSyncData == null) {
                feedSyncData = new GDataSyncData.FeedData(0, 0, false, "", 0);
                feedSyncData.windowEnd = getSyncWindowEnd();
                ((GDataSyncData) baseSyncData).feedData.put(feedUrl, feedSyncData);
            }

            if (extras.getBoolean("moveWindow", false)) {
                // This is a move window sync.  Set the new end.
                // Setting newWindowEnd makes this a sliding window expansion sync.
                if (feedSyncData.newWindowEnd == 0) {
                    feedSyncData.newWindowEnd = getSyncWindowEnd();
                }
            } else {
                if (getSyncWindowEnd() > feedSyncData.windowEnd) {
                    // Schedule a move-the-window sync

                    Bundle syncExtras = new Bundle();
                    syncExtras.clear();
                    syncExtras.putBoolean("moveWindow", true);
                    syncExtras.putString("feed", feedUrl);
                    ContentResolver.requestSync(null /* account */, Calendar.AUTHORITY, syncExtras);
                }
            }
            getServerDiffsForFeed(context, baseSyncData, tempProvider, feedUrl,
                    baseSyncInfo, syncResult);
            return;
        }

        // At this point, either metafeed sync or poll.
        // For the poll (or metafeed sync), refresh the list of calendars.
        // we can move away from this when we move to the new allcalendars feed, which is
        // syncable.  until then, we'll rely on the daily poll to keep the list of calendars
        // up to date.

        mRefresh++;
        context.setStatusText("Fetching list of calendars");
        fetchCalendarsFromServer();

        if (syncingMetafeedOnly) {
            // If not polling, nothing more to do.
            return;
        }

        // select the set of calendars for this account.
        final Account account = getAccount();
        final String[] accountSelectionArgs = new String[]{account.name, account.type};
        Cursor cursor = cr.query(Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION, SELECT_BY_ACCOUNT,
                accountSelectionArgs, null /* sort order */);

        Bundle syncExtras = new Bundle();

        try {
            while (cursor.moveToNext()) {
                boolean syncEvents = (cursor.getInt(6) == 1);
                String feedUrl = cursor.getString(3);

                if (!syncEvents) {
                    continue;
                }

                // schedule syncs for each of these feeds.
                syncExtras.clear();
                syncExtras.putAll(extras);
                syncExtras.putString("feed", feedUrl);
                ContentResolver.requestSync(account,
                        Calendar.Calendars.CONTENT_URI.getAuthority(), syncExtras);
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Gets end of the sliding sync window.
     *
     * @return end of window in ms
     */
    private long getSyncWindowEnd() {
        // How many days in the future the window extends (e.g. 1 year).  0 for no sliding window.
        long window = Settings.Gservices.getLong(getContext().getContentResolver(),
                Settings.Gservices.GOOGLE_CALENDAR_SYNC_WINDOW_DAYS, 0);
        if (window > 0) {
            // How often to advance the window (e.g. 30 days)
            long advanceInterval = Settings.Gservices.getLong(getContext().getContentResolver(),
                    Settings.Gservices.GOOGLE_CALENDAR_SYNC_WINDOW_UPDATE_DAYS, 30) * DAY_IN_MS;
            if (advanceInterval > 0) {
                // endOfWindow is the proposed end of the sliding window (e.g. 1 year out)
                long endOfWindow = System.currentTimeMillis() + window * DAY_IN_MS;
                // We don't want the end of the window to advance smoothly or else we would
                // be constantly doing syncs to update the window.  We "snap" the window to
                // a multiple of advanceInterval so the end of the window will only advance
                // every e.g. 30 days.  By dividing and multiplying by advanceInterval, the
                // window is truncated down to a multiple of advanceInterval.  This provides
                // the "snap" action.
                return (endOfWindow / advanceInterval) * advanceInterval;
            }
        }
        return Long.MAX_VALUE;
    }

    private void getServerDiffsForFeed(SyncContext context, SyncData baseSyncData,
            SyncableContentProvider tempProvider,
            String feed, Object baseSyncInfo, SyncResult syncResult) {
        final SyncInfo syncInfo = (SyncInfo) baseSyncInfo;
        final GDataSyncData syncData = (GDataSyncData) baseSyncData;

        final Account account = getAccount();
        Cursor cursor = getContext().getContentResolver().query(Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION, SELECT_BY_ACCOUNT_AND_FEED,
                new String[] { account.name, account.type, feed }, null /* sort order */);

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
        final Account account = getAccount();
        Cursor c = getContext().getContentResolver().query(
                Calendar.Calendars.CONTENT_URI,
                CALENDARS_PROJECTION,
                SELECT_BY_ACCOUNT, new String[]{account.name, account.type},
                null /* sort order */);
        final int idIndex = c.getColumnIndexOrThrow(Calendars._ID);
        final int urlIndex = c.getColumnIndexOrThrow(Calendars.URL);
        final int timezoneIndex = c.getColumnIndexOrThrow(Calendars.TIMEZONE);
        final int ownerAccountIndex = c.getColumnIndexOrThrow(Calendars.OWNER_ACCOUNT);
        while (c.moveToNext()) {
            map.clear();
            map.put(Calendars._ID, c.getLong(idIndex));
            map.put(Calendars.URL, c.getString(urlIndex));
            map.put(Calendars.TIMEZONE, c.getString(timezoneIndex));
            map.put(Calendars.OWNER_ACCOUNT, c.getString(ownerAccountIndex));
            cp.insert(Calendar.Calendars.CONTENT_URI, map);
        }
        c.close();
    }

    public void onAccountsChanged(Account[] accountsArray) {
        // - Get a cursor (A) over all sync'd calendars over all accounts
        // - Get a cursor (B) over all subscribed feeds for calendar
        // - If an item is in A but not B then add a subscription
        // - If an item is in B but not A then remove the subscription

        ContentResolver cr = getContext().getContentResolver();
        Cursor cursorA = null;
        Cursor cursorB = null;
        try {
            cursorA = Calendar.Calendars.query(cr, null /* projection */,
                    Calendar.Calendars.SYNC_EVENTS + "=1", CALENDAR_KEY_SORT_ORDER);
            int urlIndexA = cursorA.getColumnIndexOrThrow(Calendar.Calendars.URL);
            int accountNameIndexA = cursorA.getColumnIndexOrThrow(Calendar.Calendars._SYNC_ACCOUNT);
            int accountTypeIndexA =
                    cursorA.getColumnIndexOrThrow(Calendar.Calendars._SYNC_ACCOUNT_TYPE);
            cursorB = SubscribedFeeds.Feeds.query(cr, FEEDS_KEY_COLUMNS,
                    SubscribedFeeds.Feeds.AUTHORITY + "=?", new String[]{Calendar.AUTHORITY},
                    FEEDS_KEY_SORT_ORDER);
	    if (cursorB == null) {
		// This will happen if subscribed feeds are not installed. Get out since there
		// are no feeds to manipulate.
                return;
            }
            int urlIndexB = cursorB.getColumnIndexOrThrow(SubscribedFeeds.Feeds.FEED);
            int accountNameIndexB =
                    cursorB.getColumnIndexOrThrow(SubscribedFeeds.Feeds._SYNC_ACCOUNT);
            int accountTypeIndexB =
                    cursorB.getColumnIndexOrThrow(SubscribedFeeds.Feeds._SYNC_ACCOUNT_TYPE);
            for (CursorJoiner.Result joinerResult :
                    new CursorJoiner(cursorA, CALENDAR_KEY_COLUMNS, cursorB, FEEDS_KEY_COLUMNS)) {
                switch (joinerResult) {
                    case LEFT:
                        SubscribedFeeds.addFeed(
                                cr,
                                cursorA.getString(urlIndexA),
                                new Account(cursorA.getString(accountNameIndexA),
                                        cursorA.getString(accountTypeIndexA)),
                                Calendar.AUTHORITY,
                                CalendarClient.SERVICE);
                        break;
                    case RIGHT:
                        SubscribedFeeds.deleteFeed(
                                cr,
                                cursorB.getString(urlIndexB),
                                new Account(cursorB.getString(accountNameIndexB),
                                        cursorB.getString(accountTypeIndexB)),
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
    protected String getFeedUrl(Account account) {
        throw new UnsupportedOperationException("getFeedUrl() should not get called.");
    }

    protected Class getFeedEntryClass() {
        return EventEntry.class;
    }

    // XXX temporary debugging
    private static void extrasToStringBuilder(Bundle bundle, StringBuilder sb) {
        sb.append("[");
        for (String key : bundle.keySet()) {
            sb.append(key).append("=").append(bundle.get(key)).append(" ");
        }
        sb.append("]");
    }


    @Override
    protected void updateQueryParameters(QueryParams params, GDataSyncData.FeedData feedSyncData) {
        if (feedSyncData != null && feedSyncData.newWindowEnd > 0) {
            // Advancing the sliding window: set the parameters to the new part of the window
            params.setUpdatedMin(null);
            params.setParamValue("requirealldeleted", "false");
            Time startMinTime = new Time(Time.TIMEZONE_UTC);
            Time startMaxTime = new Time(Time.TIMEZONE_UTC);
            startMinTime.set(feedSyncData.windowEnd);
            startMaxTime.set(feedSyncData.newWindowEnd);
            String startMin = startMinTime.format("%Y-%m-%dT%H:%M:%S.000Z");
            String startMax = startMaxTime.format("%Y-%m-%dT%H:%M:%S.000Z");
            params.setParamValue("start-min", startMin);
            params.setParamValue("start-max", startMax);
        } else if (params.getUpdatedMin() == null) {
            // if this is the first sync, only bother syncing starting from
            // one month ago.
            // TODO: remove this restriction -- we may want all of
            // historical calendar events.
            Time lastMonth = new Time(Time.TIMEZONE_UTC);
            lastMonth.setToNow();
            --lastMonth.month;
            lastMonth.normalize(true /* ignore isDst */);
            // TODO: move start-min to CalendarClient?
            // or create CalendarQueryParams subclass (extra class)?
            String startMin = lastMonth.format("%Y-%m-%dT%H:%M:%S.000Z");
            params.setParamValue("start-min", startMin);
            // Note: start-max is not set for regular syncs.  The sync needs to pick up events
            // outside the window in case an event inside the window got moved outside.
            // The event will be discarded later.
        }

        // HACK: specify that we want to expand recurrences in the past,
        // so the server does not expand any recurrences.  we do this to
        // avoid a large number of gd:when elements that we do not need,
        // since we process gd:recurrence elements instead.
        params.setParamValue("recurrence-expansion-start", "1970-01-01");
        params.setParamValue("recurrence-expansion-end", "1970-01-01");
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

    private void fetchCalendarsFromServer() {
        if (mCalendarClient == null) {
            Log.w(TAG, "Cannot fetch calendars -- calendar url defined.");
            return;
        }

        Account account = null;
        String authToken = null;


        try {
            // TODO: allow caller to specify which account's feeds should be updated
            String[] features = new String[]{
                    GoogleLoginServiceConstants.FEATURE_LEGACY_HOSTED_OR_GOOGLE};
            Account[] accounts = AccountManager.get(getContext()).getAccountsByTypeAndFeatures(
                    GoogleLoginServiceConstants.ACCOUNT_TYPE, features, null, null).getResult();
            if (accounts.length == 0) {
                Log.w(TAG, "Unable to update calendars from server -- no users configured.");
                return;
            }

            account = accounts[0];

            Bundle bundle = AccountManager.get(getContext()).getAuthToken(
                    account, mCalendarClient.getServiceName(),
                    true /* notifyAuthFailure */, null /* callback */, null /* handler */)
                    .getResult();
            authToken = bundle.getString(AccountManager.KEY_AUTHTOKEN);
            if (authToken == null) {
                Log.w(TAG, "Unable to update calendars from server -- could not "
                        + "authenticate user " + account);
                return;
            }
        } catch (IOException e) {
            Log.w(TAG, "Unable to update calendars from server -- could not "
                    + "authenticate user " + account, e);
            return;
        } catch (AuthenticatorException e) {
            Log.w(TAG, "Unable to update calendars from server -- could not "
                    + "authenticate user " + account, e);
            return;
        } catch (OperationCanceledException e) {
            Log.w(TAG, "Unable to update calendars from server -- could not "
                    + "authenticate user " + account, e);
            return;
        }

        // get the current set of calendars.  we'll need to pay attention to
        // which calendars we get back from the server, so we can delete
        // calendars that have been deleted from the server.
        Set<Long> existingCalendarIds = new HashSet<Long>();

        getCurrentCalendars(existingCalendarIds);

        // get and process the calendars meta feed
        GDataParser parser = null;
        try {
            String feedUrl = mCalendarClient.getUserCalendarsUrl(account.name);
            feedUrl = CalendarSyncAdapter.rewriteUrlforAccount(account, feedUrl);
            parser = mCalendarClient.getParserForUserCalendars(feedUrl, authToken);
            // process the calendars
            processCalendars(account, parser, existingCalendarIds);
        } catch (ParseException pe) {
            Log.w(TAG, "Unable to process calendars from server -- could not "
                    + "parse calendar feed.", pe);
            return;
        } catch (IOException ioe) {
            Log.w(TAG, "Unable to process calendars from server -- encountered "
                    + "i/o error", ioe);
            return;
        } catch (HttpException e) {
            switch (e.getStatusCode()) {
                case HttpException.SC_UNAUTHORIZED:
                    Log.w(TAG, "Unable to process calendars from server -- could not "
                            + "authenticate user.", e);
                    return;
                case HttpException.SC_GONE:
                    Log.w(TAG, "Unable to process calendars from server -- encountered "
                            + "an AllDeletedUnavailableException, this should never happen", e);
                    return;
                default:
                    Log.w(TAG, "Unable to process calendars from server -- error", e);
                    return;
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        // delete calendars that are no longer sent from the server.
        final Uri calendarContentUri = Calendars.CONTENT_URI;
        final ContentResolver cr = getContext().getContentResolver();
        for (long calId : existingCalendarIds) {
            // NOTE: triggers delete all events, instances for this calendar.
            cr.delete(ContentUris.withAppendedId(calendarContentUri, calId),
                    null /* where */, null /* selectionArgs */);
        }
    }

    private void getCurrentCalendars(Set<Long> calendarIds) {
        final ContentResolver cr = getContext().getContentResolver();
        Cursor cursor = cr.query(Calendars.CONTENT_URI,
                new String[] { Calendars._ID },
                null /* selection */,
                null /* selectionArgs */,
                null /* sort */);
        if (cursor != null) {
            try {
                while (cursor.moveToNext()) {
                    calendarIds.add(cursor.getLong(0));
                }
            } finally {
                cursor.close();
            }
        }
    }

    private void processCalendars(Account account,
            GDataParser parser,
            Set<Long> existingCalendarIds)
            throws ParseException, IOException {
        final ContentResolver cr = getContext().getContentResolver();
        CalendarsFeed feed = (CalendarsFeed) parser.init();
        Entry entry = null;
        final Uri calendarContentUri = Calendars.CONTENT_URI;
        ArrayList<ContentValues> inserts = new ArrayList<ContentValues>();
        while (parser.hasMoreData()) {
            entry = parser.readNextEntry(entry);
            if (Config.LOGV) Log.v(TAG, "Read entry: " + entry.toString());
            CalendarEntry calendarEntry = (CalendarEntry) entry;
            ContentValues map = new ContentValues();
            String feedUrl = calendarEntryToContentValues(account, feed, calendarEntry, map);
            if (TextUtils.isEmpty(feedUrl)) {
                continue;
            }
            long calId = -1;

            Cursor c = cr.query(calendarContentUri,
                    new String[] { Calendars._ID },
                    Calendars.URL + "='"
                            + feedUrl + '\'' /* selection */,
                    null /* selectionArgs */,
                    null /* sort */);
            if (c != null) {
                try {
                    if (c.moveToFirst()) {
                        calId = c.getLong(0);
                        existingCalendarIds.remove(calId);
                    }
                } finally {
                    c.close();
                }
            }

            if (calId != -1) {
                if (Config.LOGV) Log.v(TAG, "Updating calendar " + map);
                // don't override the existing "selected" or "hidden" settings.
                map.remove(Calendars.SELECTED);
                map.remove(Calendars.HIDDEN);
                cr.update(ContentUris.withAppendedId(calendarContentUri, calId), map,
                        null /* where */, null /* selectionArgs */);
            } else {
                // Select this calendar for syncing and display if it is
                // selected and not hidden.
                int syncAndDisplay = 0;
                if (calendarEntry.isSelected() && !calendarEntry.isHidden()) {
                    syncAndDisplay = 1;
                }
                map.put(Calendars.SYNC_EVENTS, syncAndDisplay);
                map.put(Calendars.SELECTED, syncAndDisplay);
                map.put(Calendars.HIDDEN, 0);
                map.put(Calendars._SYNC_ACCOUNT, account.name);
                map.put(Calendars._SYNC_ACCOUNT_TYPE, account.type);
                if (Config.LOGV) Log.v(TAG, "Adding calendar " + map);
                inserts.add(map);
            }
        }
        if (!inserts.isEmpty()) {
            if (Config.LOGV) Log.v(TAG, "Bulk updating calendar list.");
            cr.bulkInsert(calendarContentUri, inserts.toArray(new ContentValues[inserts.size()]));
        }
    }

    /**
     * Convert the CalenderEntry to a Bundle that can be inserted/updated into the
     * Calendars table.
     */
    private String calendarEntryToContentValues(Account account, CalendarsFeed feed,
            CalendarEntry entry,
            ContentValues map) {
        map.clear();

        String url = entry.getAlternateLink();

        if (TextUtils.isEmpty(url)) {
            // yuck.  the alternate link was not available.  we should
            // reconstruct from the id.
            url = entry.getId();
            if (!TextUtils.isEmpty(url)) {
                url = convertCalendarIdToFeedUrl(url);
            } else {
                if (Config.LOGV) {
                    Log.v(TAG, "Cannot generate url for calendar feed.");
                }
                return null;
            }
        }

        url = rewriteUrlforAccount(account, url);

        map.put(Calendars.URL, url);
        map.put(Calendars.OWNER_ACCOUNT, calendarEmailAddressFromFeedUrl(url));
        map.put(Calendars.NAME, entry.getTitle());

        // TODO:
        map.put(Calendars.DISPLAY_NAME, entry.getTitle());

        map.put(Calendars.TIMEZONE, entry.getTimezone());

        String colorStr = entry.getColor();
        if (!TextUtils.isEmpty(colorStr)) {
            int color = Color.parseColor(colorStr);
            // Ensure the alpha is set to max
            color |= 0xff000000;
            map.put(Calendars.COLOR, color);
        }

        map.put(Calendars.SELECTED, entry.isSelected() ? 1 : 0);

        map.put(Calendars.HIDDEN, entry.isHidden() ? 1 : 0);

        int accesslevel;
        switch (entry.getAccessLevel()) {
            case CalendarEntry.ACCESS_NONE:
                accesslevel = Calendars.NO_ACCESS;
                break;
            case CalendarEntry.ACCESS_READ:
                accesslevel = Calendars.READ_ACCESS;
                break;
            case CalendarEntry.ACCESS_FREEBUSY:
                accesslevel = Calendars.FREEBUSY_ACCESS;
                break;
            case CalendarEntry.ACCESS_EDITOR:
                accesslevel = Calendars.EDITOR_ACCESS;
                break;
            case CalendarEntry.ACCESS_OWNER:
                accesslevel = Calendars.OWNER_ACCESS;
                break;
            case CalendarEntry.ACCESS_ROOT:
                accesslevel = Calendars.ROOT_ACCESS;
                break;
            default:
                accesslevel = Calendars.NO_ACCESS;
        }
        map.put(Calendars.ACCESS_LEVEL, accesslevel);
        // TODO: use the update time, when calendar actually supports this.
        // right now, calendar modifies the update time frequently.
        map.put(Calendars._SYNC_TIME, System.currentTimeMillis());

        return url;
    }

    // TODO: unit test.
    protected static final String convertCalendarIdToFeedUrl(String url) {
        // id: http://www.google.com/calendar/feeds/<username>/<cal id>
        // desired feed:
        //   http://www.google.com/calendar/feeds/<cal id>/<projection>
        int start = url.indexOf(FEEDS_SUBSTRING);
        if (start != -1) {
            // strip out the */ in /feeds/*/
            start += FEEDS_SUBSTRING.length();
            int end = url.indexOf('/', start);
            if (end != -1) {
                url = url.replace(url.substring(start, end + 1), "");
            }
            url = url + PRIVATE_FULL;
        }
        return url;
    }

    /**
     * Extracts the calendar email from a calendar feed url.
     * @param feed the calendar feed url
     * @return the calendar email that is in the feed url or null if it can't
     * find the email address.
     */
    public static String calendarEmailAddressFromFeedUrl(String feed) {
        // Example feed url:
        // https://www.google.com/calendar/feeds/foo%40gmail.com/private/full-noattendees
        String[] pathComponents = feed.split("/");
        if (pathComponents.length > 5 && "feeds".equals(pathComponents[4])) {
            try {
                return URLDecoder.decode(pathComponents[5], "UTF-8");
            } catch (UnsupportedEncodingException e) {
                Log.e(TAG, "unable to url decode the email address in calendar " + feed);
                return null;
            }
        }

        Log.e(TAG, "unable to find the email address in calendar " + feed);
        return null;
    }
}
