"""sopel-chanlogs

A channel logging plugin for Sopel IRC bots

Built on an original plugin by David Baumgold <david@davidbaumgold.com>, 2014
Continued by Elsie Powell, Max Gurela, dgw, and other contributors

Licensed under the Eiffel Forum License 2

https://sopel.chat/

Database Logging Configuration:
------------------------------
This plugin supports both file-based and database logging, which can be enabled independently.

Configuration options:
- file_log (boolean): Enable file-based logging (default: True)
- db_log (boolean): Enable database logging (default: False)

Example configurations:

# File logging only (default behavior)
[chanlogs]
dir = ~/chanlogs
file_log = yes
db_log = no

# Database logging only
[chanlogs]
file_log = no
db_log = yes

# Both file and database logging
[chanlogs]
dir = ~/chanlogs
file_log = yes
db_log = yes

Database Requirements:
- SQLAlchemy (automatically available in Sopel)
- Sopel's built-in database support (configured via [db] section)

The database logging creates a 'channel_logs' table with the following fields:
- id: Primary key
- timestamp: When the event occurred
- channel: Channel name (cleaned)
- nick: User nickname
- event_type: Type of event (message, action, join, part, quit, nick, topic)
- message: Message content (for messages/actions/topics)
- raw_line: Formatted log line as it would appear in files

Query Functions:
- get_recent_messages(bot, channel, limit=100): Get recent messages from a channel
- search_messages(bot, channel, search_term, limit=50): Search for messages containing a term
"""
from __future__ import annotations

from datetime import datetime
import os.path
import re
import threading

import pytz
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

from sopel import plugin
from sopel.config.types import (
    BooleanAttribute,
    FilenameAttribute,
    StaticSection,
    ValidatedAttribute,
)
from sopel.tools.memories import SopelMemoryWithDefault


MESSAGE_TPL = "{datetime}  <{trigger.nick}> {message}"
ACTION_TPL = "{datetime}  * {trigger.nick} {message}"
NICK_TPL = "{datetime}  *** {trigger.nick} is now known as {trigger.sender}"
JOIN_TPL = "{datetime}  *** {trigger.nick} has joined {trigger}"
PART_TPL = "{datetime}  *** {trigger.nick} has left {trigger}"
QUIT_TPL = "{datetime}  *** {trigger.nick} has quit IRC"
TOPIC_TPL = "{datetime}  *** {trigger.nick} changed the topic to {trigger.args[1]}"
# According to Wikipedia
BAD_CHARS = re.compile(r'[\/?%*:|"<>. ]')


# Database model for channel logs
Base = declarative_base()


class ChannelLog(Base):
    """SQLAlchemy model for storing channel log entries."""
    __tablename__ = 'channel_logs'

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    channel = Column(String(100), nullable=False, index=True)
    nick = Column(String(100), nullable=False, index=True)
    event_type = Column(String(20), nullable=False)  # message, action, join, part, quit, nick, topic
    message = Column(Text, nullable=True)  # the actual message content
    raw_line = Column(Text, nullable=False)  # formatted log line as it appears in files

    def __repr__(self):
        return f"<ChannelLog(timestamp={self.timestamp}, channel={self.channel}, nick={self.nick}, event_type={self.event_type})>"


class ChanlogsSection(StaticSection):
    dir = FilenameAttribute('dir', directory=True, default='~/chanlogs')
    """Path to channel log storage directory"""
    by_day = BooleanAttribute('by_day', default=True)
    """Split log files by day"""
    privmsg = BooleanAttribute('privmsg', default=False)
    """Record private messages"""
    microseconds = BooleanAttribute('microseconds', default=False)
    """Microsecond precision"""
    localtime = BooleanAttribute('localtime', default=False)
    """Attempt to use preferred timezone instead of UTC"""
    # Logging destination options
    file_log = BooleanAttribute('file_log', default=True)
    """Enable file-based logging"""
    db_log = BooleanAttribute('db_log', default=False)
    """Enable database logging"""
    # TODO: Allow configuration of templates; perhaps the user would like to use
    #       parsers that support only specific formats.
    message_template = ValidatedAttribute('message_template', default=None)
    action_template = ValidatedAttribute('action_template', default=None)
    join_template = ValidatedAttribute('join_template', default=None)
    part_template = ValidatedAttribute('part_template', default=None)
    quit_template = ValidatedAttribute('quit_template', default=None)
    nick_template = ValidatedAttribute('nick_template', default=None)
    topic_template = ValidatedAttribute('topic_template', default=None)


def configure(config):
    config.define_section('chanlogs', ChanlogsSection, validate=False)
    config.chanlogs.configure_setting(
        'dir',
        'Path to channel log storage directory',
    )
    config.chanlogs.configure_setting(
        'file_log',
        'Enable file-based logging? (yes/no)',
    )
    config.chanlogs.configure_setting(
        'db_log',
        'Enable database logging? (yes/no)',
    )


def get_datetime(bot):
    """Get a datetime object of the current time."""
    dt = datetime.now(pytz.utc)
    target_tz = bot.config.core.default_timezone

    if bot.config.chanlogs.localtime and target_tz != 'UTC':
        # small optimization above, making sure not to convert UTC to UTC
        target_tz = pytz.timezone(target_tz)
        dt = dt.astimezone(target_tz)

    if not bot.config.chanlogs.microseconds:
        dt = dt.replace(microsecond=0)

    return dt


def get_fpath(bot, trigger, channel=None):
    """Get the appropriate log file path.

    Returns a string corresponding to the path to the file where the message
    currently being handled should be logged.
    """
    basedir = bot.config.chanlogs.dir
    channel = channel or trigger.sender
    channel = channel.lstrip("#")
    channel = BAD_CHARS.sub('__', channel)
    channel = bot.make_identifier(channel).lower()

    dt = get_datetime(bot)
    if bot.config.chanlogs.by_day:
        fname = "{channel}-{date}.log".format(channel=channel, date=dt.date().isoformat())
    else:
        fname = "{channel}.log".format(channel=channel)
    return os.path.join(basedir, fname)


def _format_template(tpl, bot, trigger, **kwargs):
    dt = get_datetime(bot)

    formatted = tpl.format(
        trigger=trigger, datetime=dt.isoformat(),
        date=dt.date().isoformat(), time=dt.time().isoformat(),
        **kwargs
    ) + "\n"

    return formatted


def _create_db_tables(bot):
    """Create database tables if they don't exist."""
    try:
        Base.metadata.create_all(bot.db.engine)
    except SQLAlchemyError as e:
        bot.logger.error(f"Failed to create channel log tables: {e}")


def _log_to_database(bot, channel, nick, event_type, message_content, formatted_line):
    """Log an entry to the database."""
    try:
        session = bot.db.session()
        dt = get_datetime(bot)

        clean_channel = bot.make_identifier(clean_channel).lower()

        log_entry = ChannelLog(
            timestamp=dt,
            channel=clean_channel,
            nick=nick,
            event_type=event_type,
            message=message_content,
            raw_line=formatted_line.rstrip('\n')
        )

        session.add(log_entry)
        session.commit()

    except SQLAlchemyError as e:
        bot.logger.error(f"Failed to log to database: {e}")
        try:
            session.rollback()
        except:
            pass
    finally:
        try:
            session.close()
        except:
            pass


def _should_log_to_file(bot):
    """Determine if we should log to files based on configuration."""
    return bot.config.chanlogs.file_log


def _should_log_to_db(bot):
    """Determine if we should log to database based on configuration."""
    return bot.config.chanlogs.db_log


def setup(bot):
    bot.config.define_section('chanlogs', ChanlogsSection)

    # Validate that at least one logging method is enabled
    if not bot.config.chanlogs.file_log and not bot.config.chanlogs.db_log:
        raise ValueError(
            "Both file_log and db_log are disabled. At least one logging method must be enabled. "
            "Set file_log=yes for file-based logging or db_log=yes for database logging."
        )

    # locks for log files
    if 'chanlog_locks' not in bot.memory:
        bot.memory['chanlog_locks'] = SopelMemoryWithDefault(threading.Lock)

    # Create database tables if db logging is enabled
    if _should_log_to_db(bot):
        _create_db_tables(bot)


@plugin.rule('.*')
@plugin.echo
@plugin.unblockable
def log_message(bot, message):
    """Log all messages, including Sopel's own"""
    # if this is a private message and we're not logging those, return early
    if message.sender.is_nick() and not bot.config.chanlogs.privmsg:
        return

    # determine which template we want, message or action
    event_type = 'message'
    if message.startswith("\001ACTION ") and message.endswith("\001"):
        event_type = 'action'
        tpl = bot.config.chanlogs.action_template or ACTION_TPL
        # strip off start and end
        message = message[8:-1]
    else:
        tpl = bot.config.chanlogs.message_template or MESSAGE_TPL

    logline = _format_template(tpl, bot, message, message=message)

    # Log to file if enabled
    if _should_log_to_file(bot):
        fpath = get_fpath(bot, message)
        with bot.memory['chanlog_locks'][fpath]:
            with open(fpath, "ab") as f:
                f.write(logline.encode('utf8'))

    # Log to database if enabled
    if _should_log_to_db(bot):
        _log_to_database(bot, message.sender, message.nick, event_type, message, logline)


@plugin.rule('.*')
@plugin.event("JOIN")
@plugin.unblockable
def log_join(bot, trigger):
    """Log joins"""
    tpl = bot.config.chanlogs.join_template or JOIN_TPL
    logline = _format_template(tpl, bot, trigger)

    # Log to file if enabled
    if _should_log_to_file(bot):
        fpath = get_fpath(bot, trigger, channel=trigger.sender)
        with bot.memory['chanlog_locks'][fpath]:
            with open(fpath, "ab") as f:
                f.write(logline.encode('utf8'))

    # Log to database if enabled
    if _should_log_to_db(bot):
        _log_to_database(bot, trigger.sender, trigger.nick, 'join', None, logline)


@plugin.rule('.*')
@plugin.event("PART")
@plugin.unblockable
def log_part(bot, trigger):
    """Log parts"""
    tpl = bot.config.chanlogs.part_template or PART_TPL
    logline = _format_template(tpl, bot, trigger=trigger)

    # Log to file if enabled
    if _should_log_to_file(bot):
        fpath = get_fpath(bot, trigger, channel=trigger.sender)
        with bot.memory['chanlog_locks'][fpath]:
            with open(fpath, "ab") as f:
                f.write(logline.encode('utf8'))

    # Log to database if enabled
    if _should_log_to_db(bot):
        _log_to_database(bot, trigger.sender, trigger.nick, 'part', None, logline)


@plugin.rule('.*')
@plugin.event("QUIT")
@plugin.unblockable
@plugin.thread(False)
@plugin.priority('high')
def log_quit(bot, trigger):
    """Log quits"""
    tpl = bot.config.chanlogs.quit_template or QUIT_TPL
    logline = _format_template(tpl, bot, trigger)
    # make a copy of Sopel's channel list that we can safely iterate over
    channels_copy = list(bot.channels.values())
    # write logline to *all* channels that the user was present in
    for channel in channels_copy:
        if trigger.nick in channel.users:
            # Log to file if enabled
            if _should_log_to_file(bot):
                fpath = get_fpath(bot, trigger, channel.name)
                with bot.memory['chanlog_locks'][fpath]:
                    with open(fpath, "ab") as f:
                        f.write(logline.encode('utf8'))

            # Log to database if enabled
            if _should_log_to_db(bot):
                _log_to_database(bot, channel.name, trigger.nick, 'quit', None, logline)


@plugin.rule('.*')
@plugin.event("NICK")
@plugin.unblockable
def log_nick_change(bot, trigger):
    """Log nick changes"""
    tpl = bot.config.chanlogs.nick_template or NICK_TPL
    logline = _format_template(tpl, bot, trigger)
    old_nick = trigger.nick
    new_nick = trigger.sender
    # make a copy of Sopel's channel list that we can safely iterate over
    channels_copy = list(bot.channels.values())
    # write logline to *all* channels that the user is present in
    for channel in channels_copy:
        if old_nick in channel.users or new_nick in channel.users:
            # Log to file if enabled
            if _should_log_to_file(bot):
                fpath = get_fpath(bot, trigger, channel.name)
                with bot.memory['chanlog_locks'][fpath]:
                    with open(fpath, "ab") as f:
                        f.write(logline.encode('utf8'))

            # Log to database if enabled
            if _should_log_to_db(bot):
                _log_to_database(bot, channel.name, old_nick, 'nick', new_nick, logline)


@plugin.rule('.*')
@plugin.event("TOPIC")
@plugin.unblockable
def log_topic(bot, trigger):
    """Log topic changes"""
    tpl = bot.config.chanlogs.topic_template or TOPIC_TPL
    logline = _format_template(tpl, bot, trigger)
    topic_content = trigger.args[1] if len(trigger.args) > 1 else None

    # Log to file if enabled
    if _should_log_to_file(bot):
        fpath = get_fpath(bot, trigger, channel=trigger.sender)
        with bot.memory['chanlog_locks'][fpath]:
            with open(fpath, "ab") as f:
                f.write(logline.encode('utf8'))

    # Log to database if enabled
    if _should_log_to_db(bot):
        _log_to_database(bot, trigger.sender, trigger.nick, 'topic', topic_content, logline)


# Optional query functions for database access
def get_recent_messages(bot, channel, limit=100):
    """Get recent messages from a channel (requires db_log enabled)."""
    if not _should_log_to_db(bot):
        return None

    try:
        session = bot.db.session()
        clean_channel = bot.make_identifier(clean_channel).lower()

        messages = session.query(ChannelLog).filter(
            ChannelLog.channel == clean_channel
        ).order_by(ChannelLog.timestamp.desc()).limit(limit).all()

        return messages
    except SQLAlchemyError as e:
        bot.logger.error(f"Failed to query database: {e}")
        return None
    finally:
        try:
            session.close()
        except:
            pass


def search_messages(bot, channel, search_term, limit=50):
    """Search for messages containing a term (requires db_log enabled)."""
    if not _should_log_to_db(bot):
        return None

    try:
        session = bot.db.session()
        clean_channel = bot.make_identifier(clean_channel).lower()

        messages = session.query(ChannelLog).filter(
            ChannelLog.channel == clean_channel,
            ChannelLog.message.contains(search_term)
        ).order_by(ChannelLog.timestamp.desc()).limit(limit).all()

        return messages
    except SQLAlchemyError as e:
        bot.logger.error(f"Failed to search database: {e}")
        return None
    finally:
        try:
            session.close()
        except:
            pass
