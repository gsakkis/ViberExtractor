import argparse
import csv
import itertools as it
import json
import logging
import mimetypes
import os
import sqlite3
import sys
from datetime import timedelta
from operator import itemgetter

from dateutil import parser as dateparser


MEDIA_TYPE_MAPPING = {
    2: "image",
    3: "video",
    4: "sticker",
    6: "voice mail",
    7: "viber instant video",
    11: "audio",
}


class MultipleResultsException(Exception):
    pass


def fetch(conn, query, one=False):
    cur = conn.cursor()
    logging.debug(query)
    cur.execute(query)
    rows = cur.fetchall()
    if not one:
        return rows
    if len(rows) > 1:
        raise MultipleResultsException
    return rows[0] if rows else None


def select_chat_id(conn):
    query = """
        SELECT ChatRelation.ChatID AS chat_id,
               coalesce(Contact.name, Contact.clientname) as contact
        FROM ChatRelation
        JOIN ChatInfo ON ChatInfo.ChatID = ChatRelation.ChatID
        JOIN Contact ON Contact.ContactID = ChatRelation.ContactID
        WHERE ChatRelation.ContactID != 1
    """
    rows = fetch(conn, query)
    writer = csv.writer(sys.stderr, delimiter="\t")
    while True:
        chat_ids = set()
        writer.writerow(("chatID", "contact(s)"))
        for chat_id, group in it.groupby(rows, itemgetter("chat_id")):
            chat_ids.add(chat_id)
            contacts = sorted(map(itemgetter("contact"), group))
            writer.writerow((chat_id, ", ".join(contacts)))
        print("\nPlease select one of the above chatIDs: ", file=sys.stderr)
        try:
            chat_id = int(input())
            if chat_id in chat_ids:
                return chat_id
        except ValueError:
            pass


def fetch_chat(conn, chat_id, unixtime_start=None, unixtime_end=None):
    query = """
    SELECT Events.EventID AS event_id,
           datetime(Events.timestamp/1000, 'unixepoch') AS timestamp,
           COALESCE(Contact.Name, Contact.ClientName) AS contact,
           Messages.Type AS type,
           Messages.Subject AS subject,
           Messages.Body AS body,
           Messages.Info AS info,
           Messages.Duration AS duration,
           Messages.StickerID AS sticker_id
    FROM Events
    JOIN Messages ON Events.EventID = Messages.EventID
    JOIN Contact ON Events.ContactID = Contact.ContactID
    """
    filters = [f"ChatId = {chat_id}"]
    if unixtime_start:
        filters.append(f"Events.timestamp >= {1000 * unixtime_start}")
    if unixtime_end:
        filters.append(f"Events.timestamp < {1000 * unixtime_end}")

    if filters:
        query += f" WHERE " + " AND ".join(filters)
    query += " ORDER BY Events.timestamp"

    parse = dateparser.parse
    for row in fetch(conn, query):
        yield dict(row, timestamp=parse(row["timestamp"]))


def iter_daily_sessions(rows, inactivity=None):
    if inactivity is None:
        date_rows = ((row["timestamp"].date(), row) for row in rows)
        return ((date, [session]) for date, session in group_by_first(date_rows))
    else:
        grouped_by_start = group_by_first(iter_start_rows(rows, inactivity))
        return group_by_first(
            (start.date(), session) for start, session in grouped_by_start
        )


def iter_start_rows(rows, inactivity):
    timestamp_rows = ((row["timestamp"], row) for row in rows)
    prev_it, current_it = it.tee(timestamp_rows)
    current = next(current_it, None)
    if current is None:
        return
    yield current
    start = current[0]
    for (prev_ts, prev_row), (cur_ts, cur_row) in zip(prev_it, current_it):
        if cur_ts - prev_ts > inactivity:
            start = cur_ts
        yield start, cur_row


def group_by_first(iterable):
    for key, group in it.groupby(iterable, itemgetter(0)):
        yield key, list(map(itemgetter(1), group))


def extract_message(row):
    mtype = row["type"]
    if mtype in (1, 15):
        return row["body"]

    if mtype == 9:
        return row["body"] or "_(URL not available)_"

    msg = {"type": MEDIA_TYPE_MAPPING[mtype]}
    if row["subject"]:
        msg["subject"] = row["subject"]

    info = json.loads(row["info"])
    file_info = info.get("fileInfo") or {}
    file_name = file_info.get("FileName")
    if file_name:
        msg["filename"] = file_name
        mime_type = mimetypes.guess_type(file_name)[0]
        if mime_type is not None:
            msg["type"] = mime_type.split("/")[0]

    media_type = msg["type"]
    if media_type == "sticker":
        msg["id"] = row["sticker_id"]
    elif media_type == "voice mail":
        msg["duration"] = format_duration(row["duration"])
    elif media_type == "audio":
        msg["duration"] = format_duration(file_info["Duration"])
    elif media_type == "viber instant video":
        msg.update(info["ivmInfo"])

    return msg


def format_duration(msec):
    return f"{int(msec/1000)} seconds"


def format_message(row):
    msg = extract_message(row)
    msg = msg.strip() if isinstance(msg, str) else f"_{msg}_"
    when = row["timestamp"].time()
    who = row["contact"]
    return f"[{when}] **{who}**: {msg}"


def main():
    parser = argparse.ArgumentParser(
        description="Extract messages from a given SQLite database of Viber message logs."
    )
    parser.add_argument(
        "db", help="path to the Viber database file",
    )
    parser.add_argument(
        "-c", "--chat", help="chatID of the chat to extract messages from",
    )
    parser.add_argument(
        "-f", "--from", help="start date(-time) to filter from",
    )
    parser.add_argument(
        "-t", "--to", help="end date(-time) to filter to",
    )
    parser.add_argument(
        "-s",
        "--session",
        type=int,
        metavar="M",
        help="split the chat log into sessions separated by at least M minutes of inactivity",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    chat_id = args.chat or select_chat_id(conn)
    start, end = [getattr(args, opt) for opt in ("from", "to")]
    start, end = [dateparser.parse(s).timestamp() if s else None for s in (start, end)]
    rows = fetch_chat(conn, chat_id, start, end)
    inactivity = timedelta(minutes=args.session) if args.session else None
    for date, sessions in iter_daily_sessions(rows, inactivity):
        print(f"## {date}\n")
        for session in sessions:
            for row in session:
                print(format_message(row))
            print()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
