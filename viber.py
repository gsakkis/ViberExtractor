import itertools as it
import json
import logging
import mimetypes
import os
import sqlite3
import argparse
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


def fetch_chat_id(conn, name):
    # 1. search for group chat with this name
    chat_id = fetch(
        conn, f"SELECT ChatID FROM ChatInfo WHERE Name='{name}'", one=True
    ) or fetch(
        conn, f"SELECT ChatID FROM ChatInfo WHERE instr(Name, '{name}')", one=True
    )
    if chat_id:
        return chat_id[0]

    # 2. no group chat was found: check contacts for an individual name (exact match)
    contact_id = fetch(
        conn,
        f"SELECT ContactID FROM Contact WHERE '{name}' in (Name, ClientName)",
        one=True,
    ) or fetch(
        conn,
        f"SELECT ContactID FROM Contact "
        f"WHERE instr(Name, '{name}') OR instr(ClientName, '{name}')",
        one=True,
    )
    if not contact_id:
        raise ValueError(f"Couldn't find chat name {name!r} in groups or contacts")

    contact_id = contact_id[0]

    # 3. get the chat with contact_id in it that only has 2 people (you + contact)
    chat_id = fetch(
        conn,
        f"SELECT c1.ChatID FROM ChatRelation c1 "
        f"WHERE c1.ContactID={contact_id} "
        f"  AND (SELECT COUNT(*) FROM ChatRelation c2 WHERE c2.ChatID=c1.ChatID)=2",
        one=True,
    )
    if not chat_id:
        raise ValueError(f"Found contact with name {name!r}, but no chat found.")

    return chat_id[0]


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

    # filters.append("Messages.type in (6)")
    if filters:
        query += f" WHERE " + " AND ".join(filters)
    query += " ORDER BY Events.timestamp"

    parse = dateparser.parse
    for row in fetch(conn, query):
        yield dict(row, timestamp=parse(row["timestamp"]))


def iter_sessions(rows, inactivity):
    iter_date_sessions = (
        (start.date(), list(map(itemgetter(1), group)))
        for start, group in it.groupby(iter_start_rows(rows, inactivity), itemgetter(0))
    )
    for start_date, group in it.groupby(iter_date_sessions, itemgetter(0)):
        yield start_date, list(map(itemgetter(1), group))


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
        "-n", "--name", help="name of the chat to extract messages from",
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
    start, end = [getattr(args, opt) for opt in ("from", "to")]
    start, end = [dateparser.parse(s).timestamp() if s else None for s in (start, end)]

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    try:
        chat_id = fetch_chat_id(conn, args.name)
    except MultipleResultsException:
        raise ValueError(f"Ambiguous name {args.name!r}: more than one contacts found")

    rows = fetch_chat(conn, chat_id, start, end)
    if args.session:
        inactivity = timedelta(minutes=args.session)
        for start_date, sessions in iter_sessions(rows, inactivity):
            print(f"## {start_date}\n")
            for session in sessions:
                for line in session:
                    print(format_message(line))
                print()
    else:
        for line in rows:
            print(format_message(line))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
