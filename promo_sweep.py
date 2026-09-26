#!/usr/bin/env python3
"""
promo_sweep.py - clear a promotional-mail backlog by sending domain, without an LLM.

Reads only headers (From, Subject, List-Unsubscribe, labels, date), so 80k messages scan
in minutes. A message counts as "bulk" if it carries a List-Unsubscribe header or Gmail
filed it under Promotions. Only bulk messages are ever touched: a personal email from
someone@shop.com is left alone even if shop.com is swept.

Steps
  1. python promo_sweep.py scan                       # headers -> sweep_domains.csv
  2. edit sweep_domains.csv: retain = TRUE for domains you want to keep seeing
  3. python promo_sweep.py apply --dry-run            # what would happen, per domain
  4. python promo_sweep.py apply                      # label "Promotional" + archive
     python promo_sweep.py apply --action trash       # or send to Trash instead

Rerunning scan picks up new mail and keeps your retain column. mail_sorter.py also honors
retain = TRUE from this file, so new mail from retained domains stays in your inbox.

Needs GMAIL_USER and GMAIL_APP_PASSWORD, same as mail_sorter.py (in the same folder).
"""

import argparse
import csv
import datetime
import email
import imaplib
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict

from mail_sorter import (
    SEED_RETAIN,
    base_domain,
    decode,
    find_special,
    gmail_search,
    load_retain,
    quote_mailbox,
    unsubscribe_links,
)

# Personal-mail domains: bulk mail from these is rare and usually wanted.
FREEMAIL = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "fastmail.com",
}

HEADER_FIELDS = "FROM SUBJECT LIST-UNSUBSCRIBE LIST-UNSUBSCRIBE-POST"


def open_db(path):
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS headers (
            msgid TEXT PRIMARY KEY, uid INTEGER, uidvalidity TEXT, date TEXT, sender TEXT,
            domain TEXT, subject TEXT, list_unsub INTEGER, unsub_url TEXT, one_click INTEGER,
            gmail_promo INTEGER, in_inbox INTEGER, swept TEXT DEFAULT '')"""
    )
    return con


def connect(readonly):
    user, pw = os.environ.get("GMAIL_USER"), os.environ.get("GMAIL_APP_PASSWORD")
    if not (user and pw):
        sys.exit("Set GMAIL_USER and GMAIL_APP_PASSWORD.")
    M = imaplib.IMAP4_SSL("imap.gmail.com")
    M.login(user, pw.replace(" ", ""))
    trash = find_special(M, b"\\Trash", "[Gmail]/Trash")
    M.select(
        quote_mailbox(find_special(M, b"\\All", "[Gmail]/All Mail")), readonly=readonly
    )
    uidvalidity = (M.response("UIDVALIDITY")[1] or [b""])[0]
    return M, trash, (uidvalidity or b"").decode()


def load_all_retain(path):
    retain = load_retain(path)
    if not os.path.exists(path):
        retain.update({d: True for d in FREEMAIL | SEED_RETAIN})
    return retain


def fetch_headers(M, uids, chunk=500):
    for i in range(0, len(uids), chunk):
        batch = b",".join(uids[i : i + chunk]).decode()
        typ, data = M.uid(
            "FETCH",
            batch,
            f"(X-GM-MSGID X-GM-LABELS INTERNALDATE BODY.PEEK[HEADER.FIELDS ({HEADER_FIELDS})])",
        )
        if typ != "OK":
            raise RuntimeError(f"fetch failed: {data}")
        for k, item in enumerate(data or []):
            if not isinstance(item, tuple):
                continue
            meta = item[0]
            nxt = data[k + 1] if k + 1 < len(data) else b""
            if isinstance(nxt, bytes):
                meta += nxt  # attributes Gmail sends after the header literal
            u = re.search(rb"UID (\d+)", meta)
            g = re.search(rb"X-GM-MSGID (\d+)", meta)
            if not (u and g):
                continue
            labels = re.search(rb"X-GM-LABELS \((.*?)\)", meta)
            in_inbox = int(bool(labels and b"\\Inbox" in labels.group(1)))
            t = imaplib.Internaldate2tuple(meta)
            date = time.strftime("%Y-%m-%d", t) if t else ""
            yield (
                int(u.group(1)),
                g.group(1).decode(),
                date,
                in_inbox,
                email.message_from_bytes(item[1]),
            )
        print(
            f"  headers {min(i + chunk, len(uids))}/{len(uids)}", end="\r", flush=True
        )
    print()


def cmd_scan(args):
    con = open_db(args.db)
    M, _, uidvalidity = connect(readonly=True)
    uids = gmail_search(M, args.query)
    print(f"{len(uids)} messages match {args.query!r}")
    promo_uids = {
        int(u) for u in gmail_search(M, f"({args.query}) category:promotions")
    }
    print(f"{len(promo_uids)} of them are in Gmail's Promotions category")

    for uid, msgid, date, in_inbox, hdr in fetch_headers(M, uids):
        sender = decode(hdr.get("From"))
        url, _, one_click = unsubscribe_links(hdr)
        con.execute(
            """INSERT INTO headers (msgid, uid, uidvalidity, date, sender, domain, subject, list_unsub,
                                    unsub_url, one_click, gmail_promo, in_inbox)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(msgid) DO UPDATE SET uid=excluded.uid, uidvalidity=excluded.uidvalidity,
                 date=excluded.date, sender=excluded.sender, domain=excluded.domain,
                 subject=excluded.subject, list_unsub=excluded.list_unsub, unsub_url=excluded.unsub_url,
                 one_click=excluded.one_click, gmail_promo=excluded.gmail_promo, in_inbox=excluded.in_inbox""",
            (
                msgid,
                uid,
                uidvalidity,
                date,
                sender,
                base_domain(sender),
                decode(hdr.get("Subject")),
                int(bool(hdr.get("List-Unsubscribe"))),
                url,
                one_click,
                int(uid in promo_uids),
                in_inbox,
            ),
        )
    con.commit()
    M.logout()
    n = write_report(con, args.domains_file, load_all_retain(args.domains_file))
    print(
        f"{n} bulk-mail domains written to {args.domains_file}. Mark retain = TRUE for any you want to keep."
    )


def write_report(con, path, retain):
    dates = [d for (d,) in con.execute("SELECT date FROM headers WHERE date != ''")]
    months = 1.0
    if dates:
        span = (
            datetime.date.fromisoformat(max(dates))
            - datetime.date.fromisoformat(min(dates))
        ).days
        months = max(1.0, span / 30.44)

    stats = {}
    for domain, date, sender, subject, url, one_click, promo, inbox in con.execute(
        """SELECT domain, date, sender, subject, unsub_url, one_click, gmail_promo, in_inbox
           FROM headers WHERE list_unsub = 1 OR gmail_promo = 1"""
    ):
        d = domain or "(unknown)"
        s = stats.setdefault(
            d,
            {
                "n": 0,
                "inbox": 0,
                "promo": 0,
                "first": date,
                "last": "",
                "from": "",
                "subject": "",
                "url": "",
                "one_click": 0,
            },
        )
        s["n"] += 1
        s["inbox"] += inbox
        s["promo"] += promo
        s["first"] = min(s["first"] or date, date or s["first"])
        if (date or "") >= s["last"]:
            s["last"], s["from"], s["subject"] = date or "", sender, subject
            if url:
                s["url"], s["one_click"] = url, one_click
    for d in retain:
        stats.setdefault(
            d,
            {
                "n": 0,
                "inbox": 0,
                "promo": 0,
                "first": "",
                "last": "",
                "from": "",
                "subject": "",
                "url": "",
                "one_click": 0,
            },
        )

    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "domain",
                "last_seen",
                "per_month",
                "retain",
                "messages",
                "in_inbox",
                "gmail_promo_share",
                "first_seen",
                "latest_from",
                "latest_subject",
                "one_click_unsubscribe",
                "unsubscribe_url",
            ]
        )
        for d, s in sorted(
            stats.items(), key=lambda kv: (kv[1]["last"], kv[1]["n"]), reverse=True
        ):
            w.writerow(
                [
                    d,
                    s["last"],
                    round(s["n"] / months, 1),
                    "TRUE" if retain.get(d) else "FALSE",
                    s["n"],
                    s["inbox"],
                    f"{s['promo'] / s['n']:.0%}" if s["n"] else "",
                    s["first"],
                    s["from"],
                    s["subject"],
                    "yes" if s["one_click"] else "",
                    s["url"],
                ]
            )
    return len(stats)


def cmd_apply(args):
    if not os.path.exists(args.domains_file):
        sys.exit(f"{args.domains_file} not found - run scan first.")
    retain = load_retain(args.domains_file)
    con = open_db(args.db)
    rows = con.execute(
        """SELECT uid, uidvalidity, domain, in_inbox, swept FROM headers
           WHERE list_unsub = 1 OR gmail_promo = 1"""
    ).fetchall()

    todo, kept = defaultdict(list), defaultdict(int)
    for uid, uidvalidity, domain, in_inbox, swept in rows:
        if retain.get(domain):
            kept[domain] += 1
        elif swept != args.action:
            todo[domain].append((uid, uidvalidity))
    total = sum(len(v) for v in todo.values())

    verb = {
        "label": f"+{args.label}",
        "archive": f"+{args.label}, archive",
        "trash": "move to Trash",
    }[args.action]
    print(
        f"{'would' if args.dry_run else 'will'}: {verb} -> {total} messages from {len(todo)} domains"
    )
    print(f"retained: {sum(kept.values())} messages from {len(kept)} domains\n")
    for d, items in sorted(todo.items(), key=lambda kv: -len(kv[1]))[:25]:
        print(f"{len(items):7}  {d}")
    if len(todo) > 25:
        print(f"    ... and {len(todo) - 25} more domains")
    if args.dry_run or not total:
        return

    M, trash, uidvalidity = connect(readonly=False)
    uids = [u for items in todo.values() for u, v in items if v == uidvalidity]
    if len(uids) < total:
        print(
            f"\n{total - len(uids)} messages have stale IDs; run scan again first to include them."
        )
    if args.action in ("label", "archive"):
        M.create(quote_mailbox(args.label))

    done = 0
    for i in range(0, len(uids), 500):
        batch = uids[i : i + 500]
        uid_set = ",".join(map(str, batch))
        if args.action == "trash":
            typ, _ = M.uid("MOVE", uid_set, quote_mailbox(trash))
            if typ != "OK":
                M.uid("COPY", uid_set, quote_mailbox(trash))
        else:
            M.uid("STORE", uid_set, "+X-GM-LABELS", f"({quote_mailbox(args.label)})")
            if args.action == "archive":
                M.uid("STORE", uid_set, "-X-GM-LABELS", "(\\Inbox)")
        con.executemany(
            "UPDATE headers SET swept=? WHERE uid=? AND uidvalidity=?",
            [(args.action, u, uidvalidity) for u in batch],
        )
        con.commit()
        done += len(batch)
        print(f"  {done}/{len(uids)}", end="\r", flush=True)
    M.logout()
    print(f"\nDone: {verb} on {done} messages.")


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--db", default="job_mail_cache.sqlite")
    p.add_argument("--domains-file", default="sweep_domains.csv")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan", help="read headers and write the domain report")
    s.add_argument(
        "--query",
        default="after:1990/01/01",
        help="Gmail search to scan (default: everything)",
    )
    a = sub.add_parser("apply", help="act on bulk mail from domains not marked retain")
    a.add_argument("--action", choices=["label", "archive", "trash"], default="archive")
    a.add_argument("--label", default="Promotional")
    a.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    cmd_scan(args) if args.cmd == "scan" else cmd_apply(args)


if __name__ == "__main__":
    main()
