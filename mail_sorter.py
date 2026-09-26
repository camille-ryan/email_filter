#!/usr/bin/env python3
"""
job_mail_sorter.py - sort Gmail with a local LLM (Ollama): job-search mail gets labeled,
promotional mail gets cleared out of the inbox.

Pipeline
  1. IMAP into Gmail's "All Mail" (Spam and Trash are not included).
  2. Every message matching --query is a candidate (optional --prefilter narrows it to
     job keyword/sender matches first; note that skips promo detection for everything else).
  3. A local LLM classifies each message (structured JSON output).
  4. Actions:
       job categories -> "Job Applications" label (+ optional sub-label, optional archive)
       promotional    -> "Promotional" label + archive by default (see --promos)
  5. Every decision goes to a CSV. Promotional mail is summarized by domain in
     promo_domains.csv (most recent first, messages/month, unsubscribe link). Put TRUE in
     its "retain" column for domains you want to keep seeing; they're left alone from then
     on, and your choices survive reruns. Results are cached in SQLite, so reruns are incremental and a
     dry run followed by a real run doesn't re-call the LLM.

Setup
  python3 -m venv .venv && source .venv/bin/activate
  pip install requests beautifulsoup4
  ollama pull qwen2.5:7b-instruct            # or any instruct model; see --model
  export GMAIL_USER=you@gmail.com
  export GMAIL_APP_PASSWORD=abcdabcdabcdabcd  # https://myaccount.google.com/apppasswords

Usage
  python job_mail_sorter.py --dry-run --limit 50     # classify + log, change nothing
  python job_mail_sorter.py                          # apply (reuses cached classifications)
  python job_mail_sorter.py --promos trash           # send promotional mail to Trash instead
  python job_mail_sorter.py --sublabels              # also Job Applications/Rejections, /Interviews, ...
  python job_mail_sorter.py --query "newer_than:1d"  # routine run on recent mail (e.g. from cron)
"""

import argparse
import csv
import datetime
import email
import imaplib
import json
import os
import re
import sqlite3
import sys
import time
from collections import defaultdict
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:  # falls back to a crude tag strip
    BeautifulSoup = None


# category -> (group, job sub-label)
CATEGORIES = {
    "application_confirmation": ("job", "Confirmations"),
    "rejection": ("job", "Rejections"),
    "interview_next_steps": ("job", "Interviews"),
    "offer": ("job", "Offers"),
    "recruiter_outreach": ("job", "Recruiters"),
    "job_alert": ("job", "Alerts"),
    "promotional": ("promo", None),
    "other": (None, None),
}

SYSTEM_PROMPT = """You sort a person's incoming email into exactly one category.

Job-search categories (only for email about the person's OWN job search):
- application_confirmation: an employer or applicant-tracking system acknowledging an application the recipient submitted, or a status update on it.
- rejection: the employer declines to move forward with the recipient's candidacy.
- interview_next_steps: scheduling or confirming interviews or screens, take-home assignments, coding assessments, reference checks, requests for more information, or the recipient's own emails to hiring contacts.
- offer: a job offer, offer letter, or offer negotiation.
- recruiter_outreach: a recruiter or hiring manager reaching out about a role or asking to talk about opportunities.
- job_alert: automated job listings, "jobs you may be interested in", job-board digests.

- promotional: marketing sent by a business or organization to sell or promote something: sales, discounts,
  deals, product launches and feature announcements, brand newsletters, "we miss you" / re-engagement emails,
  event, webinar and conference promotions, course or bootcamp marketing, fundraising appeals, surveys and
  review requests.

- other: everything else, including personal and work correspondence, receipts, order and shipping updates,
  bills and statements, account and security notices, appointment confirmations, editorial newsletters or
  publications the person reads, social notifications, and LinkedIn posts or messages not about a specific job.

Judge by meaning, not keywords: "your application" for a credit card, apartment, or grant is not a job category.
Only use a job category if the email concerns hiring for a specific role, the person's candidacy, or is an
automated list of job openings. A receipt or shipping notice is "other" even if it advertises something.
When unsure, choose "other".

reason: first, briefly say what the email actually is (under 20 words).
confidence: your probability (0 to 1) that the category is correct.
company: the hiring company or sending business, or "" if none."""

SCHEMA = {
    "type": "object",
    "properties": {
        "reason": {"type": "string"},
        "category": {"type": "string", "enum": list(CATEGORIES)},
        "confidence": {"type": "number"},
        "company": {"type": "string"},
    },
    "required": ["reason", "category", "confidence", "company"],
}

# Retained on the first run only; after that promo_domains.csv is the source of truth.
SEED_RETAIN = {"steampowered.com", "steamcommunity.com"}

TRUTHY = {"true", "yes", "y", "1", "x"}
TWO_LEVEL_SUFFIXES = {
    "co.uk",
    "org.uk",
    "ac.uk",
    "gov.uk",
    "com.au",
    "net.au",
    "org.au",
    "co.nz",
    "co.jp",
    "co.in",
    "com.br",
    "com.mx",
    "co.za",
    "com.sg",
    "com.hk",
    "com.cn",
}

# Gmail search syntax; {a b c} means a OR b OR c.
DEFAULT_PREFILTER = (
    '{"thank you for applying" "thanks for applying" "your application" "application received" '
    '"application for" "applied for" "unfortunately" "move forward" "other candidates" '
    'interview recruiter recruiting "talent acquisition" "hiring team" "hiring manager" '
    '"job alert" "jobs you may" position role opportunity candidate offer '
    "from:greenhouse.io from:greenhouse-mail.io from:lever.co from:hire.lever.co "
    "from:myworkday.com from:workday.com from:ashbyhq.com from:smartrecruiters.com "
    "from:icims.com from:jobvite.com from:bamboohr.com from:jazzhr.com from:workable.com "
    "from:rippling.com from:dover.com from:linkedin.com from:indeed.com from:wellfound.com "
    "from:otta.com from:welcometothejungle.com from:hired.com from:dice.com from:ziprecruiter.com}"
)


# ---------------------------------------------------------------- helpers


def decode(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def extract_text(msg, limit=2500):
    plain, html, attachments = [], [], []
    for part in msg.walk():
        if part.get_content_maintype() == "multipart":
            continue
        if part.get_filename():
            attachments.append(decode(part.get_filename()))
            continue
        ctype = part.get_content_type()
        if ctype not in ("text/plain", "text/html"):
            continue
        try:
            payload = part.get_payload(decode=True) or b""
            text = payload.decode(
                part.get_content_charset() or "utf-8", errors="replace"
            )
        except (LookupError, AssertionError):
            continue
        (plain if ctype == "text/plain" else html).append(text)

    text = re.sub(r"\s+", " ", "\n".join(plain)).strip()
    if len(text) < 300 and html:  # plain part missing or a "view in browser" stub
        raw = "\n".join(html)
        if BeautifulSoup:
            soup = BeautifulSoup(raw, "html.parser")
            for tag in soup(["style", "script", "head"]):
                tag.decompose()
            html_text = soup.get_text(" ")
        else:
            html_text = re.sub(
                r"<(style|script|head)\b.*?</\1>|<[^>]+>", " ", raw, flags=re.S | re.I
            )
        html_text = re.sub(r"\s+", " ", html_text).strip()
        if len(html_text) > len(text):
            text = html_text
    return text[:limit], attachments


def unsubscribe_links(msg):
    links = re.findall(r"<([^>]+)>", decode(msg.get("List-Unsubscribe")))
    url = next((l for l in links if l.lower().startswith("http")), "")
    mailto = next((l for l in links if l.lower().startswith("mailto:")), "")
    one_click = "one-click" in (msg.get("List-Unsubscribe-Post") or "").lower()
    return url, mailto, int(one_click and bool(url))


def quote_mailbox(name):
    return '"' + name.replace("\\", "\\\\").replace('"', '\\"') + '"'


def find_special(M, flag, default):
    typ, lines = M.list()
    for line in lines or []:
        if line and flag in line:
            return line.rsplit(b' "/" ', 1)[-1].strip().strip(b'"').decode()
    return default


def gmail_search(M, query):
    M.literal = query.encode(
        "utf-8"
    )  # sent as an IMAP literal, so no quote escaping headaches
    typ, data = M.uid("SEARCH", "CHARSET", "UTF-8", "X-GM-RAW")
    if typ != "OK":
        raise RuntimeError(f"search failed: {data}")
    return data[0].split() if data and data[0] else []


def fetch_msgids(M, uids, chunk=500):
    """uid -> Gmail's stable X-GM-MSGID (UIDs are per-folder; MSGIDs are global)."""
    out = {}
    for i in range(0, len(uids), chunk):
        batch = b",".join(uids[i : i + chunk]).decode()
        typ, data = M.uid("FETCH", batch, "(X-GM-MSGID)")
        for item in data or []:
            line = item[0] if isinstance(item, tuple) else item
            if not isinstance(line, bytes):
                continue
            u = re.search(rb"UID (\d+)", line)
            g = re.search(rb"X-GM-MSGID (\d+)", line)
            if u and g:
                out[u.group(1).decode()] = g.group(1).decode()
    return out


def fetch_message(M, uid):
    typ, data = M.uid("FETCH", uid, "(BODY.PEEK[])")  # PEEK = don't mark as read
    for item in data or []:
        if isinstance(item, tuple):
            return email.message_from_bytes(item[1])
    return None


def classify(host, model, prompt_text):
    r = requests.post(
        f"{host}/api/chat",
        json={
            "model": model,
            "stream": False,
            "format": SCHEMA,
            "options": {"temperature": 0, "num_ctx": 8192},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt_text},
            ],
        },
        timeout=300,
    )
    r.raise_for_status()
    try:
        out = json.loads(r.json()["message"]["content"])
    except (KeyError, json.JSONDecodeError):
        out = {}
    if out.get("category") not in CATEGORIES:
        out = {
            "category": "other",
            "confidence": 0.0,
            "company": "",
            "reason": "unparseable model output",
        }
    out["confidence"] = float(out.get("confidence") or 0)
    return out


COLUMNS = [
    "msgid",
    "date",
    "sender",
    "subject",
    "category",
    "confidence",
    "company",
    "reason",
    "model",
    "applied",
    "unsub_url",
    "unsub_mailto",
    "one_click",
]


def open_db(path):
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS decisions (
            msgid TEXT PRIMARY KEY, date TEXT, sender TEXT, subject TEXT,
            category TEXT, confidence REAL, company TEXT, reason TEXT,
            model TEXT, applied TEXT DEFAULT '')"""
    )
    have = {r[1] for r in con.execute("PRAGMA table_info(decisions)")}
    for col, typ in [
        ("unsub_url", "TEXT"),
        ("unsub_mailto", "TEXT"),
        ("one_click", "INTEGER"),
    ]:
        if col not in have:
            con.execute(f"ALTER TABLE decisions ADD COLUMN {col} {typ}")
    return con


# ---------------------------------------------------------------- actions


def base_domain(sender):
    """news.store.steampowered.com -> steampowered.com"""
    addr = parseaddr(sender or "")[1].lower()
    host = addr.rsplit("@", 1)[-1].strip(".") if "@" in addr else ""
    parts = host.split(".")
    if len(parts) >= 3 and ".".join(parts[-2:]) in TWO_LEVEL_SUFFIXES:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def load_retain(path):
    """domain -> bool from the user's edited CSV; seeds defaults if the file doesn't exist yet."""
    if not os.path.exists(path):
        return {d: True for d in SEED_RETAIN}
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return {
            r["domain"].strip().lower(): (r.get("retain") or "").strip().lower()
            in TRUTHY
            for r in csv.DictReader(fh)
            if r.get("domain")
        }


def plan(category, confidence, args, domain="", retain=None):
    """Returns (description, ops). ops is None when nothing should happen."""
    group, sub = CATEGORIES[category]
    if group == "job" and not (category == "job_alert" and args.skip_alerts):
        if confidence < args.min_confidence:
            return "review (low confidence)", None
        labels = [args.label] + ([f"{args.label}/{sub}"] if args.sublabels else [])
        ops = {"add": labels, "archive": args.archive, "trash": False}
    elif group == "promo" and args.promos != "off":
        if retain and retain.get(domain):
            return f"retained ({domain})", None
        if confidence < args.promo_min_confidence:
            return "review (low confidence)", None
        if args.promos == "trash":
            ops = {"add": [], "archive": False, "trash": True}
        else:
            ops = {
                "add": [args.promo_label],
                "archive": args.promos == "archive",
                "trash": False,
            }
    else:
        return "no action", None

    if ops["trash"]:
        desc = "move to Trash"
    else:
        desc = " ".join(f"+{l}" for l in ops["add"]) + (
            ", archive" if ops["archive"] else ""
        )
    return desc, ops


def apply_ops(M, uid, ops, trash_folder):
    if ops["trash"]:
        typ, _ = M.uid("MOVE", uid, quote_mailbox(trash_folder))
        if typ != "OK":
            M.uid("COPY", uid, quote_mailbox(trash_folder))
        return
    if ops["add"]:
        M.uid(
            "STORE",
            uid,
            "+X-GM-LABELS",
            "(" + " ".join(quote_mailbox(l) for l in ops["add"]) + ")",
        )
    if ops["archive"]:
        M.uid("STORE", uid, "-X-GM-LABELS", "(\\Inbox)")


def write_domain_report(con, path, retain):
    """Promotional mail by sending domain, most recent first. Preserves the user's retain column."""
    dates = [d for (d,) in con.execute("SELECT date FROM decisions WHERE date != ''")]
    if dates:
        span_days = (
            datetime.date.fromisoformat(max(dates))
            - datetime.date.fromisoformat(min(dates))
        ).days
        months = max(1.0, span_days / 30.44)
    else:
        months = 1.0

    stats = {}
    for sender, date, subject, url, one_click in con.execute(
        "SELECT sender, date, subject, unsub_url, one_click FROM decisions WHERE category='promotional'"
    ):
        d = base_domain(sender) or (sender or "").lower()
        date = date or ""
        s = stats.setdefault(
            d,
            {
                "count": 0,
                "first": date,
                "last": "",
                "from": "",
                "subject": "",
                "url": "",
                "one_click": 0,
            },
        )
        s["count"] += 1
        s["first"] = min(s["first"], date) if s["first"] else date
        if date >= s["last"]:
            s.update(last=date, subject=subject or "", **{"from": sender or ""})
            if url:
                s["url"], s["one_click"] = url, one_click or 0
    for d in retain:  # keep rows the user edited, even if not seen this time
        stats.setdefault(
            d,
            {
                "count": 0,
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
                "first_seen",
                "latest_from",
                "latest_subject",
                "one_click_unsubscribe",
                "unsubscribe_url",
            ]
        )
        for d, s in sorted(
            stats.items(), key=lambda kv: (kv[1]["last"], kv[1]["count"]), reverse=True
        ):
            w.writerow(
                [
                    d,
                    s["last"],
                    round(s["count"] / months, 1),
                    "TRUE" if retain.get(d) else "FALSE",
                    s["count"],
                    s["first"],
                    s["from"],
                    s["subject"],
                    "yes" if s["one_click"] else "",
                    s["url"],
                ]
            )
    return len(stats)


# ---------------------------------------------------------------- main


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--query",
        default="newer_than:2y",
        help="Gmail search scoping the run (default: newer_than:2y)",
    )
    p.add_argument(
        "--prefilter",
        action="store_true",
        help="only classify job keyword/sender matches (skips promo detection elsewhere)",
    )
    p.add_argument("--label", default="Job Applications")
    p.add_argument(
        "--sublabels",
        action="store_true",
        help="also add Label/Rejections, Label/Interviews, ...",
    )
    p.add_argument(
        "--skip-alerts",
        action="store_true",
        help="don't label automated job-alert digests",
    )
    p.add_argument(
        "--min-confidence", type=float, default=0.6, help="threshold for job labels"
    )
    p.add_argument(
        "--archive",
        action="store_true",
        help="also remove job-labeled messages from the Inbox",
    )
    p.add_argument(
        "--promos",
        choices=["off", "label", "archive", "trash"],
        default="archive",
        help="what to do with promotional mail (default: archive = label + remove from Inbox)",
    )
    p.add_argument("--promo-label", default="Promotional")
    p.add_argument(
        "--domains-file",
        default="promo_domains.csv",
        help="per-domain promo report; edit its retain column to keep a domain's mail",
    )
    p.add_argument(
        "--sweep-file",
        default="sweep_domains.csv",
        help="promo_sweep.py's domain report (retain honored)",
    )
    p.add_argument(
        "--promo-min-confidence",
        type=float,
        default=0.8,
        help="threshold for promo actions",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="classify and log only; read-only IMAP session",
    )
    p.add_argument(
        "--reclassify", action="store_true", help="ignore cached classifications"
    )
    p.add_argument(
        "--limit", type=int, default=0, help="only process the N most recent candidates"
    )
    p.add_argument(
        "--model", default=os.environ.get("OLLAMA_MODEL", "qwen2.5:7b-instruct")
    )
    p.add_argument(
        "--ollama", default=os.environ.get("OLLAMA_HOST", "http://localhost:11434")
    )
    p.add_argument("--db", default="job_mail_cache.sqlite")
    args = p.parse_args()

    user, pw = os.environ.get("GMAIL_USER"), os.environ.get("GMAIL_APP_PASSWORD")
    if not (user and pw):
        sys.exit("Set GMAIL_USER and GMAIL_APP_PASSWORD.")

    con = open_db(args.db)
    retain = load_retain(args.domains_file)
    if os.path.exists(
        args.sweep_file
    ):  # retain = TRUE in promo_sweep.py's report counts too
        retain.update(
            {d: True for d, keep in load_retain(args.sweep_file).items() if keep}
        )
    M = imaplib.IMAP4_SSL("imap.gmail.com")
    M.login(user, pw.replace(" ", ""))
    trash_folder = find_special(M, b"\\Trash", "[Gmail]/Trash")
    M.select(
        quote_mailbox(find_special(M, b"\\All", "[Gmail]/All Mail")),
        readonly=args.dry_run,
    )

    query = f"({args.query}) {DEFAULT_PREFILTER}" if args.prefilter else args.query
    uids = gmail_search(M, query)
    uids.sort(key=int, reverse=True)  # newest first
    if args.limit:
        uids = uids[: args.limit]
    msgids = fetch_msgids(M, uids)
    print(f"{len(msgids)} candidate messages")

    if not args.dry_run:
        names = [args.label] + (
            [f"{args.label}/{s}" for g, s in CATEGORIES.values() if s]
            if args.sublabels
            else []
        )
        if args.promos in ("label", "archive"):
            names.append(args.promo_label)
        for name in names:
            M.create(quote_mailbox(name))  # returns NO if it already exists; harmless

    stamp = time.strftime("%Y%m%d-%H%M%S")
    csv_path = f"decisions_{stamp}.csv"
    counts = defaultdict(int)
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "date",
                "from",
                "subject",
                "category",
                "confidence",
                "company",
                "reason",
                "action",
                "gmail_msgid",
            ]
        )

        for n, (uid, msgid) in enumerate(msgids.items(), 1):
            row = con.execute(
                "SELECT date, sender, subject, category, confidence, company, reason, applied FROM decisions WHERE msgid=?",
                (msgid,),
            ).fetchone()

            if row and row[3] in CATEGORIES and not args.reclassify:
                (
                    date,
                    sender,
                    subject,
                    category,
                    confidence,
                    company,
                    reason,
                    applied,
                ) = row
            else:
                msg = fetch_message(M, uid)
                if msg is None:
                    continue
                sender, subject = decode(msg.get("From")), decode(msg.get("Subject"))
                try:
                    date = parsedate_to_datetime(msg.get("Date")).strftime("%Y-%m-%d")
                except Exception:
                    date = ""
                body, attachments = extract_text(msg)
                prompt = (
                    f"From: {sender}\nTo: {decode(msg.get('To'))}\nDate: {date}\nSubject: {subject}\n"
                    f"Attachments: {', '.join(attachments) or 'none'}\n\n{body}"
                )
                res = classify(args.ollama, args.model, prompt)
                category, confidence = res["category"], res["confidence"]
                company, reason = res.get("company", ""), res.get("reason", "")
                applied = (row[7] if row else "") or ""
                unsub_url, unsub_mailto, one_click = unsubscribe_links(msg)
                con.execute(
                    f"INSERT OR REPLACE INTO decisions ({','.join(COLUMNS)}) VALUES ({','.join('?' * len(COLUMNS))})",
                    (
                        msgid,
                        date,
                        sender,
                        subject,
                        category,
                        confidence,
                        company,
                        reason,
                        args.model,
                        applied,
                        unsub_url,
                        unsub_mailto,
                        one_click,
                    ),
                )
                con.commit()

            desc, ops = plan(category, confidence, args, base_domain(sender), retain)
            if ops is None:
                action = desc
            elif str(applied) == desc:
                action = f"already done: {desc}"
            elif args.dry_run:
                action = f"would: {desc}"
            else:
                apply_ops(M, uid, ops, trash_folder)
                con.execute(
                    "UPDATE decisions SET applied=? WHERE msgid=?", (desc, msgid)
                )
                con.commit()
                action = f"done: {desc}"

            counts[action] += 1
            w.writerow(
                [
                    date,
                    sender,
                    subject,
                    category,
                    f"{confidence:.2f}",
                    company,
                    reason,
                    action,
                    msgid,
                ]
            )
            fh.flush()
            print(f"[{n}/{len(msgids)}] {category:24} {action:45} {subject[:60]}")

    M.logout()
    print()
    for action, c in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"{c:6}  {action}")
    print(f"\nDecisions written to {csv_path}")
    n_domains = write_domain_report(con, args.domains_file, retain)
    print(
        f"{n_domains} promotional domains written to {args.domains_file} (edit its retain column)"
    )


if __name__ == "__main__":
    main()
