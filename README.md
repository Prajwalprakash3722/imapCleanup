# Gmail Cleanup

I had 50,000+ emails. Gmail search is slow. I wanted to nuke all those Swiggy, Zomato, and GitHub notification emails in one go.

So I (By I mean LLM) built this, this was inspired by https://x.com/mrkaran_/status/2006054634918711429?s=20, @mr-karan

## What it does

1. **Fetches** all your email headers via IMAP → stores in SQLite
2. **Query** with SQL (way faster than Gmail search)
3. **Bulk delete** thousands of emails by sender pattern

```
$ uv run main.py cleanup swiggy zomato groww --delete

Emails matching patterns: ['swiggy', 'zomato', 'groww']
======================================================================
Sender                                        Count   Size (MB)
----------------------------------------------------------------------
noreply@swiggy.in                              2,847      45.23
no-reply@zomato.com                            1,234      23.45
updates@groww.in                                 892      12.34
----------------------------------------------------------------------
TOTAL                                          4,973      81.02

Will delete 4,973 emails. Type 'DELETE 4973' to confirm:
```

Gone. Forever. No more scrolling through pages of food delivery receipts.

## Setup

### 1. Get a Gmail App Password

You need an "App Password" (not your regular Gmail password):

1. Go to [Google App Passwords](https://myaccount.google.com/apppasswords)
2. Select "Mail" and generate
3. Copy the 16-character password

> **Note:** You need 2FA enabled on your Google account for this to work.

### 2. Create `.env` file

```bash
GMAIL_EMAIL=you@gmail.com
GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
```

### 3. Install & run

```bash
# Install dependencies
uv sync

# Fetch all your emails (first run takes a few minutes)
uv run main.py fetch

# See what you've got
uv run main.py stats
```

## Usage

### See the damage

```bash
# Overview
uv run main.py stats

# Who's spamming you the most?
uv run main.py top-senders

# Find newsletter/automated senders
uv run main.py newsletters
```

### Query like a boss

```bash
# Raw SQL access to your emails
uv run main.py query "SELECT sender_email, COUNT(*) as c FROM emails GROUP BY sender_email ORDER BY c DESC LIMIT 20"

# How much space is GitHub using?
uv run main.py query "SELECT SUM(size_bytes)/1024/1024 as mb FROM emails WHERE sender_email LIKE '%github%'"

# Emails older than 2 years
uv run main.py query "SELECT COUNT(*) FROM emails WHERE date_parsed < '2023-01-01'"
```

### Clean up

```bash
# Preview what would be deleted
uv run main.py cleanup swiggy zomato dunzo

# See sample emails
uv run main.py cleanup github.com --show

# Actually delete (asks for confirmation)
uv run main.py cleanup swiggy zomato --delete

# YOLO mode (no confirmation - be careful!)
uv run main.py cleanup newsletters@spam.com --delete --yes
```

### Incremental sync

After the first fetch, subsequent runs only grab new emails:

```bash
# Only fetches emails newer than last sync
uv run main.py fetch

# Force full re-fetch if needed
uv run main.py fetch --full
```

## How it works

```
Gmail (IMAP) ──fetch──> SQLite (gmail.db) ──query──> You
                                               │
                                               └──delete──> Gmail (IMAP)
```

1. Connects to Gmail via IMAP (port 993, SSL)
2. Fetches headers from `[Gmail]/All Mail` (contains everything)
3. Stores in local SQLite: sender, subject, date, size, etc.
4. You query locally (instant) instead of Gmail search (slow)
5. Deletion sends IMAP commands to mark emails as deleted

## Files

```
main.py         # CLI entry point
fetch.py        # IMAP fetch logic
imap_client.py  # Gmail IMAP wrapper
db.py           # SQLite operations
delete.py       # Deletion logic
gmail.db        # Your email database (created on first run)
.env            # Your credentials (don't commit this!)
```

## Database schema

```sql
CREATE TABLE emails (
    uid INTEGER PRIMARY KEY,
    message_id TEXT,
    sender_name TEXT,
    sender_email TEXT,
    subject TEXT,
    date_raw TEXT,
    date_parsed DATETIME,
    size_bytes INTEGER,
    headers_raw TEXT,
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

## Caveats

- **Gmail rate limits**: Fetching 50k+ emails takes time. The tool handles this with batching and delays.
- **Deletion is real**: When you delete, emails go to Trash. They're permanently deleted after 30 days (or immediately if you empty Trash).
- **Headers only**: We only fetch headers (From, Subject, Date), not email bodies. This is faster and uses less bandwidth.
- **App Password required**: Regular passwords don't work if you have 2FA (which you should).

## Why not just use Gmail's UI?

- Can't easily query "all emails from domains matching X"
- Can't bulk delete more than 50 at a time
- Search is slow on large mailboxes
- No way to see "top senders by count/size"
- Can't do SQL queries like "emails older than X grouped by sender"

## License

Do whatever you want with it. It's just a script.

---

*Built because I was mass deleting emails 50 at a time like a caveman.*
