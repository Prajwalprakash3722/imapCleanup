"""
SQLite database layer for Gmail email metadata storage.

Schema Design Notes:
- UID is the primary key because Gmail UIDs are stable within a mailbox
- Message-ID is indexed for deduplication (Gmail shows same email in multiple folders)
- sender_email is extracted and normalized for analytics queries
- All text fields use COLLATE NOCASE for case-insensitive matching

Gmail IMAP Quirk:
- The same email appears in multiple "folders" (labels) with the same UID in [Gmail]/All Mail
- We only fetch from [Gmail]/All Mail to avoid duplicates
- UIDs are only unique per-mailbox, but we only use one mailbox so this is fine
"""

import sqlite3
from pathlib import Path
from typing import Optional

# Default database path
DEFAULT_DB_PATH = Path(__file__).parent / "gmail.db"


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Get a database connection with optimal settings for our use case."""
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path)

    # Enable foreign keys and WAL mode for better concurrent access
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")

    # Return rows as sqlite3.Row for dict-like access
    conn.row_factory = sqlite3.Row

    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    """
    Initialize the database schema.

    Safe to call multiple times (uses IF NOT EXISTS).
    """
    conn = get_connection(db_path)

    conn.executescript("""
        -- Main emails table
        -- UID is primary key: stable identifier within Gmail's All Mail
        CREATE TABLE IF NOT EXISTS emails (
            uid             INTEGER PRIMARY KEY,  -- IMAP UID from [Gmail]/All Mail
            message_id      TEXT,                 -- RFC Message-ID header (for dedup reference)
            sender_raw      TEXT,                 -- Full From header as-is
            sender_email    TEXT COLLATE NOCASE,  -- Extracted email address, lowercased
            sender_name     TEXT COLLATE NOCASE,  -- Extracted display name
            recipient_raw   TEXT,                 -- Full To header as-is
            subject         TEXT COLLATE NOCASE,  -- Subject line
            date_header     TEXT,                 -- Raw Date header string
            date_parsed     TEXT,                 -- ISO8601 parsed date (for sorting)
            size_bytes      INTEGER,              -- RFC822.SIZE
            fetched_at      TEXT DEFAULT (datetime('now'))  -- When we fetched this
        );

        -- Indexes for common analytics queries
        -- sender_email: "top senders by count"
        CREATE INDEX IF NOT EXISTS idx_sender_email ON emails(sender_email);

        -- date_parsed: "emails older than X"
        CREATE INDEX IF NOT EXISTS idx_date_parsed ON emails(date_parsed);

        -- size_bytes: "largest emails"
        CREATE INDEX IF NOT EXISTS idx_size_bytes ON emails(size_bytes);

        -- message_id: deduplication lookups
        CREATE INDEX IF NOT EXISTS idx_message_id ON emails(message_id);

        -- Composite index for sender analytics (count + size)
        CREATE INDEX IF NOT EXISTS idx_sender_size ON emails(sender_email, size_bytes);

        -- Track sync state to enable incremental fetches
        CREATE TABLE IF NOT EXISTS sync_state (
            id              INTEGER PRIMARY KEY CHECK (id = 1),  -- Singleton row
            last_uid        INTEGER,              -- Highest UID we've fetched
            last_sync       TEXT,                 -- Timestamp of last sync
            total_messages  INTEGER               -- UIDNEXT or message count at last sync
        );

        -- Initialize sync state if not present
        INSERT OR IGNORE INTO sync_state (id, last_uid, last_sync, total_messages)
        VALUES (1, 0, NULL, 0);

        -- Track deleted UIDs to avoid re-fetching tombstones
        -- (Gmail doesn't immediately remove UIDs after deletion)
        CREATE TABLE IF NOT EXISTS deleted_uids (
            uid             INTEGER PRIMARY KEY,
            deleted_at      TEXT DEFAULT (datetime('now'))
        );
    """)

    conn.commit()
    conn.close()


def insert_email(conn: sqlite3.Connection, email_data: dict) -> bool:
    """
    Insert an email record. Uses INSERT OR IGNORE to handle duplicates.

    Args:
        conn: Database connection
        email_data: Dict with keys matching column names

    Returns:
        True if inserted, False if already existed (duplicate UID)
    """
    cursor = conn.execute("""
        INSERT OR IGNORE INTO emails (
            uid, message_id, sender_raw, sender_email, sender_name,
            recipient_raw, subject, date_header, date_parsed, size_bytes
        ) VALUES (
            :uid, :message_id, :sender_raw, :sender_email, :sender_name,
            :recipient_raw, :subject, :date_header, :date_parsed, :size_bytes
        )
    """, email_data)

    return cursor.rowcount > 0


def insert_emails_batch(conn: sqlite3.Connection, emails: list[dict]) -> int:
    """
    Insert multiple emails in a single transaction.

    Returns:
        Number of new emails inserted (excludes duplicates)
    """
    inserted = 0
    for email_data in emails:
        if insert_email(conn, email_data):
            inserted += 1
    conn.commit()
    return inserted


def get_sync_state(conn: sqlite3.Connection) -> dict:
    """Get the current sync state."""
    row = conn.execute("SELECT * FROM sync_state WHERE id = 1").fetchone()
    return dict(row) if row else {"last_uid": 0, "last_sync": None, "total_messages": 0}


def update_sync_state(conn: sqlite3.Connection, last_uid: int, total_messages: int) -> None:
    """Update sync state after a fetch operation."""
    conn.execute("""
        UPDATE sync_state
        SET last_uid = ?, last_sync = datetime('now'), total_messages = ?
        WHERE id = 1
    """, (last_uid, total_messages))
    conn.commit()


def mark_deleted(conn: sqlite3.Connection, uids: list[int]) -> None:
    """
    Mark UIDs as deleted (after successful IMAP deletion).

    This removes them from the emails table and adds them to deleted_uids
    so we don't try to re-fetch them.
    """
    conn.executemany(
        "INSERT OR IGNORE INTO deleted_uids (uid) VALUES (?)",
        [(uid,) for uid in uids]
    )
    conn.executemany(
        "DELETE FROM emails WHERE uid = ?",
        [(uid,) for uid in uids]
    )
    conn.commit()


def get_email_count(conn: sqlite3.Connection) -> int:
    """Get total number of stored emails."""
    return conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]


def get_deleted_uids(conn: sqlite3.Connection) -> set[int]:
    """Get set of UIDs we've previously deleted."""
    rows = conn.execute("SELECT uid FROM deleted_uids").fetchall()
    return {row[0] for row in rows}


if __name__ == "__main__":
    # Quick test: initialize DB and print schema
    init_db()
    conn = get_connection()

    print("Database initialized at:", DEFAULT_DB_PATH)
    print("\nTables:")
    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        print(f"  - {row[0]}")

    print("\nSync state:", dict(get_sync_state(conn)))
    conn.close()
