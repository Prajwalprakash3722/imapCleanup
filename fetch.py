"""
Email fetcher - orchestrates IMAP fetching and SQLite storage.

This module handles:
- Parsing raw IMAP headers into structured data
- Extracting sender email/name from From header
- Converting dates to ISO8601 for reliable sorting
- Batched fetching with progress reporting
- Incremental sync (only fetch new emails)
"""

import email
import email.utils
import email.header
import re
from datetime import datetime, timezone
from typing import Optional

from imap_client import GmailIMAPClient, load_config_from_env
from db import (
    get_connection, init_db, insert_emails_batch,
    get_sync_state, update_sync_state, get_deleted_uids
)


def decode_header_value(value: Optional[str]) -> str:
    """
    Decode RFC 2047 encoded header values (e.g., =?utf-8?Q?...?=).

    Email headers can contain encoded text for non-ASCII characters.
    This decodes them to plain Unicode strings.
    """
    if not value:
        return ""

    try:
        # email.header.decode_header returns list of (decoded_bytes, charset) tuples
        decoded_parts = email.header.decode_header(value)
        result = []

        for data, charset in decoded_parts:
            if isinstance(data, bytes):
                # Decode bytes using specified charset (fallback to utf-8, then latin-1)
                encoding = charset or "utf-8"
                try:
                    result.append(data.decode(encoding))
                except (UnicodeDecodeError, LookupError):
                    # Some emails have bogus charset declarations
                    try:
                        result.append(data.decode("utf-8", errors="replace"))
                    except Exception:
                        result.append(data.decode("latin-1", errors="replace"))
            else:
                result.append(str(data))

        return "".join(result)
    except Exception:
        # If all else fails, return original value
        return str(value) if value else ""


def extract_email_address(from_header: str) -> tuple[str, str]:
    """
    Extract email address and display name from From header.

    Handles formats:
    - "Name <email@example.com>"
    - "email@example.com"
    - "<email@example.com>"
    - "email@example.com (Name)"

    Returns:
        Tuple of (email_address, display_name), both may be empty strings
    """
    if not from_header:
        return "", ""

    # Use email.utils.parseaddr - handles most edge cases
    name, addr = email.utils.parseaddr(from_header)

    # Normalize email to lowercase
    addr = addr.lower().strip() if addr else ""

    # Decode name if it's RFC 2047 encoded
    name = decode_header_value(name).strip()

    return addr, name


def parse_date(date_header: str) -> Optional[str]:
    """
    Parse email Date header to ISO8601 format.

    Email dates are notoriously inconsistent. This handles:
    - RFC 2822 format: "Mon, 1 Jan 2024 12:00:00 +0000"
    - Various broken formats that email.utils can salvage

    Returns:
        ISO8601 string (YYYY-MM-DDTHH:MM:SS+00:00) or None if unparseable
    """
    if not date_header:
        return None

    try:
        # email.utils.parsedate_to_datetime handles most email date formats
        dt = email.utils.parsedate_to_datetime(date_header)

        # Convert to UTC for consistent sorting
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.isoformat()
    except Exception:
        # Some emails have completely broken dates
        return None


def parse_headers(raw_headers: bytes, size: int, uid: int) -> dict:
    """
    Parse raw IMAP headers into a structured dict for database storage.

    Args:
        raw_headers: Raw header bytes from IMAP FETCH
        size: RFC822.SIZE in bytes
        uid: IMAP UID

    Returns:
        Dict ready for insert_email()
    """
    # Parse headers using email module
    msg = email.message_from_bytes(raw_headers)

    # Extract fields
    from_raw = decode_header_value(msg.get("From", ""))
    to_raw = decode_header_value(msg.get("To", ""))
    subject = decode_header_value(msg.get("Subject", ""))
    date_header = msg.get("Date", "")
    message_id = msg.get("Message-ID", "")

    # Extract sender email and name
    sender_email, sender_name = extract_email_address(from_raw)

    # Parse date to ISO8601
    date_parsed = parse_date(date_header)

    return {
        "uid": uid,
        "message_id": message_id.strip() if message_id else None,
        "sender_raw": from_raw,
        "sender_email": sender_email,
        "sender_name": sender_name,
        "recipient_raw": to_raw,
        "subject": subject,
        "date_header": date_header,
        "date_parsed": date_parsed,
        "size_bytes": size
    }


def fetch_all(
    batch_size: int = 100,
    progress_callback=None,
    incremental: bool = True
) -> dict:
    """
    Fetch all emails from Gmail and store in SQLite.

    Args:
        batch_size: Number of emails to fetch per IMAP request
        progress_callback: Optional callable(fetched, total, new_count) for progress updates
        incremental: If True, only fetch emails newer than last sync

    Returns:
        Dict with stats: {"total_fetched", "new_stored", "already_existed", "errors"}
    """
    # Initialize database
    init_db()
    conn = get_connection()

    # Load IMAP config and connect
    config = load_config_from_env()

    stats = {
        "total_fetched": 0,
        "new_stored": 0,
        "already_existed": 0,
        "errors": 0
    }

    with GmailIMAPClient(config) as client:
        # Get all UIDs (or just new ones for incremental)
        if incremental:
            sync_state = get_sync_state(conn)
            last_uid = sync_state["last_uid"]

            if last_uid > 0:
                print(f"Incremental sync: fetching UIDs > {last_uid}")
                uids = client.search_since_uid(last_uid)
            else:
                print("First sync: fetching all UIDs")
                uids = client.search_all()
        else:
            print("Full sync: fetching all UIDs")
            uids = client.search_all()

        # Filter out UIDs we've previously deleted
        deleted_uids = get_deleted_uids(conn)
        uids = [uid for uid in uids if uid not in deleted_uids]

        total = len(uids)
        print(f"Found {total} emails to fetch")

        if total == 0:
            print("No new emails to fetch")
            return stats

        # Fetch in batches
        fetched = 0
        batch_emails = []

        for batch_data in client.fetch_headers_batch(uids, batch_size=batch_size):
            for uid, data in batch_data.items():
                try:
                    email_record = parse_headers(
                        raw_headers=data["headers"],
                        size=data["size"],
                        uid=uid
                    )
                    batch_emails.append(email_record)
                    fetched += 1
                except Exception as e:
                    print(f"Error parsing UID {uid}: {e}")
                    stats["errors"] += 1

            # Insert batch into database
            if len(batch_emails) >= batch_size:
                new_count = insert_emails_batch(conn, batch_emails)
                stats["new_stored"] += new_count
                stats["already_existed"] += len(batch_emails) - new_count
                batch_emails = []

            # Progress callback
            if progress_callback:
                progress_callback(fetched, total, stats["new_stored"])
            else:
                pct = (fetched / total) * 100 if total > 0 else 0
                print(f"\rProgress: {fetched}/{total} ({pct:.1f}%)", end="", flush=True)

        # Insert remaining emails
        if batch_emails:
            new_count = insert_emails_batch(conn, batch_emails)
            stats["new_stored"] += new_count
            stats["already_existed"] += len(batch_emails) - new_count

        # Update sync state
        if uids:
            max_uid = max(uids)
            uidnext = client.get_uidnext()
            update_sync_state(conn, max_uid, uidnext)

    stats["total_fetched"] = fetched
    print()  # Newline after progress
    conn.close()

    return stats


def fetch_sample(count: int = 10) -> list[dict]:
    """
    Fetch a small sample of recent emails (for testing/debugging).

    Does NOT store in database.

    Returns:
        List of parsed email dicts
    """
    config = load_config_from_env()

    with GmailIMAPClient(config) as client:
        uids = client.search_all()

        # Get most recent UIDs
        recent_uids = sorted(uids, reverse=True)[:count]

        batch = client.fetch_headers(recent_uids)

        results = []
        for uid, data in batch.items():
            parsed = parse_headers(
                raw_headers=data["headers"],
                size=data["size"],
                uid=uid
            )
            results.append(parsed)

        return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--sample":
        # Fetch a sample without storing
        print("Fetching 10 most recent emails (not storing)...\n")
        samples = fetch_sample(10)

        for email_data in samples:
            print(f"UID: {email_data['uid']}")
            print(f"  From: {email_data['sender_email']} ({email_data['sender_name']})")
            print(f"  Subject: {email_data['subject'][:60]}...")
            print(f"  Date: {email_data['date_parsed']}")
            print(f"  Size: {email_data['size_bytes']:,} bytes")
            print()
    else:
        # Full fetch
        print("Starting email fetch...")
        stats = fetch_all()

        print("\n--- Fetch Complete ---")
        print(f"Total fetched: {stats['total_fetched']}")
        print(f"New stored: {stats['new_stored']}")
        print(f"Already existed: {stats['already_existed']}")
        print(f"Errors: {stats['errors']}")
