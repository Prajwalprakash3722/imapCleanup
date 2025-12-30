"""
Email deletion engine.

CAUTION: This module performs DESTRUCTIVE operations.
Always use --dry-run first to preview what will be deleted.

Gmail deletion behavior:
1. Setting \Deleted flag moves email to Trash
2. EXPUNGE removes from current folder (All Mail)
3. Emails in Trash are auto-deleted after 30 days
4. To immediately permanently delete, you'd need to also delete from Trash

This module:
- Accepts UIDs from stdin, file, or direct argument
- Supports batch deletion with rate limiting
- Has mandatory dry-run mode by default
- Updates local SQLite database after deletion
"""

import sys
import time
from typing import TextIO

from imap_client import GmailIMAPClient, load_config_from_env
from db import get_connection, mark_deleted, get_connection

# Safety: batch size for deletion (smaller = safer, slower)
DELETE_BATCH_SIZE = 50

# Delay between deletion batches (seconds)
DELETE_BATCH_DELAY = 1.0


def read_uids_from_stream(stream: TextIO) -> list[int]:
    """
    Read UIDs from a text stream (stdin or file).

    Expected format: one UID per line, or comma-separated.
    Ignores empty lines and lines starting with #.
    """
    uids = []

    for line in stream:
        line = line.strip()

        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue

        # Handle comma-separated values on a line
        parts = line.replace(",", " ").split()

        for part in parts:
            try:
                uid = int(part)
                uids.append(uid)
            except ValueError:
                print(f"Warning: ignoring non-integer value: {part}", file=sys.stderr)

    return uids


def preview_deletion(uids: list[int]) -> None:
    """
    Preview what would be deleted (dry-run mode).

    Shows email details from local database for the given UIDs.
    """
    if not uids:
        print("No UIDs to delete.")
        return

    conn = get_connection()

    # Query emails from database
    placeholders = ",".join("?" * len(uids))
    cursor = conn.execute(f"""
        SELECT uid, sender_email, subject, date_parsed, size_bytes
        FROM emails
        WHERE uid IN ({placeholders})
        ORDER BY date_parsed DESC
    """, uids)

    rows = cursor.fetchall()

    print(f"\n{'='*60}")
    print(f"DRY RUN: Would delete {len(uids)} emails")
    print(f"{'='*60}\n")

    total_size = 0
    found_count = 0

    for row in rows:
        found_count += 1
        total_size += row["size_bytes"] or 0

        subject = (row["subject"] or "")[:50]
        size_kb = (row["size_bytes"] or 0) / 1024

        print(f"UID {row['uid']:>10}: {row['sender_email']:<40}")
        print(f"             Subject: {subject}...")
        print(f"             Date: {row['date_parsed']} | Size: {size_kb:.1f} KB")
        print()

    not_found = len(uids) - found_count

    print(f"{'='*60}")
    print(f"Summary:")
    print(f"  - UIDs provided: {len(uids)}")
    print(f"  - Found in database: {found_count}")
    print(f"  - Not in database: {not_found}")
    print(f"  - Total size: {total_size / 1024 / 1024:.2f} MB")
    print(f"{'='*60}")

    if not_found > 0:
        print(f"\nWarning: {not_found} UIDs not found in local database.")
        print("They may have been deleted already or not yet synced.")

    print("\nTo actually delete, run with --confirm flag")

    conn.close()


def delete_emails(
    uids: list[int],
    batch_size: int = DELETE_BATCH_SIZE,
    delay: float = DELETE_BATCH_DELAY,
    progress_callback=None
) -> dict:
    """
    Delete emails via IMAP and update local database.

    Args:
        uids: List of UIDs to delete
        batch_size: Number of emails per IMAP delete operation
        delay: Seconds to wait between batches
        progress_callback: Optional callable(deleted, total)

    Returns:
        Dict with stats: {"deleted", "errors", "not_found"}
    """
    if not uids:
        return {"deleted": 0, "errors": 0, "not_found": 0}

    stats = {"deleted": 0, "errors": 0, "not_found": 0}

    config = load_config_from_env()
    conn = get_connection()

    with GmailIMAPClient(config) as client:
        total = len(uids)

        for i in range(0, total, batch_size):
            batch = uids[i:i + batch_size]

            try:
                # Delete via IMAP
                deleted = client.delete_messages(batch, expunge=True)
                stats["deleted"] += deleted

                # Update local database
                mark_deleted(conn, batch)

                if progress_callback:
                    progress_callback(stats["deleted"], total)
                else:
                    pct = (stats["deleted"] / total) * 100
                    print(f"\rDeleted: {stats['deleted']}/{total} ({pct:.1f}%)", end="", flush=True)

            except Exception as e:
                print(f"\nError deleting batch starting at UID {batch[0]}: {e}", file=sys.stderr)
                stats["errors"] += len(batch)

            # Rate limit between batches
            if i + batch_size < total:
                time.sleep(delay)

    print()  # Newline after progress
    conn.close()

    return stats


def main():
    """CLI entry point for deletion."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Delete emails from Gmail via IMAP",
        epilog="""
Examples:
  # Dry run with UIDs from a query
  sqlite3 gmail.db "SELECT uid FROM emails WHERE sender_email LIKE '%newsletter%'" | python delete.py

  # Dry run with UIDs from file
  python delete.py --file uids_to_delete.txt

  # Dry run with inline UIDs
  python delete.py 12345 12346 12347

  # Actually delete (requires --confirm)
  python delete.py --confirm --file uids_to_delete.txt
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "uids",
        nargs="*",
        type=int,
        help="UIDs to delete (can also be provided via stdin or --file)"
    )

    parser.add_argument(
        "-f", "--file",
        type=argparse.FileType("r"),
        help="File containing UIDs (one per line)"
    )

    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually delete (without this flag, only previews)"
    )

    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip interactive confirmation (use with caution!)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DELETE_BATCH_SIZE,
        help=f"Emails per deletion batch (default: {DELETE_BATCH_SIZE})"
    )

    args = parser.parse_args()

    # Collect UIDs from all sources
    all_uids = list(args.uids)

    # From file
    if args.file:
        all_uids.extend(read_uids_from_stream(args.file))

    # From stdin (if not a TTY)
    if not sys.stdin.isatty():
        all_uids.extend(read_uids_from_stream(sys.stdin))

    # Deduplicate
    all_uids = sorted(set(all_uids))

    if not all_uids:
        print("No UIDs provided. Use --help for usage.", file=sys.stderr)
        sys.exit(1)

    print(f"Collected {len(all_uids)} unique UIDs")

    if args.confirm:
        # DANGER ZONE: Actually delete
        print("\n*** DELETION MODE ***")
        print("This will PERMANENTLY delete emails from Gmail!")

        # Double-check confirmation (skip if --yes is provided)
        if not args.yes:
            try:
                response = input(f"\nType 'DELETE {len(all_uids)}' to confirm: ")
                expected = f"DELETE {len(all_uids)}"

                if response != expected:
                    print("Confirmation failed. Aborting.")
                    sys.exit(1)
            except EOFError:
                print("\nError: Cannot read confirmation (stdin is piped).")
                print("Use --yes flag to skip confirmation when piping UIDs.")
                sys.exit(1)
        else:
            print(f"\n--yes flag provided, skipping confirmation...")

        print("\nStarting deletion...")
        stats = delete_emails(all_uids, batch_size=args.batch_size)

        print(f"\n{'='*40}")
        print("Deletion complete:")
        print(f"  - Deleted: {stats['deleted']}")
        print(f"  - Errors: {stats['errors']}")
        print(f"{'='*40}")

    else:
        # Dry run mode (default)
        preview_deletion(all_uids)


if __name__ == "__main__":
    main()
