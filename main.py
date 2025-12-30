#!/usr/bin/env python3
"""
Gmail Cleanup Tool - CLI Entry Point

A local Gmail analysis and cleanup tool using IMAP.
Fetches email metadata, stores in SQLite for analysis, and supports deletion.

Usage:
    python main.py fetch           # Fetch all emails (incremental)
    python main.py fetch --full    # Full re-fetch (ignore sync state)
    python main.py stats           # Show overview statistics
    python main.py query "SQL"     # Run ad-hoc SQL query
    python main.py top-senders     # Show top senders
    python main.py sample          # Fetch sample without storing
"""

import argparse
import sys

from db import get_connection, init_db, get_email_count, get_sync_state


def cmd_fetch(args):
    """Fetch emails from Gmail."""
    from fetch import fetch_all, fetch_sample

    if args.sample:
        print("Fetching sample of recent emails (not storing)...\n")
        samples = fetch_sample(args.sample)

        for email_data in samples:
            print(f"UID: {email_data['uid']}")
            print(f"  From: {email_data['sender_email']}")
            print(f"  Subject: {email_data['subject'][:60]}...")
            print(f"  Date: {email_data['date_parsed']}")
            print(f"  Size: {email_data['size_bytes']:,} bytes")
            print()
        return

    incremental = not args.full

    print("Starting email fetch...")
    if incremental:
        print("Mode: incremental (only new emails)")
    else:
        print("Mode: full (re-fetching all)")

    stats = fetch_all(
        batch_size=args.batch_size,
        incremental=incremental
    )

    print(f"\n{'='*40}")
    print("Fetch Complete:")
    print(f"  Total fetched: {stats['total_fetched']}")
    print(f"  New stored: {stats['new_stored']}")
    print(f"  Already existed: {stats['already_existed']}")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*40}")


def cmd_stats(args):
    """Show database statistics."""
    init_db()
    conn = get_connection()

    count = get_email_count(conn)
    sync = get_sync_state(conn)

    # Get size stats
    row = conn.execute("""
        SELECT
            COUNT(*) as count,
            COALESCE(SUM(size_bytes), 0) as total_size,
            MIN(date_parsed) as oldest,
            MAX(date_parsed) as newest
        FROM emails
    """).fetchone()

    print(f"\n{'='*50}")
    print("Gmail Database Statistics")
    print(f"{'='*50}")
    print(f"Total emails:     {row['count']:,}")
    print(f"Total size:       {row['total_size'] / 1024 / 1024:.2f} MB")
    print(f"Oldest email:     {row['oldest'] or 'N/A'}")
    print(f"Newest email:     {row['newest'] or 'N/A'}")
    print(f"{'='*50}")
    print("Sync State:")
    print(f"  Last UID:       {sync['last_uid']}")
    print(f"  Last sync:      {sync['last_sync'] or 'Never'}")
    print(f"{'='*50}")

    conn.close()


def cmd_query(args):
    """Run ad-hoc SQL query."""
    init_db()
    conn = get_connection()

    query = args.sql

    try:
        cursor = conn.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("No results.")
            return

        # Get column names
        columns = [desc[0] for desc in cursor.description]

        # Print header
        print("\t".join(columns))
        print("-" * 80)

        # Print rows
        for row in rows:
            print("\t".join(str(row[col]) for col in columns))

        print(f"\n({len(rows)} rows)")

    except Exception as e:
        print(f"SQL Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def cmd_top_senders(args):
    """Show top senders by count and size."""
    init_db()
    conn = get_connection()

    limit = args.limit

    print(f"\n{'='*70}")
    print(f"Top {limit} Senders by Email Count")
    print(f"{'='*70}")

    rows = conn.execute("""
        SELECT
            sender_email,
            COUNT(*) as count,
            SUM(size_bytes) / 1024.0 / 1024.0 as size_mb
        FROM emails
        WHERE sender_email != ''
        GROUP BY sender_email
        ORDER BY count DESC
        LIMIT ?
    """, (limit,)).fetchall()

    print(f"{'Sender':<45} {'Count':>8} {'Size (MB)':>10}")
    print("-" * 70)

    for row in rows:
        sender = row['sender_email'][:44]
        print(f"{sender:<45} {row['count']:>8} {row['size_mb']:>10.2f}")

    print(f"\n{'='*70}")
    print(f"Top {limit} Senders by Total Size")
    print(f"{'='*70}")

    rows = conn.execute("""
        SELECT
            sender_email,
            COUNT(*) as count,
            SUM(size_bytes) / 1024.0 / 1024.0 as size_mb
        FROM emails
        WHERE sender_email != ''
        GROUP BY sender_email
        ORDER BY size_mb DESC
        LIMIT ?
    """, (limit,)).fetchall()

    print(f"{'Sender':<45} {'Count':>8} {'Size (MB)':>10}")
    print("-" * 70)

    for row in rows:
        sender = row['sender_email'][:44]
        print(f"{sender:<45} {row['count']:>8} {row['size_mb']:>10.2f}")

    conn.close()


def cmd_newsletters(args):
    """Show likely newsletter senders."""
    init_db()
    conn = get_connection()

    print(f"\n{'='*70}")
    print("Likely Newsletter / Automated Senders")
    print(f"{'='*70}")

    rows = conn.execute("""
        SELECT
            sender_email,
            COUNT(*) as count,
            SUM(size_bytes) / 1024.0 / 1024.0 as size_mb
        FROM emails
        WHERE
            sender_email LIKE '%noreply%'
            OR sender_email LIKE '%no-reply%'
            OR sender_email LIKE '%newsletter%'
            OR sender_email LIKE '%notifications%'
            OR sender_email LIKE '%updates%'
            OR sender_email LIKE '%mailer%'
            OR sender_email LIKE 'mail@%'
            OR sender_email LIKE 'info@%'
            OR sender_email LIKE 'news@%'
        GROUP BY sender_email
        ORDER BY count DESC
        LIMIT 30
    """).fetchall()

    total_count = 0
    total_size = 0

    print(f"{'Sender':<45} {'Count':>8} {'Size (MB)':>10}")
    print("-" * 70)

    for row in rows:
        sender = row['sender_email'][:44]
        print(f"{sender:<45} {row['count']:>8} {row['size_mb']:>10.2f}")
        total_count += row['count']
        total_size += row['size_mb']

    print("-" * 70)
    print(f"{'TOTAL':<45} {total_count:>8} {total_size:>10.2f}")

    conn.close()


def cmd_delete(args):
    """Run the deletion tool."""
    # Just invoke delete.py's main
    import delete
    sys.argv = ['delete.py'] + args.delete_args
    delete.main()


def cmd_cleanup(args):
    """Find and optionally delete emails matching sender patterns."""
    init_db()
    conn = get_connection()

    patterns = args.patterns

    # Build WHERE clause for all patterns
    conditions = " OR ".join(["sender_email LIKE ?" for _ in patterns])
    like_patterns = [f"%{p}%" for p in patterns]

    # Get matching emails
    rows = conn.execute(f"""
        SELECT
            uid,
            sender_email,
            subject,
            date_parsed,
            size_bytes
        FROM emails
        WHERE {conditions}
        ORDER BY date_parsed DESC
    """, like_patterns).fetchall()

    if not rows:
        print(f"No emails found matching patterns: {patterns}")
        conn.close()
        return

    # Group by sender for summary
    sender_stats = conn.execute(f"""
        SELECT
            sender_email,
            COUNT(*) as count,
            SUM(size_bytes) / 1024.0 / 1024.0 as size_mb
        FROM emails
        WHERE {conditions}
        GROUP BY sender_email
        ORDER BY count DESC
    """, like_patterns).fetchall()

    total_count = len(rows)
    total_size = sum(r['size_bytes'] for r in rows) / 1024 / 1024

    print(f"\n{'='*70}")
    print(f"Emails matching patterns: {patterns}")
    print(f"{'='*70}")
    print(f"{'Sender':<45} {'Count':>8} {'Size (MB)':>10}")
    print("-" * 70)

    for row in sender_stats:
        sender = row['sender_email'][:44]
        print(f"{sender:<45} {row['count']:>8} {row['size_mb']:>10.2f}")

    print("-" * 70)
    print(f"{'TOTAL':<45} {total_count:>8} {total_size:>10.2f}")
    print(f"{'='*70}")

    if args.show:
        print(f"\nSample emails (first {min(10, len(rows))}):")
        print("-" * 70)
        for row in rows[:10]:
            print(f"UID {row['uid']}: {row['sender_email']}")
            print(f"  Subject: {row['subject'][:60]}")
            print(f"  Date: {row['date_parsed']}")
            print()

    if args.delete:
        uids = [row['uid'] for row in rows]

        print(f"\n*** DELETION MODE ***")
        print(f"Will delete {len(uids)} emails matching: {patterns}")

        if not args.yes:
            try:
                response = input(f"\nType 'DELETE {len(uids)}' to confirm: ")
                if response != f"DELETE {len(uids)}":
                    print("Confirmation failed. Aborting.")
                    conn.close()
                    return
            except EOFError:
                print("\nUse --yes flag to skip confirmation.")
                conn.close()
                return
        else:
            print("--yes flag provided, skipping confirmation...")

        # Import and run deletion
        from delete import delete_emails
        print("\nStarting deletion...")
        stats = delete_emails(uids, batch_size=args.batch_size)

        print(f"\n{'='*40}")
        print("Deletion complete:")
        print(f"  - Deleted: {stats['deleted']}")
        print(f"  - Errors: {stats['errors']}")
        print(f"{'='*40}")
    else:
        print(f"\nTo delete these emails, run:")
        print(f"  uv run main.py cleanup {' '.join(patterns)} --delete")

    conn.close()


def cli():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Gmail Cleanup Tool - Analyze and clean up your Gmail via IMAP",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # First time setup
  1. Create .env file with your credentials:
     GMAIL_EMAIL=yourname@gmail.com
     GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

  2. Fetch all emails:
     python main.py fetch

  3. View statistics:
     python main.py stats
     python main.py top-senders
     python main.py newsletters

  4. Run custom SQL:
     python main.py query "SELECT COUNT(*) FROM emails WHERE sender_email LIKE '%@github.com'"

  5. Delete emails (see delete.py --help for details):
     python delete.py --file uids.txt
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch emails from Gmail")
    fetch_parser.add_argument("--full", action="store_true", help="Full re-fetch (ignore sync state)")
    fetch_parser.add_argument("--batch-size", type=int, default=100, help="Batch size (default: 100)")
    fetch_parser.add_argument("--sample", type=int, metavar="N", help="Just fetch N recent emails without storing")
    fetch_parser.set_defaults(func=cmd_fetch)

    # stats command
    stats_parser = subparsers.add_parser("stats", help="Show database statistics")
    stats_parser.set_defaults(func=cmd_stats)

    # query command
    query_parser = subparsers.add_parser("query", help="Run SQL query")
    query_parser.add_argument("sql", help="SQL query to execute")
    query_parser.set_defaults(func=cmd_query)

    # top-senders command
    senders_parser = subparsers.add_parser("top-senders", help="Show top email senders")
    senders_parser.add_argument("--limit", type=int, default=20, help="Number of senders to show")
    senders_parser.set_defaults(func=cmd_top_senders)

    # newsletters command
    news_parser = subparsers.add_parser("newsletters", help="Show likely newsletter senders")
    news_parser.set_defaults(func=cmd_newsletters)

    # cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Find/delete emails by sender patterns")
    cleanup_parser.add_argument("patterns", nargs="+", help="Sender patterns to match (e.g., swiggy groww 1mg)")
    cleanup_parser.add_argument("--delete", action="store_true", help="Actually delete matching emails")
    cleanup_parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    cleanup_parser.add_argument("--show", action="store_true", help="Show sample of matching emails")
    cleanup_parser.add_argument("--batch-size", type=int, default=50, help="Deletion batch size (default: 50)")
    cleanup_parser.set_defaults(func=cmd_cleanup)

    # delete command (passes through to delete.py)
    del_parser = subparsers.add_parser("delete", help="Delete emails (see delete.py --help)")
    del_parser.add_argument("delete_args", nargs="*", help="Arguments for delete.py")
    del_parser.set_defaults(func=cmd_delete)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    cli()
