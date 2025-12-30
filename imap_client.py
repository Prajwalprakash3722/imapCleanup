"""
IMAP client wrapper for Gmail.

Gmail IMAP Quirks (read these!):
1. Gmail requires SSL on port 993
2. Must use App Password if 2FA enabled (which you should have)
3. Gmail has aggressive rate limiting - expect disconnects on large fetches
4. The folder "[Gmail]/All Mail" contains all emails regardless of labels
5. UIDs are only stable within a single mailbox - don't mix UIDs across folders
6. PEEK flag is critical - without it, Gmail marks emails as read
7. Gmail may throttle you to ~500MB/day for IMAP downloads
8. IDLE is supported but we don't need it for batch operations

Authentication:
- Go to https://myaccount.google.com/apppasswords
- Generate an "App Password" for "Mail"
- Use that 16-char password, not your Google password
"""

import imaplib
import ssl
import time
from dataclasses import dataclass
from typing import Optional

# Gmail IMAP settings (these are fixed)
GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993

# Gmail's "All Mail" folder - contains every email regardless of labels
# This is the ONLY folder we fetch from to avoid duplicates
GMAIL_ALL_MAIL = "[Gmail]/All Mail"

# Rate limiting: pause between batches to avoid Gmail throttling
DEFAULT_BATCH_DELAY_SECONDS = 0.5


@dataclass
class IMAPConfig:
    """IMAP connection configuration."""
    email: str
    app_password: str
    host: str = GMAIL_IMAP_HOST
    port: int = GMAIL_IMAP_PORT


class GmailIMAPClient:
    """
    Wrapper around imaplib for Gmail-specific operations.

    Usage:
        config = IMAPConfig(email="you@gmail.com", app_password="xxxx xxxx xxxx xxxx")
        with GmailIMAPClient(config) as client:
            uids = client.search_all()
            for batch in client.fetch_headers_batch(uids, batch_size=100):
                process(batch)
    """

    def __init__(self, config: IMAPConfig):
        self.config = config
        self._conn: Optional[imaplib.IMAP4_SSL] = None

    def connect(self) -> None:
        """
        Establish SSL connection and authenticate.

        Raises:
            imaplib.IMAP4.error: On authentication failure
            ssl.SSLError: On SSL/TLS issues
            socket.error: On network issues
        """
        # Create SSL context (Gmail requires TLS 1.2+)
        context = ssl.create_default_context()

        self._conn = imaplib.IMAP4_SSL(
            host=self.config.host,
            port=self.config.port,
            ssl_context=context
        )

        # Authenticate with app password
        # Gmail will reject regular passwords if 2FA is enabled
        self._conn.login(self.config.email, self.config.app_password)

        # Select All Mail in read-only mode by default
        # read-only=True prevents accidental flag changes
        self._select_all_mail(readonly=True)

    def _select_all_mail(self, readonly: bool = True) -> int:
        """
        Select [Gmail]/All Mail folder.

        Returns:
            Total number of messages in folder
        """
        # Quote the mailbox name - required for names with special chars like brackets/spaces
        status, data = self._conn.select(f'"{GMAIL_ALL_MAIL}"', readonly=readonly)
        if status != "OK":
            raise RuntimeError(f"Failed to select {GMAIL_ALL_MAIL}: {data}")

        # data[0] contains message count
        return int(data[0])

    def disconnect(self) -> None:
        """Close connection gracefully."""
        if self._conn:
            try:
                self._conn.close()
                self._conn.logout()
            except Exception:
                pass  # Connection may already be dead
            self._conn = None

    def __enter__(self) -> "GmailIMAPClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def search_all(self) -> list[int]:
        """
        Get UIDs of all messages in All Mail.

        Returns:
            List of UIDs (as integers), sorted ascending

        Note:
            On a large mailbox (100k+ emails), this can take 10-30 seconds.
            Gmail seems to handle this fine without rate limiting.
        """
        # UID SEARCH returns UIDs instead of sequence numbers
        # "ALL" matches every message
        status, data = self._conn.uid("SEARCH", None, "ALL")

        if status != "OK":
            raise RuntimeError(f"Search failed: {data}")

        # data[0] is a space-separated byte string of UIDs
        if not data[0]:
            return []

        uid_strings = data[0].decode().split()
        return sorted(int(uid) for uid in uid_strings)

    def search_since_uid(self, min_uid: int) -> list[int]:
        """
        Get UIDs greater than min_uid (for incremental sync).

        Gmail IMAP doesn't support "UID > X" directly, so we fetch all
        and filter. For very large mailboxes, consider using UIDNEXT.

        Args:
            min_uid: Fetch emails with UID > this value

        Returns:
            List of UIDs greater than min_uid
        """
        all_uids = self.search_all()
        return [uid for uid in all_uids if uid > min_uid]

    def get_uidnext(self) -> int:
        """
        Get UIDNEXT - the UID that will be assigned to the next message.

        Useful for detecting new messages without fetching all UIDs.
        """
        # Re-select to get fresh STATUS
        # Quote the mailbox name - required for names with special chars like brackets/spaces
        status, data = self._conn.status(f'"{GMAIL_ALL_MAIL}"', "(UIDNEXT)")
        if status != "OK":
            raise RuntimeError(f"STATUS failed: {data}")

        # Parse "UIDNEXT 12345" from response
        # Response format: b'[Gmail]/All Mail (UIDNEXT 12345)'
        import re
        match = re.search(rb"UIDNEXT (\d+)", data[0])
        if not match:
            raise RuntimeError(f"Could not parse UIDNEXT from: {data}")
        return int(match.group(1))

    def fetch_headers(self, uids: list[int]) -> dict[int, dict]:
        """
        Fetch headers for a list of UIDs.

        Uses BODY.PEEK to avoid marking as read.

        Args:
            uids: List of UIDs to fetch

        Returns:
            Dict mapping UID -> raw header data dict containing:
                - "headers": raw header bytes
                - "size": RFC822.SIZE in bytes

        Gotcha:
            Gmail may disconnect you if you fetch too many at once.
            Use fetch_headers_batch() for large fetches.
        """
        if not uids:
            return {}

        # Build UID set string: "1,2,3,4,5"
        uid_set = ",".join(str(uid) for uid in uids)

        # Fetch parts:
        # - BODY.PEEK[HEADER] - all headers without marking as read
        # - RFC822.SIZE - email size in bytes
        # - INTERNALDATE - when Gmail received it (optional, for debugging)

        # PEEK is critical! Without it, Gmail marks emails as \Seen
        fetch_parts = "(BODY.PEEK[HEADER] RFC822.SIZE)"

        status, data = self._conn.uid("FETCH", uid_set, fetch_parts)

        if status != "OK":
            raise RuntimeError(f"Fetch failed: {data}")

        return self._parse_fetch_response(data)

    def _parse_fetch_response(self, data: list) -> dict[int, dict]:
        """
        Parse IMAP FETCH response into usable format.

        IMAP responses are... special. Format varies by server.
        Gmail's format: [(b'1 (UID 123 RFC822.SIZE 4567 BODY[HEADER] {890}', b'headers...'), b')']
        """
        results = {}
        i = 0

        while i < len(data):
            item = data[i]

            # Skip closing parens and None values
            if item is None or item == b")":
                i += 1
                continue

            # Each message is a tuple: (metadata_bytes, header_bytes)
            if isinstance(item, tuple) and len(item) >= 2:
                metadata, headers = item[0], item[1]

                # Parse UID from metadata
                # Format: b'123 (UID 456 RFC822.SIZE 789 BODY[HEADER] {123}'
                import re
                uid_match = re.search(rb"UID (\d+)", metadata)
                size_match = re.search(rb"RFC822.SIZE (\d+)", metadata)

                if uid_match:
                    uid = int(uid_match.group(1))
                    size = int(size_match.group(1)) if size_match else 0

                    results[uid] = {
                        "headers": headers,
                        "size": size
                    }

            i += 1

        return results

    def fetch_headers_batch(
        self,
        uids: list[int],
        batch_size: int = 100,
        delay_seconds: float = DEFAULT_BATCH_DELAY_SECONDS
    ):
        """
        Generator that fetches headers in batches with rate limiting.

        Yields:
            dict[int, dict]: Batch of UID -> header data

        Why batches?
            Gmail will disconnect you if you try to fetch too many at once.
            100-200 seems safe. Going higher risks "connection reset".

        Why delay?
            Gmail rate limits IMAP. Small delays prevent throttling.
            0.5s between batches is conservative but reliable.
        """
        for i in range(0, len(uids), batch_size):
            batch_uids = uids[i:i + batch_size]

            yield self.fetch_headers(batch_uids)

            # Rate limit: pause between batches
            if i + batch_size < len(uids):
                time.sleep(delay_seconds)

    def delete_messages(self, uids: list[int], expunge: bool = True) -> int:
        """
        Delete messages by UID.

        This is a DESTRUCTIVE operation. Messages go to Trash, then
        are permanently deleted after 30 days (or immediately if expunged from Trash).

        Args:
            uids: UIDs to delete
            expunge: If True, permanently remove (can't be undone!)

        Returns:
            Number of messages successfully marked for deletion

        Gmail behavior:
            - Setting \Deleted flag moves to Trash
            - EXPUNGE removes from current folder (All Mail)
            - To permanently delete, you'd need to also expunge from Trash
        """
        if not uids:
            return 0

        # Must re-select in read-write mode for modifications
        self._select_all_mail(readonly=False)

        uid_set = ",".join(str(uid) for uid in uids)

        # Add \Deleted flag
        status, data = self._conn.uid("STORE", uid_set, "+FLAGS", "(\\Deleted)")

        if status != "OK":
            raise RuntimeError(f"Failed to mark deleted: {data}")

        deleted_count = len(uids)

        # EXPUNGE permanently removes messages with \Deleted flag
        if expunge:
            self._conn.expunge()

        # Return to read-only mode for safety
        self._select_all_mail(readonly=True)

        return deleted_count


def load_config_from_env() -> IMAPConfig:
    """
    Load IMAP config from environment variables.

    Expected env vars:
        GMAIL_EMAIL: Your Gmail address
        GMAIL_APP_PASSWORD: 16-character app password

    Create a .env file:
        GMAIL_EMAIL=yourname@gmail.com
        GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
    """
    import os
    from dotenv import load_dotenv

    load_dotenv()

    email = os.getenv("GMAIL_EMAIL")
    password = os.getenv("GMAIL_APP_PASSWORD")

    if not email or not password:
        raise ValueError(
            "Missing credentials. Set GMAIL_EMAIL and GMAIL_APP_PASSWORD in .env file.\n"
            "Get an app password at: https://myaccount.google.com/apppasswords"
        )

    return IMAPConfig(email=email, app_password=password)


if __name__ == "__main__":
    # Quick connection test
    config = load_config_from_env()

    print(f"Connecting to Gmail as {config.email}...")
    with GmailIMAPClient(config) as client:
        print("Connected!")

        uids = client.search_all()
        print(f"Found {len(uids)} messages in All Mail")

        if uids:
            print(f"UID range: {min(uids)} - {max(uids)}")
            print(f"UIDNEXT: {client.get_uidnext()}")
