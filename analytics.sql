-- =============================================================================
-- Gmail Analytics Queries
-- =============================================================================
-- Run these queries against gmail.db using:
--   sqlite3 gmail.db < analytics.sql
--   sqlite3 gmail.db "SELECT ..."
--   or use a SQLite GUI like DB Browser for SQLite
-- =============================================================================

-- -----------------------------------------------------------------------------
-- OVERVIEW STATS
-- -----------------------------------------------------------------------------

-- Total emails and storage used
SELECT
    COUNT(*) as total_emails,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb,
    printf("%.2f", AVG(size_bytes) / 1024.0) as avg_size_kb,
    MIN(date_parsed) as oldest_email,
    MAX(date_parsed) as newest_email
FROM emails;


-- Emails per year
SELECT
    strftime('%Y', date_parsed) as year,
    COUNT(*) as count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as size_mb
FROM emails
WHERE date_parsed IS NOT NULL
GROUP BY year
ORDER BY year DESC;


-- -----------------------------------------------------------------------------
-- TOP SENDERS BY COUNT
-- -----------------------------------------------------------------------------

-- Top 20 senders by email count
SELECT
    sender_email,
    COUNT(*) as email_count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb
FROM emails
WHERE sender_email != ''
GROUP BY sender_email
ORDER BY email_count DESC
LIMIT 20;


-- Top senders by count (with domain breakdown)
SELECT
    SUBSTR(sender_email, INSTR(sender_email, '@') + 1) as domain,
    COUNT(*) as email_count,
    COUNT(DISTINCT sender_email) as unique_senders,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb
FROM emails
WHERE sender_email LIKE '%@%'
GROUP BY domain
ORDER BY email_count DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- TOP SENDERS BY SIZE (Storage hogs)
-- -----------------------------------------------------------------------------

-- Top 20 senders by total storage used
SELECT
    sender_email,
    COUNT(*) as email_count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb,
    printf("%.2f", AVG(size_bytes) / 1024.0) as avg_size_kb
FROM emails
WHERE sender_email != ''
GROUP BY sender_email
ORDER BY SUM(size_bytes) DESC
LIMIT 20;


-- Largest individual emails
SELECT
    uid,
    sender_email,
    subject,
    printf("%.2f", size_bytes / 1024.0 / 1024.0) as size_mb,
    date_parsed
FROM emails
ORDER BY size_bytes DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- OLD EMAILS (Cleanup candidates)
-- -----------------------------------------------------------------------------

-- Emails older than 5 years
SELECT
    COUNT(*) as count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as size_mb
FROM emails
WHERE date_parsed < datetime('now', '-5 years');


-- Old emails by sender (cleanup targets)
SELECT
    sender_email,
    COUNT(*) as email_count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb,
    MIN(date_parsed) as oldest,
    MAX(date_parsed) as newest
FROM emails
WHERE date_parsed < datetime('now', '-3 years')
GROUP BY sender_email
ORDER BY email_count DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- NEWSLETTER / AUTOMATED EMAIL DETECTION
-- -----------------------------------------------------------------------------

-- Likely newsletters (common patterns)
SELECT
    sender_email,
    COUNT(*) as email_count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb
FROM emails
WHERE
    sender_email LIKE '%noreply%'
    OR sender_email LIKE '%no-reply%'
    OR sender_email LIKE '%newsletter%'
    OR sender_email LIKE '%notifications%'
    OR sender_email LIKE '%updates%'
    OR sender_email LIKE '%mailer%'
    OR sender_email LIKE '%mail@%'
    OR sender_email LIKE '%info@%'
    OR sender_email LIKE '%news@%'
    OR sender_email LIKE '%digest@%'
    OR sender_email LIKE '%automated%'
    OR sender_email LIKE '%bounce%'
GROUP BY sender_email
ORDER BY email_count DESC
LIMIT 30;


-- Subject line patterns suggesting bulk mail
SELECT
    sender_email,
    COUNT(*) as email_count
FROM emails
WHERE
    subject LIKE '%unsubscribe%'
    OR subject LIKE '%weekly%'
    OR subject LIKE '%daily%'
    OR subject LIKE '%digest%'
    OR subject LIKE '%newsletter%'
    OR subject LIKE '%notification%'
GROUP BY sender_email
ORDER BY email_count DESC
LIMIT 20;


-- Domains sending the most automated-looking emails
SELECT
    SUBSTR(sender_email, INSTR(sender_email, '@') + 1) as domain,
    COUNT(*) as email_count,
    printf("%.2f", SUM(size_bytes) / 1024.0 / 1024.0) as total_size_mb
FROM emails
WHERE
    sender_email LIKE '%noreply%'
    OR sender_email LIKE '%no-reply%'
    OR sender_email LIKE 'notifications@%'
    OR sender_email LIKE 'alerts@%'
GROUP BY domain
ORDER BY email_count DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- EXPORT UIDS FOR DELETION
-- -----------------------------------------------------------------------------

-- Get UIDs from a specific sender (for deletion)
-- Usage: pipe to delete.py
SELECT uid FROM emails WHERE sender_email = 'newsletter@example.com';


-- Get UIDs of old emails from a sender
SELECT uid FROM emails
WHERE sender_email LIKE '%@promotions.example.com'
AND date_parsed < datetime('now', '-1 year');


-- Get UIDs of large old emails
SELECT uid FROM emails
WHERE size_bytes > 1024 * 1024  -- > 1MB
AND date_parsed < datetime('now', '-2 years');


-- UIDs from top unwanted senders (newsletter-like + old)
SELECT uid FROM emails
WHERE sender_email IN (
    SELECT sender_email FROM emails
    WHERE sender_email LIKE '%noreply%'
    GROUP BY sender_email
    HAVING COUNT(*) > 50
)
AND date_parsed < datetime('now', '-1 year');


-- -----------------------------------------------------------------------------
-- DEBUGGING / DATA QUALITY
-- -----------------------------------------------------------------------------

-- Emails with parsing issues (no parsed date)
SELECT COUNT(*) as unparsed_dates FROM emails WHERE date_parsed IS NULL;


-- Sample of unparsed dates (to debug date parsing)
SELECT uid, date_header, sender_email
FROM emails
WHERE date_parsed IS NULL
LIMIT 10;


-- Emails without sender (possibly malformed)
SELECT COUNT(*) as no_sender FROM emails WHERE sender_email = '' OR sender_email IS NULL;


-- Duplicate message IDs (shouldn't happen if only fetching All Mail)
SELECT message_id, COUNT(*) as count
FROM emails
WHERE message_id IS NOT NULL AND message_id != ''
GROUP BY message_id
HAVING count > 1
LIMIT 10;


-- -----------------------------------------------------------------------------
-- USEFUL QUERIES FOR AD-HOC EXPLORATION
-- -----------------------------------------------------------------------------

-- Search emails by sender domain
-- SELECT * FROM emails WHERE sender_email LIKE '%@github.com' LIMIT 10;

-- Search emails by subject keyword
-- SELECT uid, sender_email, subject, date_parsed FROM emails WHERE subject LIKE '%invoice%' LIMIT 20;

-- Count emails from a specific sender
-- SELECT COUNT(*), printf("%.2f MB", SUM(size_bytes)/1024.0/1024.0) FROM emails WHERE sender_email = 'someone@example.com';

-- Get all senders matching a pattern
-- SELECT DISTINCT sender_email FROM emails WHERE sender_email LIKE '%amazon%';
