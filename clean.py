#!/usr/bin/env python3
"""
Reposilite Cleaner — deletes artifacts older than a specified age.

Usage:
    python3 clean.py --url https://reposilite.flipp.dev --token <token> [options]

Authentication:
    Use an access token with delete permissions. Pass via --token or
    REPOSILITE_TOKEN env var. Format: "name:secret" (used as Basic auth).
"""

import argparse
import logging
import os
import sys
import time
import urllib.request
import urllib.error
import json
import ssl

DEFAULT_MAX_AGE_DAYS = 30
DEFAULT_REPOS = ["releases", "snapshots"]
DEFAULT_EXCLUDE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exclude.txt")
DEFAULT_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clean.log")

log = logging.getLogger("reposilite-cleaner")


def load_exclude_file(path):
    """Load exclusion list from a file, one path per line. Ignores blank lines and comments."""
    if not os.path.isfile(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def make_request(url, method="GET", token=None):
    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", "reposilite-cleaner/1.0")
    if token:
        if method == "DELETE":
            import base64
            credentials = base64.b64encode(token.encode()).decode()
            req.add_header("Authorization", "Basic " + credentials)
        else:
            req.add_header("Authorization", "Bearer " + token)
    # Allow self-signed certs if needed
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, context=ctx) as resp:
            if method == "GET":
                return json.loads(resp.read().decode())
            return resp.status
    except urllib.error.HTTPError as e:
        if method == "GET":
            log.warning("HTTP %d fetching %s", e.code, url)
            return None
        raise


def list_details(base_url, repository, path="", token=None):
    """Get directory/file details from Reposilite API."""
    url = f"{base_url}/api/maven/details/{repository}"
    if path:
        url += f"/{path}"
    return make_request(url, token=token)


def delete_file(base_url, repository, path, token=None):
    """Delete a file from Reposilite."""
    url = f"{base_url}/{repository}/{path}"
    return make_request(url, method="DELETE", token=token)


def is_excluded(repository, entry_path, exclude_paths):
    full_path = f"{repository}/{entry_path}"
    return full_path in exclude_paths


PROGRESS_INTERVAL = 60  # seconds


def log_progress_if_needed(stats, repository):
    now = time.time()
    if now - stats["last_progress_time"] >= PROGRESS_INTERVAL:
        elapsed = int(now - stats["start_time"])
        log.info(
            "PROGRESS [%s] %dm%02ds elapsed | scanned: %d files (%s) | old: %d | deleted: %d | errors: %d",
            repository, elapsed // 60, elapsed % 60,
            stats["total_files"], fmt_size(stats["total_size"]),
            stats["old_files"], stats["deleted"], stats["errors"],
        )
        stats["last_progress_time"] = now


def walk_repository(base_url, repository, path, token, cutoff_ts, stats, exclude_paths):
    """Recursively walk a repository and delete old artifacts."""
    details = list_details(base_url, repository, path, token)
    if details is None:
        return

    files = details.get("files", [])
    for entry in files:
        log_progress_if_needed(stats, repository)
        name = entry["name"]
        entry_path = f"{path}/{name}" if path else name

        if is_excluded(repository, entry_path, exclude_paths):
            log.info("SKIP (excluded): %s/%s", repository, entry_path)
            stats["skipped"] += 1
            continue

        if entry["type"] == "DIRECTORY":
            walk_repository(base_url, repository, entry_path, token, cutoff_ts, stats, exclude_paths)
            # After cleaning children, re-check if directory is now completely empty
            after = list_details(base_url, repository, entry_path, token)
            if after is None:
                # Directory already gone (deleted by Reposilite when last file removed)
                continue
            remaining = after.get("files", None)
            if remaining is not None and len(remaining) == 0:
                stats["empty_dirs"] += 1
                log.info("DELETE empty dir: %s/%s", repository, entry_path)
                try:
                    delete_file(base_url, repository, entry_path, token)
                except Exception as e:
                    log.error("Failed to delete dir %s/%s: %s", repository, entry_path, e)
        elif entry["type"] == "FILE":
            last_modified = entry.get("lastModifiedTime", 0)
            stats["total_files"] += 1
            stats["total_size"] += entry.get("contentLength", 0)

            if last_modified < cutoff_ts:
                stats["old_files"] += 1
                stats["old_size"] += entry.get("contentLength", 0)
                age_days = (time.time() - last_modified) / 86400

                log.info("DELETE %s/%s  (%dd old, %s)", repository, entry_path, age_days, fmt_size(entry.get('contentLength', 0)))
                try:
                    delete_file(base_url, repository, entry_path, token)
                    stats["deleted"] += 1
                except urllib.error.HTTPError as e:
                    log.error("Failed to delete %s/%s: HTTP %d", repository, entry_path, e.code)
                    stats["errors"] += 1


def fmt_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main():
    parser = argparse.ArgumentParser(description="Delete old artifacts from Reposilite")
    parser.add_argument("--url", default="https://reposilite.flipp.dev",
                        help="Reposilite base URL (default: https://reposilite.flipp.dev)")
    parser.add_argument("--token", default=os.environ.get("REPOSILITE_TOKEN"),
                        help="Auth token as 'name:secret' (or set REPOSILITE_TOKEN env var)")
    parser.add_argument("--repos", nargs="+", default=DEFAULT_REPOS,
                        help=f"Repositories to clean (default: {DEFAULT_REPOS})")
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS,
                        help=f"Delete artifacts older than this many days (default: {DEFAULT_MAX_AGE_DAYS})")
    parser.add_argument("--path", default="",
                        help="Only clean under this subpath (e.g. 'net/flipper')")
    parser.add_argument("--exclude-file", default=DEFAULT_EXCLUDE_FILE,
                        help=f"File with paths to exclude, one per line (default: {DEFAULT_EXCLUDE_FILE})")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE,
                        help=f"Log file path (default: {DEFAULT_LOG_FILE})")

    args = parser.parse_args()

    # Setup logging: console + file
    log_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_fmt)
    log.addHandler(console_handler)

    log_file = args.log_file
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(log_fmt)
    log.addHandler(file_handler)
    log.setLevel(logging.INFO)

    if not args.token:
        log.error("--token or REPOSILITE_TOKEN is required")
        sys.exit(1)

    cutoff_ts = time.time() - (args.max_age_days * 86400)

    log.info("Reposilite Cleaner")
    log.info("  URL:          %s", args.url)
    log.info("  Repositories: %s", args.repos)
    log.info("  Max age:      %d days", args.max_age_days)
    log.info("  Cutoff:       %s", time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(cutoff_ts)))
    exclude_paths = load_exclude_file(args.exclude_file)
    log.info("  Exclude file: %s (%d entries)", args.exclude_file, len(exclude_paths))
    log.info("  Log file:     %s", log_file)

    for repo in args.repos:
        log.info("Scanning %s/%s...", repo, args.path)
        stats = {
            "total_files": 0, "total_size": 0,
            "old_files": 0, "old_size": 0,
            "deleted": 0, "errors": 0, "skipped": 0, "empty_dirs": 0,
            "start_time": time.time(), "last_progress_time": time.time(),
        }

        walk_repository(args.url, repo, args.path, args.token, cutoff_ts, stats, exclude_paths)

        log.info("%s summary:", repo)
        log.info("  Total files scanned: %d (%s)", stats['total_files'], fmt_size(stats['total_size']))
        log.info("  Old files found:     %d (%s)", stats['old_files'], fmt_size(stats['old_size']))
        log.info("  Deleted:             %d", stats['deleted'])
        log.info("  Errors:              %d", stats['errors'])
        log.info("  Skipped (excluded):  %d", stats['skipped'])
        log.info("  Empty dirs removed:  %d", stats['empty_dirs'])


if __name__ == "__main__":
    main()
