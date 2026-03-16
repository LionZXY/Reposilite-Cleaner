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
import base64
import logging
import os
import sys
import threading
import time
import urllib.request
import urllib.error
import json
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_MAX_AGE_DAYS = 30
DEFAULT_REPOS = ["releases", "snapshots"]
DEFAULT_WORKERS = 8
DEFAULT_EXCLUDE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "exclude.txt")
DEFAULT_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clean.log")

log = logging.getLogger("reposilite-cleaner")


def load_exclude_file(path):
    """Load exclusion list from a file, one path per line. Ignores blank lines and comments."""
    if not os.path.isfile(path):
        return set()
    with open(path) as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


_stats_ref = None


def make_request(url, method="GET", token=None):
    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", "reposilite-cleaner/1.0")
    if token:
        if method == "DELETE":
            credentials = base64.b64encode(token.encode()).decode()
            req.add_header("Authorization", "Basic " + credentials)
        else:
            req.add_header("Authorization", "Bearer " + token)
    ctx = ssl.create_default_context()
    if _stats_ref:
        _stats_ref.add_request()
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


class Stats:
    """Thread-safe statistics counter."""

    def __init__(self):
        self._lock = threading.Lock()
        self.requests = 0
        self.total_files = 0
        self.total_size = 0
        self.old_files = 0
        self.old_size = 0
        self.deleted = 0
        self.errors = 0
        self.skipped = 0
        self.empty_dirs = 0
        self.start_time = time.time()
        self._last_progress_time = time.time()

    def add_request(self):
        with self._lock:
            self.requests += 1

    def add_scanned(self, size):
        with self._lock:
            self.total_files += 1
            self.total_size += size

    def add_old(self, size):
        with self._lock:
            self.old_files += 1
            self.old_size += size

    def add_deleted(self):
        with self._lock:
            self.deleted += 1

    def add_error(self):
        with self._lock:
            self.errors += 1

    def add_skipped(self):
        with self._lock:
            self.skipped += 1

    def add_empty_dir(self):
        with self._lock:
            self.empty_dirs += 1

    def log_progress(self, repository):
        with self._lock:
            elapsed = int(time.time() - self.start_time)
            log.info(
                "PROGRESS [%s] %dm%02ds elapsed | requests: %d | scanned: %d files (%s) | old: %d | deleted: %d | errors: %d",
                repository, elapsed // 60, elapsed % 60,
                self.requests, self.total_files, fmt_size(self.total_size),
                self.old_files, self.deleted, self.errors,
            )

    def start_progress_timer(self, repository):
        self._stop_event = threading.Event()
        def _timer():
            while not self._stop_event.wait(PROGRESS_INTERVAL):
                self.log_progress(repository)
        self._timer_thread = threading.Thread(target=_timer, daemon=True)
        self._timer_thread.start()

    def stop_progress_timer(self):
        self._stop_event.set()
        self._timer_thread.join()


def walk_repository(base_url, repository, start_path, token, cutoff_ts, stats, exclude_paths, executor):
    """BFS walk: parallel directory listing + parallel file deletion."""
    dirs_to_scan = [start_path]
    all_visited_dirs = []
    delete_futures = []

    # Phase 1: BFS scan directories and delete old files
    while dirs_to_scan:
        # List all directories in current batch in parallel
        future_to_path = {
            executor.submit(list_details, base_url, repository, d, token): d
            for d in dirs_to_scan
        }
        dirs_to_scan = []

        for future in as_completed(future_to_path):
            path = future_to_path[future]
            details = future.result()
            if details is None:
                continue

            for entry in details.get("files", []):
                name = entry["name"]
                entry_path = f"{path}/{name}" if path else name

                if is_excluded(repository, entry_path, exclude_paths):
                    log.info("SKIP (excluded): %s/%s", repository, entry_path)
                    stats.add_skipped()
                    continue

                if entry["type"] == "DIRECTORY":
                    dirs_to_scan.append(entry_path)
                    all_visited_dirs.append(entry_path)
                elif entry["type"] == "FILE":
                    content_length = entry.get("contentLength", 0)
                    last_modified = entry.get("lastModifiedTime", 0)
                    stats.add_scanned(content_length)

                    if last_modified < cutoff_ts:
                        stats.add_old(content_length)
                        age_days = (time.time() - last_modified) / 86400
                        log.info("DELETE %s/%s  (%dd old, %s)", repository, entry_path, age_days, fmt_size(content_length))
                        f = executor.submit(_delete_file_task, base_url, repository, entry_path, token, stats)
                        delete_futures.append(f)

    # Wait for all file deletes to finish before checking empty dirs
    for future in as_completed(delete_futures):
        future.result()

    # Phase 2: clean up empty directories (deepest first)
    for dir_path in reversed(all_visited_dirs):
        after = list_details(base_url, repository, dir_path, token)
        if after is None:
            continue
        remaining = after.get("files", None)
        if remaining is not None and len(remaining) == 0:
            stats.add_empty_dir()
            log.info("DELETE empty dir: %s/%s", repository, dir_path)
            try:
                delete_file(base_url, repository, dir_path, token)
            except Exception as e:
                log.error("Failed to delete dir %s/%s: %s", repository, dir_path, e)


def _delete_file_task(base_url, repository, entry_path, token, stats):
    try:
        delete_file(base_url, repository, entry_path, token)
        stats.add_deleted()
    except urllib.error.HTTPError as e:
        log.error("Failed to delete %s/%s: HTTP %d", repository, entry_path, e.code)
        stats.add_error()


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
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})")
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
    log.info("  Workers:      %d", args.workers)
    exclude_paths = load_exclude_file(args.exclude_file)
    log.info("  Exclude file: %s (%d entries)", args.exclude_file, len(exclude_paths))
    log.info("  Log file:     %s", log_file)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for repo in args.repos:
            log.info("Scanning %s/%s...", repo, args.path)
            global _stats_ref
            stats = Stats()
            _stats_ref = stats
            stats.start_progress_timer(repo)

            walk_repository(args.url, repo, args.path, args.token, cutoff_ts, stats, exclude_paths, executor)

            stats.stop_progress_timer()

            log.info("%s summary:", repo)
            log.info("  HTTP requests:       %d", stats.requests)
            log.info("  Total files scanned: %d (%s)", stats.total_files, fmt_size(stats.total_size))
            log.info("  Old files found:     %d (%s)", stats.old_files, fmt_size(stats.old_size))
            log.info("  Deleted:             %d", stats.deleted)
            log.info("  Errors:              %d", stats.errors)
            log.info("  Skipped (excluded):  %d", stats.skipped)
            log.info("  Empty dirs removed:  %d", stats.empty_dirs)


if __name__ == "__main__":
    main()
