#!/usr/bin/env python3
"""Delete artifacts listed in a scan result JSON file."""

import argparse
import json
import os
import sys
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    DEFAULT_LOG_FILE, DEFAULT_URL, DEFAULT_WORKERS,
    Stats, delete_path, log, set_stats, setup_logging,
)


def _delete_task(base_url, repo, entry, token, stats):
    path = entry["path"]
    try:
        delete_path(base_url, repo, path, token)
        stats.add_deleted()
    except urllib.error.HTTPError as e:
        log.error("Failed to delete %s/%s: HTTP %d", repo, path, e.code)
        stats.add_error()


def main():
    p = argparse.ArgumentParser(description="Delete artifacts from scan result")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--token", default=os.environ.get("REPOSILITE_TOKEN"))
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p.add_argument("--log-file", default=DEFAULT_LOG_FILE)
    p.add_argument("--input", default="scan_result.json", help="Scan result JSON file")
    args = p.parse_args()

    setup_logging(args.log_file)

    if not args.token:
        log.error("--token or REPOSILITE_TOKEN is required")
        sys.exit(1)

    with open(args.input) as f:
        data = json.load(f)

    repo = data["repo"]
    entries = data["entries"]

    dir_count = sum(1 for e in entries if e["type"] == "dir")
    file_count = sum(1 for e in entries if e["type"] == "file")

    log.info("delete phase | url=%s repo=%s entries=%d (%d dirs, %d files) workers=%d",
             args.url, repo, len(entries), dir_count, file_count, args.workers)

    stats = Stats()
    set_stats(stats)
    stats.start_progress_timer(repo)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for entry in entries:
            log.info("DELETE %s %s/%s", entry["type"], repo, entry["path"])
            futures.append(executor.submit(_delete_task, args.url, repo, entry, args.token, stats))
        for f in as_completed(futures):
            f.result()

    stats.stop_progress_timer()
    stats.log_summary(repo)


if __name__ == "__main__":
    main()
