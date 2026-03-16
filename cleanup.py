#!/usr/bin/env python3
"""Scan repository and remove empty directories."""

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    DEFAULT_EXCLUDE_FILE, DEFAULT_URL, DEFAULT_WORKERS,
    Stats, delete_path, is_excluded, list_details,
    load_exclude_file, log, set_stats, setup_logging,
)


def run(base_url, repo, start_path, token, stats, exclude_paths, executor):
    dirs = [start_path]
    all_dirs = []

    while dirs:
        listing_futures = {
            executor.submit(list_details, base_url, repo, d, token): d
            for d in dirs
        }
        dirs = []

        for future in as_completed(listing_futures):
            path = listing_futures[future]
            details = future.result()
            if details is None:
                continue

            for entry in details.get("files", []):
                if entry["type"] != "DIRECTORY":
                    continue
                name = entry["name"]
                entry_path = f"{path}/{name}" if path else name

                if is_excluded(repo, entry_path, exclude_paths):
                    log.info("SKIP (excluded): %s/%s", repo, entry_path)
                    stats.add_skipped()
                    continue

                dirs.append(entry_path)
                all_dirs.append(entry_path)

    for dir_path in reversed(all_dirs):
        after = list_details(base_url, repo, dir_path, token)
        if after is None:
            continue
        remaining = after.get("files", None)
        if remaining is not None and len(remaining) == 0:
            stats.add_empty_dir()
            log.info("DELETE empty dir: %s/%s", repo, dir_path)
            try:
                delete_path(base_url, repo, dir_path, token)
            except Exception as e:
                log.error("Failed to delete dir %s/%s: %s", repo, dir_path, e)


def main():
    p = argparse.ArgumentParser(description="Remove empty directories from Reposilite")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--token", default=os.environ.get("REPOSILITE_TOKEN"))
    p.add_argument("--repo", required=True)
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p.add_argument("--path", default="")
    p.add_argument("--exclude-file", default=DEFAULT_EXCLUDE_FILE)
    args = p.parse_args()

    setup_logging()

    if not args.token:
        log.error("--token or REPOSILITE_TOKEN is required")
        sys.exit(1)

    exclude = load_exclude_file(args.exclude_file)

    log.info("cleanup phase | url=%s repo=%s workers=%d", args.url, args.repo, args.workers)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        stats = Stats()
        set_stats(stats)
        stats.start_progress_timer(args.repo)
        run(args.url, args.repo, args.path, args.token, stats, exclude, executor)
        stats.stop_progress_timer()
        stats.log_summary(args.repo)


if __name__ == "__main__":
    main()
