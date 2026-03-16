#!/usr/bin/env python3
"""Scan repository and output optimized delete list.

If all files in a directory are old, emits a single directory delete
instead of individual file deletes.
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from common import (
    DEFAULT_EXCLUDE_FILE, DEFAULT_LOG_FILE, DEFAULT_URL, DEFAULT_WORKERS,
    Stats, fmt_size, is_excluded, list_details,
    load_exclude_file, log, set_stats, setup_logging,
)

DEFAULT_MAX_AGE_DAYS = 30


def scan(base_url, repo, start_path, token, cutoff_ts, stats, exclude_paths, executor):
    """BFS scan. Returns per-directory metadata for optimization."""
    dirs_to_scan = [start_path]
    # dir_path -> {total_files, old_files, subdirs, has_excluded}
    dir_info = defaultdict(lambda: {"total_files": 0, "old_files": 0, "old_file_paths": [], "subdirs": [], "has_excluded": False})

    while dirs_to_scan:
        listing_futures = {
            executor.submit(list_details, base_url, repo, d, token): d
            for d in dirs_to_scan
        }
        dirs_to_scan = []

        for future in as_completed(listing_futures):
            parent = listing_futures[future]
            details = future.result()
            if details is None:
                continue

            for entry in details.get("files", []):
                name = entry["name"]
                entry_path = f"{parent}/{name}" if parent else name

                if is_excluded(repo, entry_path, exclude_paths):
                    log.info("SKIP (excluded): %s/%s", repo, entry_path)
                    stats.add_skipped()
                    dir_info[parent]["has_excluded"] = True
                    continue

                if entry["type"] == "DIRECTORY":
                    dirs_to_scan.append(entry_path)
                    dir_info[parent]["subdirs"].append(entry_path)
                    # Ensure entry exists even if empty
                    _ = dir_info[entry_path]
                elif entry["type"] == "FILE":
                    size = entry.get("contentLength", 0)
                    stats.add_scanned(size)
                    dir_info[parent]["total_files"] += 1

                    if entry.get("lastModifiedTime", 0) < cutoff_ts:
                        stats.add_old(size)
                        dir_info[parent]["old_files"] += 1
                        dir_info[parent]["old_file_paths"].append(entry_path)

    return dict(dir_info)


def is_fully_deletable(path, dir_info, cache):
    """Check if a directory and all its contents can be deleted as one."""
    if path in cache:
        return cache[path]

    info = dir_info.get(path)
    if info is None:
        cache[path] = False
        return False

    if info["has_excluded"]:
        cache[path] = False
        return False

    # All direct files must be old
    if info["old_files"] < info["total_files"]:
        cache[path] = False
        return False

    # All subdirs must be fully deletable
    for sub in info["subdirs"]:
        if not is_fully_deletable(sub, dir_info, cache):
            cache[path] = False
            return False

    # Must have at least some content (don't emit already-empty dirs)
    has_content = info["total_files"] > 0 or any(
        is_fully_deletable(s, dir_info, cache) for s in info["subdirs"]
    )
    cache[path] = has_content
    return has_content


def build_delete_list(start_path, dir_info, cache):
    """Build optimized list: directories where possible, files otherwise."""
    entries = []

    def walk(path):
        if is_fully_deletable(path, dir_info, cache):
            entries.append({"type": "dir", "path": path})
            return

        info = dir_info.get(path)
        if info is None:
            return

        # Add individual old files from this directory
        for file_path in info["old_file_paths"]:
            entries.append({"type": "file", "path": file_path})

        # Recurse into subdirs
        for sub in info["subdirs"]:
            walk(sub)

    if start_path and start_path in dir_info:
        walk(start_path)
    else:
        # Walk top-level subdirs from root
        info = dir_info.get("", dir_info.get(start_path))
        if info:
            for file_path in info.get("old_file_paths", []):
                entries.append({"type": "file", "path": file_path})
            for sub in info.get("subdirs", []):
                walk(sub)

    return entries


def main():
    p = argparse.ArgumentParser(description="Scan Reposilite and list old artifacts")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--token", default=os.environ.get("REPOSILITE_TOKEN"))
    p.add_argument("--repo", required=True, help="Repository to scan")
    p.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS)
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    p.add_argument("--path", default="")
    p.add_argument("--exclude-file", default=DEFAULT_EXCLUDE_FILE)
    p.add_argument("--log-file", default=DEFAULT_LOG_FILE)
    p.add_argument("--output", default="scan_result.json", help="Output JSON file")
    args = p.parse_args()

    setup_logging(args.log_file)

    if not args.token:
        log.error("--token or REPOSILITE_TOKEN is required")
        sys.exit(1)

    cutoff = time.time() - args.max_age_days * 86400
    exclude = load_exclude_file(args.exclude_file)

    log.info("scan phase | url=%s repo=%s max_age=%dd workers=%d",
             args.url, args.repo, args.max_age_days, args.workers)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        stats = Stats()
        set_stats(stats)
        stats.start_progress_timer(args.repo)
        dir_info = scan(args.url, args.repo, args.path, args.token, cutoff, stats, exclude, executor)
        stats.stop_progress_timer()
        stats.log_summary(args.repo)

    cache = {}
    entries = build_delete_list(args.path, dir_info, cache)

    dir_count = sum(1 for e in entries if e["type"] == "dir")
    file_count = sum(1 for e in entries if e["type"] == "file")

    with open(args.output, "w") as f:
        json.dump({"repo": args.repo, "entries": entries}, f)

    log.info("Wrote %d entries to %s (%d dirs, %d files)", len(entries), args.output, dir_count, file_count)


if __name__ == "__main__":
    main()
