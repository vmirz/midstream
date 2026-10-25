#!/usr/bin/env python3
"""Rename identifiers across every tracked text file, and move a package directory with git.

The brand of this platform appears in only a few kinds of places: the import package under
``python/src``, the distribution name, the console's package name and product strings, the image
and chart names, and the documents. This script renames all of them in one pass so a rebrand stays
a mechanical change; the deployed resource names come from the Helm release, and the configuration
variables and wire names carry no brand at all.

Usage::

    tools/rename.py --move python/src/midstream python/src/newname \\
        --map midstream newname --map Midstream Newname --map MIDSTREAM NEWNAME

Each ``--map OLD NEW`` is a literal replacement applied, in the order given, to every file git
tracks that is text. ``--move OLD NEW`` runs ``git mv`` first, so the maps see the new paths.
``--dry-run`` reports what would change. The script never edits itself, and it exits non-zero when
a map matched nothing, since a rename that silently misses is worse than one that stops.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

BINARY_SUFFIXES = {".parquet", ".avro", ".png", ".jpg", ".jpeg", ".gif", ".ico", ".woff", ".woff2", ".pyc", ".zip", ".gz"}


def tracked_text_files(root: Path) -> list[Path]:
    names = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True).stdout.decode()
    files = []
    for name in filter(None, names.split("\0")):
        path = root / name
        if not path.is_file() or path.suffix.lower() in BINARY_SUFFIXES or path == Path(__file__).resolve():
            continue
        try:
            with path.open("rb") as handle:
                head = handle.read(8192)
        except OSError:
            continue
        if b"\0" in head:
            continue
        files.append(path)
    return files


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--move", nargs=2, action="append", default=[], metavar=("OLD", "NEW"), help="git mv OLD NEW before replacing")
    parser.add_argument("--map", nargs=2, action="append", default=[], metavar=("OLD", "NEW"), help="literal replacement, applied in order")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.map and not args.move:
        parser.error("nothing to do: give --map and/or --move")
    pairs = [(old, new) for old, new in args.map]
    if any(not old for old, _ in pairs):
        parser.error("--map OLD must not be empty")

    root = Path(subprocess.run(["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True).stdout.strip())
    for old, new in args.move:
        print(f"move  {old} -> {new}")
        if not args.dry_run:
            subprocess.run(["git", "mv", old, new], cwd=root, check=True)

    counts = {old: [0, 0] for old, _ in pairs}  # files, occurrences
    changed_files = 0
    for path in tracked_text_files(root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = text
        for old, new in pairs:
            n = updated.count(old)
            if n:
                counts[old][0] += 1
                counts[old][1] += n
                updated = updated.replace(old, new)
        if updated != text:
            changed_files += 1
            if not args.dry_run:
                path.write_text(updated, encoding="utf-8")
    for old, new in pairs:
        files, occurrences = counts[old]
        print(f"map   {old!r} -> {new!r}: {occurrences} occurrence(s) in {files} file(s)")
    print(f"{'would change' if args.dry_run else 'changed'} {changed_files} file(s)")
    missed = [old for old, _ in pairs if counts[old][1] == 0]
    if missed:
        print("no match for: " + ", ".join(repr(m) for m in missed), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
