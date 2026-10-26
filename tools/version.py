#!/usr/bin/env python3
"""The platform version, declared in four places and kept identical.

Midstream releases the Python package, the console image and the Helm chart together under one
version. It is written as SemVer where SemVer is spoken - `console/package.json`, its lock file and
`charts/midstream/Chart.yaml` (`version` and `appVersion`) - and as PEP 440 in `python/pyproject.toml`:
`1.0.0-rc.1` there is `1.0.0rc1` here, and pip reads either.

    tools/version.py check [--tag v1.0.0-rc.1]   fail unless every file agrees (and matches the tag)
    tools/version.py semver | pep440             print the version in that form
    tools/version.py set 1.0.0-rc.1              write the version everywhere (npm must be installed)
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PYPROJECT = REPO / "python" / "pyproject.toml"
PACKAGE_JSON = REPO / "console" / "package.json"
PACKAGE_LOCK = REPO / "console" / "package-lock.json"
CHART = REPO / "charts" / "midstream" / "Chart.yaml"
SEMVER = re.compile(r"^(?P<release>\d+\.\d+\.\d+)(?:-(?P<pre>alpha|beta|rc)\.(?P<n>\d+))?$")
PRE = {"alpha": "a", "beta": "b", "rc": "rc"}


def pep440(semver: str) -> str:
    """`1.0.0-rc.1` → `1.0.0rc1`; a release is itself."""
    match = SEMVER.match(semver)
    if match is None:
        raise SystemExit(f"not a release version: {semver!r} (expected 1.2.3 or 1.2.3-rc.1)")
    if match["pre"] is None:
        return match["release"]
    return f"{match['release']}{PRE[match['pre']]}{match['n']}"


def declared() -> dict[str, str]:
    with PYPROJECT.open("rb") as handle:
        python = tomllib.load(handle)["project"]["version"]
    console = json.loads(PACKAGE_JSON.read_text())["version"]
    lock = json.loads(PACKAGE_LOCK.read_text())
    chart = CHART.read_text()
    chart_version = re.search(r"^version:\s*(\S+)\s*$", chart, re.M)
    app_version = re.search(r"^appVersion:\s*\"?([^\"\s]+)\"?\s*$", chart, re.M)
    return {
        "python/pyproject.toml": python,
        "console/package.json": console,
        "console/package-lock.json": lock["version"],
        "console/package-lock.json (root package)": lock["packages"][""]["version"],
        "charts/midstream/Chart.yaml version": chart_version.group(1) if chart_version else "",
        "charts/midstream/Chart.yaml appVersion": app_version.group(1) if app_version else "",
    }


def check(tag: str | None) -> str:
    versions = declared()
    semver = versions["console/package.json"]
    expected = {name: (pep440(semver) if name == "python/pyproject.toml" else semver) for name in versions}
    wrong = {name: (versions[name], expected[name]) for name in versions if versions[name] != expected[name]}
    if wrong:
        lines = [f"  {name}: {found!r}, expected {want!r}" for name, (found, want) in wrong.items()]
        raise SystemExit("the version is not the same everywhere:\n" + "\n".join(lines))
    if tag is not None and tag != f"v{semver}":
        raise SystemExit(f"tag {tag!r} does not match the declared version v{semver}")
    return semver


def set_version(semver: str) -> None:
    pep = pep440(semver)
    PYPROJECT.write_text(re.sub(r'^version = "[^"]+"$', f'version = "{pep}"', PYPROJECT.read_text(), count=1, flags=re.M))
    chart = CHART.read_text()
    chart = re.sub(r"^version:.*$", f"version: {semver}", chart, count=1, flags=re.M)
    chart = re.sub(r"^appVersion:.*$", f'appVersion: "{semver}"', chart, count=1, flags=re.M)
    CHART.write_text(chart)
    subprocess.run(["npm", "version", semver, "--no-git-tag-version", "--allow-same-version"], cwd=REPO / "console", check=True, capture_output=True)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    check_parser = sub.add_parser("check", help="fail unless every file declares the same version")
    check_parser.add_argument("--tag", help="a git tag that must equal v<version>")
    sub.add_parser("semver", help="print the version as SemVer")
    sub.add_parser("pep440", help="print the version as PEP 440")
    set_parser = sub.add_parser("set", help="write a version everywhere")
    set_parser.add_argument("version", help="SemVer, e.g. 1.0.0-rc.1")
    args = parser.parse_args(argv)
    if args.command == "check":
        print(f"version {check(args.tag)} declared consistently")
    elif args.command == "semver":
        print(check(None))
    elif args.command == "pep440":
        print(pep440(check(None)))
    elif args.command == "set":
        set_version(args.version)
        print(f"version {check(None)} written")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
