"""The version of this package, as shipped in the running image.

The control plane and every runtime app are the *same* Python distribution, so one version string
identifies the code both are executing. That matters because the two are deployed independently:
a runtime keeps serving an older image until its spec is re-applied, and the console needs to show
which code each one is actually running rather than assuming they match.

Read from installed distribution metadata so the declared version in `pyproject.toml` stays the
single source of truth — nothing to bump twice and no constant that can drift from the package.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _distribution_version

DISTRIBUTION_NAME = "midstream"

# Reported when the package is running from a source tree that was never installed, which is the
# case in some test and tooling contexts. Deliberately not a plausible release number.
UNKNOWN_VERSION = "0.0.0+unknown"


def package_version() -> str:
    """Return the installed package version, or `UNKNOWN_VERSION` outside an installed context."""
    try:
        return _distribution_version(DISTRIBUTION_NAME)
    except PackageNotFoundError:
        return UNKNOWN_VERSION


PACKAGE_VERSION = package_version()

__all__ = ["DISTRIBUTION_NAME", "PACKAGE_VERSION", "UNKNOWN_VERSION", "package_version"]
