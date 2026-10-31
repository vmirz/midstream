"""Object URIs per scheme, and the destinations a table may be created under.

One form per backend, the one every engine reads natively (`docs/design/storage-backends.md` §4.7):

    gs://<bucket>/<key>
    s3://<bucket>/<key>
    abfss://<container>@<account>.dfs.core.windows.net/<key>
    file:///<absolute path>          (tests and laptops only)

A table's location is written into its metadata and manifests for good, so these forms are part of
every table: Azure's names the DFS endpoint as Spark, Trino and PyIceberg expect, while Midstream's own
store reaches the same account through its Blob endpoint (`STORAGE_AZURE_ENDPOINT` for an emulator).

The parser splits on the text, not with `urllib.parse`: a key may hold `?` or `#`, which are part of
an object's name, not a query or a fragment.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Final

#: Every scheme a URI may have; `file` serves tests only.
SCHEMES: Final = ("gs", "s3", "abfss", "file")
#: The schemes a deployment's destination may have.
DEPLOYABLE_SCHEMES: Final = ("gs", "s3", "abfss")

#: A GCS or S3 bucket as it may appear in an object URI: lenient enough for every bucket that
#: exists, including S3's legacy names with capitals and underscores. Destinations are stricter.
_BUCKET = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
#: `<container>@<account>.dfs.core.windows.net`, the only Azure form (decision D4).
_AZURE_NETLOC = re.compile(r"^(?P<container>[a-z0-9$][a-z0-9-]*)@(?P<account>[a-z0-9]+)\.dfs\.core\.windows\.net$")
AZURE_DFS_SUFFIX: Final = ".dfs.core.windows.net"

#: At most this many characters in a destination.
DESTINATION_MAX_LENGTH: Final = 1024
#: A GCS bucket and optionally a prefix in it.
GCS_DESTINATION_PATTERN: Final = re.compile(r"^gs://[a-z0-9][a-z0-9._-]*[a-z0-9](/\S*)?$")
#: An S3 bucket - 3 to 63 lowercase letters, digits, dots and hyphens, starting and ending with a
#: letter or digit - and optionally a prefix in it.
S3_DESTINATION_PATTERN: Final = re.compile(r"^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](/\S*)?$")
#: An Azure container - 3 to 63 lowercase letters, digits and single hyphens, starting and ending
#: with a letter or digit - in an account of 3 to 24 lowercase letters and digits, and optionally a
#: prefix in it.
AZURE_DESTINATION_PATTERN: Final = re.compile(
    r"^abfss://[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}@[a-z0-9]{3,24}\.dfs\.core\.windows\.net(/\S*)?$"
)
#: An absolute local path; only where file destinations are allowed (tests).
FILE_DESTINATION_PATTERN: Final = re.compile(r"^file:///\S+$")

DESTINATION_PATTERNS: Final = {
    "gs": GCS_DESTINATION_PATTERN,
    "s3": S3_DESTINATION_PATTERN,
    "abfss": AZURE_DESTINATION_PATTERN,
    "file": FILE_DESTINATION_PATTERN,
}

#: What each scheme's destinations look like, for refusals people can act on.
DESTINATION_FORMS: Final = {
    "gs": "gs://<bucket>[/<prefix>]",
    "s3": "s3://<bucket>[/<prefix>]",
    "abfss": "abfss://<container>@<account>.dfs.core.windows.net[/<prefix>]",
    "file": "file:///<absolute path>",
}
_DESTINATION_RULES: Final = {
    "gs": "the bucket of lowercase letters, digits, dots, hyphens and underscores, starting and ending with a letter or digit",
    "s3": "the bucket of 3 to 63 lowercase letters, digits, dots and hyphens, starting and ending with a letter or digit",
    "abfss": (
        "the container of 3 to 63 lowercase letters, digits and single hyphens, starting and ending with a "
        "letter or digit, the storage account of 3 to 24 lowercase letters and digits"
    ),
    "file": "an absolute path",
}
#: Schemes people reach for that name a backend Midstream reads in another form.
_OTHER_FORMS: Final = {
    "gcs": "gs",
    "s3a": "s3",
    "s3n": "s3",
    "abfs": "abfss",
    "az": "abfss",
    "wasb": "abfss",
    "wasbs": "abfss",
    "adl": "abfss",
    "https": None,
    "http": None,
}


@dataclass(frozen=True, slots=True)
class ObjectUri:
    """An object URI taken apart: the container (bucket, Azure container, '' for files) and the key.

    `key` has no leading slash; it is empty for a bucket's or container's root. For `file`, the key is
    the absolute path without its leading slash. `account` is set for Azure only.
    """

    scheme: str
    container: str
    key: str
    account: str | None = None

    @property
    def uri(self) -> str:
        """The canonical text: `gs://bucket/key` (`gs://bucket/` for the root), `file:///path`."""
        if self.scheme == "file":
            return f"file:///{self.key}"
        if self.scheme == "abfss":
            return f"abfss://{self.container}@{self.account}{AZURE_DFS_SUFFIX}/{self.key}"
        return f"{self.scheme}://{self.container}/{self.key}"


def scheme_of(uri: str) -> str:
    """The scheme of `uri`, one of `SCHEMES`; ValueError for any other or for text that is no URI."""
    scheme, separator, _ = uri.partition("://")
    if not separator:
        raise ValueError(f"not an object URI: {uri!r}")
    if scheme not in SCHEMES:
        raise ValueError(f"unknown storage scheme {scheme!r} in {uri!r}: expected one of {', '.join(SCHEMES)}")
    return scheme


def parse_object_uri(uri: str) -> ObjectUri:
    """Take `uri` apart; ValueError for an unknown scheme or a malformed URI."""
    scheme = scheme_of(uri)
    rest = uri[len(scheme) + 3 :]
    if scheme == "file":
        if not rest.startswith("/"):
            raise ValueError(f"a file URI names an absolute path, file:///<path>: {uri!r}")
        return ObjectUri(scheme="file", container="", key=rest[1:])
    netloc, _, key = rest.partition("/")
    if scheme == "abfss":
        match = _AZURE_NETLOC.match(netloc)
        if match is None:
            raise ValueError(f"an abfss URI looks like abfss://<container>@<account>{AZURE_DFS_SUFFIX}/<key>: {uri!r}")
        return ObjectUri(scheme="abfss", container=match.group("container"), key=key, account=match.group("account"))
    if not _BUCKET.match(netloc):
        raise ValueError(f"a {scheme} URI looks like {scheme}://<bucket>/<key>: {uri!r}")
    return ObjectUri(scheme=scheme, container=netloc, key=key)


def require_object_uri(uri: str, *, scheme: str | None = None) -> ObjectUri:
    """`parse_object_uri` for a URI naming one object: of `scheme` when given, and with a key."""
    parsed = require_prefix_uri(uri, scheme=scheme)
    if not parsed.key:
        raise ValueError(f"{uri!r} names a {'directory' if parsed.scheme == 'file' else 'bucket'}, not an object")
    return parsed


def require_prefix_uri(prefix: str, *, scheme: str | None = None) -> ObjectUri:
    """`parse_object_uri` for a listing prefix, of `scheme` when given; its key may be empty."""
    parsed = parse_object_uri(prefix)
    if scheme is not None and parsed.scheme != scheme:
        raise ValueError(f"a {scheme}:// store cannot serve {prefix!r}")
    return parsed


def join(base: str, *parts: str) -> str:
    """`base` and `parts` joined by single slashes: `join('gs://b/x/', '/y/', 'z')` is `gs://b/x/y/z`.

    Each part loses its leading and trailing slashes and empty parts are skipped; slashes inside a
    part stay. A `base` that is only a scheme keeps its slashes: `join('file:///', 'tmp')` is
    `file:///tmp`, `join('gs://', 'bucket')` is `gs://bucket`.
    """
    pieces = [piece for part in parts if (piece := part.strip("/"))]
    if not pieces:
        return base
    head = base.rstrip("/")
    if head.endswith(":"):
        # Only the scheme and its slashes (`file:///`, `gs://`): the pieces follow them directly.
        return base + "/".join(pieces)
    return "/".join([head, *pieces])


def normalize_destination_uri(value: str | None) -> str | None:
    """Trim a destination, dropping trailing slashes so object paths join cleanly.

    Blank is no destination at all, which is how an unset default reads from the environment.
    """
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed.rstrip("/") or trimmed


def destination_problem(value: str | None, *, allow_file: bool = False) -> str | None:
    """Why `value` (normalized) cannot be a destination, in one sentence a person can act on; None
    when it can. `file:///` destinations are accepted only with `allow_file` (tests).
    """
    schemes = (*DEPLOYABLE_SCHEMES, "file") if allow_file else DEPLOYABLE_SCHEMES
    forms = _forms(schemes)
    if value is None or not value.strip():
        return f"no destination is set: give one as {forms}"
    if len(value) > DESTINATION_MAX_LENGTH:
        return f"a destination is at most {DESTINATION_MAX_LENGTH} characters long, and this one has {len(value)}"
    if any(character.isspace() for character in value):
        return f"a destination cannot contain spaces or other whitespace: give one as {forms}"
    scheme, separator, _ = value.partition("://")
    if not separator:
        return f"{value!r} is not a storage URI: give one as {forms}"
    if scheme != scheme.lower() and scheme.lower() in schemes:
        return f"write the scheme in lowercase: {scheme.lower()}://, as in {DESTINATION_FORMS[scheme.lower()]}"
    if scheme in _OTHER_FORMS:
        preferred = _OTHER_FORMS[scheme]
        if preferred is None:
            return f"a destination names its bucket by URI, not by web address: give one as {forms}"
        return f"{scheme}:// destinations are written {DESTINATION_FORMS[preferred]} in Midstream, the form every engine reads"
    if scheme == "file" and not allow_file:
        return f"file:// destinations serve tests only: give one as {forms}"
    if scheme not in schemes:
        return f"{scheme}:// is not a storage scheme Midstream writes to: give one as {forms}"
    if DESTINATION_PATTERNS[scheme].match(value) is None:
        return f"{scheme}:// destinations look like {DESTINATION_FORMS[scheme]}, {_DESTINATION_RULES[scheme]}"
    return None


def is_destination_uri(value: str | None, *, allow_file: bool = False) -> bool:
    """Whether `value` (normalized) is a destination a table can be created under."""
    return destination_problem(value, allow_file=allow_file) is None


def _forms(schemes: tuple[str, ...]) -> str:
    forms = [DESTINATION_FORMS[scheme] for scheme in schemes]
    return ", ".join(forms[:-1]) + f" or {forms[-1]}"
