"""Rules for the values people type into the directory: emails, names, descriptions, ids.

The API checks a request with these before accepting it, so a mistake comes back as a 400 naming
the field; the identity agent checks again before applying it, because it is the authority.
"""

from __future__ import annotations

import re
from typing import Any

from midstream.identity.permissions import ALL_PERMISSIONS, PERMISSION_KEYS

EMAIL_MAX_LENGTH = 254
PERSON_NAME_MAX_LENGTH = 100
LABEL_MAX_LENGTH = 80
DESCRIPTION_MAX_LENGTH = 500
MAX_IDS = 500

# Deliberately loose: one @, something on each side, a dot in the domain, no spaces. Whether an
# address really receives mail is not something a pattern can tell.
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


class IdentityValidationError(ValueError):
    """A value a field cannot take. `field` names the field, for the client to point at."""

    def __init__(self, message: str, *, field: str) -> None:
        self.field = field
        super().__init__(message)


def clean_email(value: Any, *, field: str = "email") -> str:
    """An email as it is stored: trimmed and lower-case."""
    if not isinstance(value, str) or not value.strip():
        raise IdentityValidationError("Email is required", field=field)
    email = value.strip().lower()
    if len(email) > EMAIL_MAX_LENGTH or not _EMAIL.match(email):
        raise IdentityValidationError("Enter a valid email address, such as jane@example.com", field=field)
    return email


def clean_person_name(value: Any, *, field: str, label: str) -> str:
    """A first or last name: required, trimmed, at most 100 characters, no control characters."""
    if not isinstance(value, str) or not value.strip():
        raise IdentityValidationError(f"{label} is required", field=field)
    name = " ".join(value.split())
    if len(name) > PERSON_NAME_MAX_LENGTH:
        raise IdentityValidationError(f"{label} must be at most {PERSON_NAME_MAX_LENGTH} characters", field=field)
    if _CONTROL.search(name):
        raise IdentityValidationError(f"{label} contains characters that are not allowed", field=field)
    return name


def clean_label(value: Any, *, field: str = "name", label: str = "Name") -> str:
    """A group or role name: required, trimmed, at most 80 characters."""
    if not isinstance(value, str) or not value.strip():
        raise IdentityValidationError(f"{label} is required", field=field)
    name = " ".join(value.split())
    if len(name) > LABEL_MAX_LENGTH:
        raise IdentityValidationError(f"{label} must be at most {LABEL_MAX_LENGTH} characters", field=field)
    if _CONTROL.search(name):
        raise IdentityValidationError(f"{label} contains characters that are not allowed", field=field)
    return name


def clean_description(value: Any, *, field: str = "description") -> str:
    """A description: optional, trimmed, at most 500 characters."""
    if value is None:
        return ""
    if not isinstance(value, str):
        raise IdentityValidationError("Description must be text", field=field)
    description = value.strip()
    if len(description) > DESCRIPTION_MAX_LENGTH:
        raise IdentityValidationError(
            f"Description must be at most {DESCRIPTION_MAX_LENGTH} characters",
            field=field,
        )
    return description


def clean_ids(value: Any, *, field: str) -> list[str]:
    """A list of ids, without duplicates, in the order given."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise IdentityValidationError(f"{field} must be a list", field=field)
    if len(value) > MAX_IDS:
        raise IdentityValidationError(f"{field} may hold at most {MAX_IDS} entries", field=field)
    ids: list[str] = []
    for item in value:
        if not isinstance(item, str) or not _IDENTIFIER.match(item):
            raise IdentityValidationError(f"{field} contains an id that is not valid", field=field)
        if item not in ids:
            ids.append(item)
    return ids


def clean_identifier(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise IdentityValidationError(f"{field} is not a valid id", field=field)
    return value


def clean_permissions(value: Any, *, field: str = "permissions") -> list[str]:
    """A custom role's permissions: known keys only, sorted. `*` belongs to Administrator alone."""
    if not isinstance(value, list):
        raise IdentityValidationError("Permissions must be a list", field=field)
    permissions: set[str] = set()
    for item in value:
        if item == ALL_PERMISSIONS:
            raise IdentityValidationError(
                "Only the built-in Administrator role holds every permission; choose them one by one",
                field=field,
            )
        if not isinstance(item, str) or item not in PERMISSION_KEYS:
            raise IdentityValidationError(f"{item!r} is not a permission", field=field)
        permissions.add(item)
    return sorted(permissions)
