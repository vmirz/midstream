"""Recognising a kind of sensitive data from a field's name (design §5.1).

A field is recognised by its record key at its struct level — the logical name, before the schema
policy sanitises it — normalised to lowercase letters and digits. A rule matches the normalised
name exactly or by suffix, optionally only under a parent; a plural name (`emails`, `addresses`)
matches as its singular when the plural itself matches nothing.

A container whose name matches a kind is never masked as a whole: its leaves are classified by their
own names, except that a generic leaf (`number`, `value`, `line1`, …) takes the container's kind,
and every leaf under a kind that inherits everything (`secret`) takes it.

Rules are deliberately conservative: a false positive masks a column people wanted to read, which
is worse than a miss, and misses can be chosen per field later (design §15.1).
"""

from __future__ import annotations

from functools import lru_cache

from midstream.protection.catalog import GENERIC_LEAVES, KINDS, KINDS_BY_ID


def normalize(name: object) -> str:
    """A field name as the rules match it: lowercase ASCII letters and digits only."""
    text = str(name).lower()
    return "".join(character for character in text if ("a" <= character <= "z") or ("0" <= character <= "9"))


def _forms(name: str) -> tuple[str, ...]:
    """The name, then its singular forms: a plural matches only when the name itself does not."""
    forms = [name]
    if len(name) > 3 and name.endswith("es"):
        forms.append(name[:-2])
    if len(name) > 2 and name.endswith("s") and not name.endswith("ss"):
        forms.append(name[:-1])
    return tuple(forms)


@lru_cache(maxsize=65_536)
def rule_kind(parent: str | None, name: str) -> str | None:
    """The kind a field's own name matches under its parent, by the rules alone; None for none.

    Both names are raw record keys; they are normalised here. Memoised per process: a key costs one
    pass over the rules the first time and a dictionary hit after.
    """
    normalized = normalize(name)
    if not normalized:
        return None
    parent_forms = _forms(normalize(parent)) if parent is not None else ()
    for form in _forms(normalized):
        for item in KINDS:
            for rule in item.rules:
                if rule.under and not any(candidate in rule.under for candidate in parent_forms):
                    continue
                if rule.matches(form):
                    return item.id
    return None


def classify(parent: str | None, name: str, inherited: str | None = None) -> str | None:
    """The kind of a field named `name` under `parent`, inside a container of kind `inherited`.

    A generic leaf of a container takes the container's kind (`email.address` is an email), as does
    every leaf of a container whose kind inherits everything (`secret`); otherwise the field's own
    name decides.
    """
    if inherited is not None:
        container = KINDS_BY_ID.get(inherited)
        if container is not None and (container.inherit_all or normalize(name) in GENERIC_LEAVES):
            return inherited
    return rule_kind(parent, name)


def container_kind(parent: str | None, name: str, inherited: str | None = None) -> str | None:
    """The kind a container named `name` passes to its leaves: its own name's, or, under a container
    whose kind inherits everything, that kind."""
    own = rule_kind(parent, name)
    if own is not None:
        return own
    if inherited is not None:
        container = KINDS_BY_ID.get(inherited)
        if container is not None and container.inherit_all:
            return inherited
    return None
