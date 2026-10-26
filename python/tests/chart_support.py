"""Render the Helm chart for the tests that check what it deploys.

The chart replaces the Kustomize manifests the control plane shipped with: every resource, topic and
table is named after the release, so the tests render it with `helm template` and read the documents,
the way the deployment is actually made. Rendering is cached per values file.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import shutil
import subprocess
import unittest

REPO = Path(__file__).resolve().parents[2]
CHART = REPO / "charts" / "midstream"
#: The values files the chart ships for its own checks, one per installation shape.
VALUES_FILES = tuple(sorted((CHART / "ci").glob("*.yaml")))
#: The release the tests install as; every derived name starts with it.
RELEASE = "midstream"
NAMESPACE = "midstream"
DEFAULT_VALUES = CHART / "ci" / "test-values.yaml"


def require_helm() -> None:
    if shutil.which("helm") is None:
        raise unittest.SkipTest("helm is required to render the chart")


@lru_cache(maxsize=None)
def render(values: Path = DEFAULT_VALUES, release: str = RELEASE) -> tuple[dict, ...]:
    """Every document the chart renders with the values file, as `helm template` produces them."""
    import yaml

    require_helm()
    completed = subprocess.run(
        ["helm", "template", release, str(CHART), "-n", NAMESPACE, "-f", str(values)],
        capture_output=True,
        text=True,
        check=True,
    )
    return tuple(document for document in yaml.safe_load_all(completed.stdout) if document)


def documents(kind: str | None = None, values: Path = DEFAULT_VALUES) -> list[dict]:
    """The rendered documents, of one kind if given."""
    return [document for document in render(values) if kind is None or document.get("kind") == kind]


def document(kind: str, name: str, values: Path = DEFAULT_VALUES) -> dict:
    """One rendered document by kind and its name without the release prefix (`request-intake`)."""
    wanted = f"{RELEASE}-{name}"
    for candidate in documents(kind, values):
        if candidate["metadata"]["name"] == wanted:
            return candidate
    raise KeyError(f"the chart renders no {kind} named {wanted}")


def bare(name: str) -> str:
    """A rendered resource's name without the release prefix."""
    return name[len(RELEASE) + 1 :] if name.startswith(f"{RELEASE}-") else name


def python_sources(document: dict) -> list[str]:
    """Every inline Python block in a rendered document, wherever it sits."""
    found: list[str] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "python" and isinstance(value, str):
                    found.append(value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(document)
    return found
