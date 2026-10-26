# Contributing

Thank you for considering a change. This page covers the mechanics; [AGENTS.md](AGENTS.md) holds
the conventions and guardrails every change follows, and the handbooks under `docs/handbook/` and
`console/docs/` explain how the system works.

## Setting up

You need Python 3.12 or later, Node 24 or later, and `helm` for rendering the chart.

```bash
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
cd console && npm install
```

## Checking a change

Run what your change touches before opening a pull request; the suites are the specification.

```bash
.venv/bin/python -m unittest discover -s python/tests -p 'test_*.py'   # the Python package
.venv/bin/python -m compileall python/src                               # syntax check: there is no linter
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml     # the chart
cd console && npm run typecheck && npm test && npm run build            # the console
```

A behaviour change comes with a test in the nearest existing `python/tests/test_*.py` module or
`console/**/*.test.ts` file, and with the matching handbook page updated. Keep the handbooks true to
the code: they are written to be read by people and loaded by coding agents.

## Continuous integration and releases

Every pull request runs `.github/workflows/ci.yml`: the Python suite on 3.12 and 3.13 with a wheel
build, the console's typecheck, tests, build and image build, the chart's lint and render with every
values file, and a check that the version is the same everywhere (`tools/version.py check`). A tag
`v<version>` runs `release.yml`, which publishes the package to PyPI, the console image and the chart to
GHCR, and a GitHub release. Pre-release tags look like `v1.0.0-rc.1`.

## Pull requests

Branch from `main`, keep a pull request to one topic, and write the description for someone who did
not watch you work: what changed, why, and how you checked it. Do not introduce a linter, formatter,
test runner or build system; the repository deliberately uses `pip`, `unittest`, Helm, npm,
`tsc` and Vite.

## Reporting a problem

Open an issue with what you expected, what happened, and the versions involved. For a security
problem, follow [SECURITY.md](SECURITY.md) instead of opening an issue.
