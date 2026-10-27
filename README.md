# Midstream

Midstream is a platform for running streaming applications on Apache Kafka, built on
[Kaspr](https://kaspr.dev). Its first feature is **materialization**: it continuously turns Kafka
topics into Apache Iceberg tables in object storage, exactly once, with schemas that evolve by
themselves, an Iceberg REST catalog any engine can attach, accounts and permissions, and a web
console that reads the tables directly and follows new records live.

Any number of Midstream deployments — **domains**, one per cluster or team — share one console: one
URL with a page per domain (`/d/<domain>`) and pages across all of them, search, SQL that joins
tables of different domains, sign-in through Microsoft Entra ID, GitHub, Google or any OpenID Connect
provider into every domain at once, access tokens every domain honours, and one Iceberg REST catalog
over every domain. Each domain keeps its own accounts, data and settings
([handbook 20](docs/handbook/20-domains-and-the-shared-console.md)).

Releases are on the [releases page](https://github.com/vmirz/midstream/releases): a Helm chart
(`oci://ghcr.io/vmirz/charts/midstream`), the Python package (`midstream-platform` on PyPI) and the
console image (`ghcr.io/vmirz/midstream-console`). [docs/install.md](docs/install.md) takes an empty
namespace to a first materialization.

## What is here

| Path | What it is |
|------|-----------|
| `python/` | The Python package: the control plane, the per-materialization runtime, the maintenance and tail apps, and the Iceberg library they share |
| `charts/midstream/` | The Helm chart: one domain's control plane, maintenance and tail apps, and the shared console with its registry of every domain — either or both in a release ([README](charts/midstream/README.md)) |
| `docs/install.md` | Installing it, Secrets included |
| `console/` | The web console: a Node backend-for-frontend plus a React client ([README](console/README.md), [handbook](console/docs/README.md)) |
| `docs/handbook/` | The deep reference manual ([start here](docs/handbook/README.md)) |
| `AGENTS.md` | Repository conventions and guardrails, for people and coding agents alike |

## Development

```bash
# Python: a virtual environment with the package installed in place, then the test suite
python3.12 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m unittest discover -s python/tests -p 'test_*.py'

# Chart: lint and render
helm lint charts/midstream -f charts/midstream/ci/test-values.yaml
helm template midstream charts/midstream -n midstream -f charts/midstream/ci/test-values.yaml

# Console: point it at a control plane (with no registry it serves one domain, `local`, at :6065)
kubectl -n midstream port-forward svc/midstream-control-plane-app-api 6065:6065
cd console && npm install && npm run dev
```

See [docs/handbook/01-system-overview.md](docs/handbook/01-system-overview.md) for the
architecture and [CONTRIBUTING.md](CONTRIBUTING.md) for how changes are made.

## License

[MIT](LICENSE).
