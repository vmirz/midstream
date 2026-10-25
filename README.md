# kaspr-kafka-materializer

Continuously materializes JSON records from Kafka topics into partitioned, Snappy-compressed
Parquet files in object storage, with a self-service HTTP API and automatic schema evolution.

| Path | What it is |
|------|-----------|
| `python/` | Control-plane and runtime Python package (`materializer-control-plane`) |
| `manifests/` | Kaspr CRDs and Kustomize overlays for dev, staging, and prod |
| `web/` | Materializer Console — real-time web app for the control plane ([README](web/README.md), [handbook](web/docs/README.md)) |
| `reader/` | DuckDB-over-GCS query helper |
| `docs/handbook/` | Deep reference manual ([start here](docs/handbook/README.md)) |

## Quick start

```bash
# Python: install dev dependencies and run the test suite
python -m pip install -r requirements-dev.txt
PYTHONPATH=python/src python -m unittest discover -s python/tests -p 'test_*.py'

# Manifests: render to validate
kubectl kustomize manifests/dev

# Console: point at a port-forwarded control plane and run
kubectl -n kafka-sql port-forward svc/develop-materializer-control-plane-app-api 6065:6065
cd web && npm install && npm run dev
```

See [docs/handbook/01-system-overview.md](docs/handbook/01-system-overview.md) for the
architecture and [AGENTS.md](AGENTS.md) for repository conventions.
