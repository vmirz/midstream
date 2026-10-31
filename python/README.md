# midstream-platform

The Python package of [Midstream](https://github.com/vmirz/midstream), a streaming platform on
[Kaspr](https://kaspr.dev) that materializes Kafka topics into Apache Iceberg tables — in Google
Cloud Storage, Amazon S3 or an S3-compatible server, or Azure Blob Storage — exactly once, with
schemas that evolve by themselves, an Iceberg REST catalog, accounts and permissions, and a web
console.

One distribution holds every server-side part: the control plane, the per-materialization runtime,
the maintenance and tail apps, and the Iceberg library they share. It is not a library to import into
your own code: Kaspr applications install it in their pods, and the Helm chart in the repository
deploys them. See the repository's `docs/install.md` to run it, and `docs/handbook/` for how it works.
