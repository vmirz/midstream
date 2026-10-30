"""Iceberg tables for materializations: the pointer catalog, table shape and schema policy.

Every table lives in an object store - Google Cloud Storage, Amazon S3, Azure Blob Storage - reached
through `lakehouse/store/`, which nothing above it branches on.

Shared by both planes (one package, one pinned revision): the runtime's committer writes through
it, the control plane creates and drops tables and serves the REST facade with it.
"""
