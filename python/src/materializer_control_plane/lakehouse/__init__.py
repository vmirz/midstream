"""Iceberg tables for materializations: the GCS-pointer catalog, table shape and schema policy.

Shared by both planes (one package, one pinned revision): the runtime's committer writes through
it, the control plane creates and drops tables and serves the REST facade with it.
"""
