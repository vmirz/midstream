# Column Fork Schema Design

## Goal

Replace breaking-change rejections with forward-only schema forks on the write side.

- Scalars and objects are flattened into physical parquet columns.
- Arrays are flattened structurally by path, not by positional index.
- Read-side logical view generation is deferred.

## Implemented Status

The implementation in this repository currently does all of the following:

- normalizes authoritative schema field entries into fork-aware records with `logical_field_path`, `fork_version`, `is_current`, `materialized`, and `physical_column_name`
- encodes physical parquet column names by replacing `.` with `__` and `[]` with `__items`
- materializes scalar fields and array-descendant leaf fields, while object containers remain metadata-only and array containers materialize only when they have no flattened descendants
- treats type evolution as forward-only fork creation, with current-vs-historical selection handled through `is_current`
- evaluates schema subsumption against current normalized fields only, so inactive historical forks do not suppress new live additions
- writes runtime parquet rows as flattened physical columns only for current materialized fields
- preserves array order inside list-valued columns instead of flattening numeric positions
- seals staged JSONL to parquet using an explicit whole-file PyArrow schema inference pass so structurally flattened list columns survive `null -> typed-value` evolution across reader blocks

## Physical layout

Logical paths remain authoritative, but each logical path may own multiple physical versions over time.

Examples:

- `payload.customer_id` -> `payload__customer_id`
- later type fork -> `payload__customer_id__v2`
- later type fork -> `payload__customer_id__v3`

Objects flatten by descendant path:

- `payload.customer.email` -> `payload__customer__email`
- `payload.customer.id` -> `payload__customer__id`

Object container nodes remain part of authoritative schema metadata so shape transitions are tracked, but they are not themselves materialized as parquet columns.

Arrays flatten structurally:

- `payload.items` -> logical/container array field
- `payload.items[]` -> logical array-element field
- `payload.items[].sku` -> `payload__items__items__sku`
- `payload.items[].qty` -> `payload__items__items__qty`

This does not flatten by numeric index (`items__0`, `items__1`, ...). Instead, list position is preserved inside list-valued leaf columns.

Nested arrays work the same way:

- `payload.orders[].lines[].sku` -> nested list-valued physical column

If an array contains scalars rather than objects, the scalar array may still be materialized as a single list column.

## Authoritative schema field model

Each field entry in `schema.fields` becomes a physical-schema record with the following semantics:

- `field_path`: logical field path (canonical, usually `payload....`)
- `logical_field_path`: same logical path, explicit for clarity
- `field_type`: authoritative type at this fork
- `nullable`
- `logical_type`
- `fork_version`: starts at `1`, increments on each fork for the same logical path
- `is_current`: whether this fork is the active authoritative version for that logical path
- `materialized`: whether this entry writes a parquet column
- `physical_column_name`: parquet column name when `materialized=true`

### Materialization rules

- scalar fields: `materialized=true`
- array container fields: `materialized=true` only when they have no flattened descendants
- array/object descendant leaf fields: `materialized=true`
- object container fields: `materialized=false`

## Newest-type-wins rule

When a logical path changes shape:

- old current fork for the same logical path is marked non-current
- conflicting active descendants under that path are marked non-current when the new type is scalar/array
- conflicting active scalar/array ancestors are marked non-current when the new type requires object descendants

Examples:

- scalar -> string fork: old scalar column becomes inactive, new scalar column becomes current
- scalar -> object fork: old scalar column becomes inactive, object container becomes current, descendant object fields become current
- object -> scalar fork: object container and active descendants become inactive, new scalar column becomes current
- object -> array fork: object container and active descendants become inactive, new array column becomes current

## Runtime proposal behavior

The runtime no longer treats type mismatches as breaking changes.

Instead, schema proposals carry `proposed_additions` for:

- brand-new logical fields
- type forks for existing logical fields
- object descendant fields newly observed under a current object
- array/object descendant fields newly observed under a current array path

Arrays are observed structurally. The runtime emits proposal paths such as `payload.items[]` and `payload.items[].sku` rather than preserving only the opaque array root.

## Runtime write behavior

The runtime normalizes a source payload into a flat parquet row:

- emit one top-level parquet column per current materialized authoritative field
- write `None` for missing current fields
- write object descendants into their own flattened columns
- write array descendants into typed list columns (or nested list columns for nested arrays)
- never write inactive forks
- stage rows locally as JSONL and convert to parquet only when the batch seals

Kafka metadata columns remain top-level metadata columns.

## Deferred read-side plan

At a later stage, a view generator can walk authoritative schema metadata and emit:

- `COALESCE(...)` chains for forked scalar/array columns
- `struct_pack(...)` for object re-nesting
- `list_zip`, `list_transform`, `struct_pack`, and related list/json functions for arrays and nested arrays of structs

That read-side is explicitly out of scope for this pass.

## Implementation plan

1. Add shared fork-aware schema helpers for logical-path normalization, physical-column naming, array-descendant handling, and current-field selection.
2. Upgrade authority promotion to append forked physical fields instead of rejecting type changes.
3. Change runtime drift detection so type changes become proposals rather than breaking changes, including array descendants.
4. Change runtime normalization to emit flat physical parquet columns and structurally flattened array list columns.
5. Update authority buffering/subsumption rules to understand current vs historical forks.
6. Add tests for scalar/object/array forks, newest-type-wins transitions, nested array flattening, and flat parquet output.

## Current Gaps

- read-side logical view generation is still deferred
- the WebView/detail API does not yet expose authoritative physical-schema metadata directly
