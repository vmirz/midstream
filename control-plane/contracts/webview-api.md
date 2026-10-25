# WebView API Contract

## Endpoints

- `GET /materializations`
- `POST /materializations`
- `GET /materializations/{id}`
- `POST /materializations/{id}/pause`
- `POST /materializations/{id}/resume`
- `DELETE /materializations/{id}`

## Listing Endpoint

### `GET /materializations`

Returns a bounded list of materializations.

Query parameters:

- `limit`: one of `10`, `30`, `100`, or `all`

Recommended default:

- `limit=30`

Rationale:

- materializations are stored in a key/value KasprTable rather than a rich query index
- list retrieval should remain compatible with prefix-based scanning
- fixed limit options make API cost predictable and keep the endpoint performant

Implementation guidance:

```python
for key, value in subscriptions_table.prefix_scan("materialization:"):
    ...
```

The control plane should:

- scan only the `materialization:` prefix
- stop iteration once the requested limit is reached, unless `limit=all`
- avoid free-form filters in the MVP
- return compact list items rather than full detail payloads

Suggested response shape:

```json
{
  "items": [
    {
      "materialization_id": "mat-123",
      "name": "orders-materialization",
      "source_topic": "orders",
      "destination_uri": "abfs://lakehouse/orders",
      "desired_phase": "running",
      "observed_phase": "running",
      "phase_reason": "runtime_ready",
      "updated_at": "2026-07-11T10:00:00Z"
    }
  ],
  "limit": "30",
  "returned": 1
}
```

Versioned schema:

- `control-plane/contracts/materializations-list-response.schema.json`

MVP constraints:

- do not add substring search, arbitrary sorting, or field-based filtering
- if a future need arises for richer queries, introduce a dedicated secondary index or materialized summary table rather than overloading prefix scan

## Common Response Model

All mutation endpoints should return an accepted response with both desired and observed state.

Suggested response shape:

```json
{
  "materialization_id": "mat-123",
  "request_id": "req-01jzwebview",
  "desired_phase": "running",
  "observed_phase": "requested",
  "phase_reason": "requested",
  "status_url": "/materializations/mat-123",
  "accepted_at": "2026-07-10T12:00:00Z"
}
```

## `POST /materializations`

Creates a new materialization request.

Suggested request body:

```json
{
  "name": "orders-materialization",
  "source_topic": "orders",
  "destination_uri": "abfs://lakehouse/orders",
  "runtime_mode": "dedicated",
  "runtime_name": null,
  "namespace": "kafka-sql",
  "resource_profile": "small",
  "labels": {
    "domain": "orders"
  },
  "annotations": {}
}
```

Validation rules:

- `source_topic` is required
- `destination_uri` is required
- `runtime_mode` defaults to `dedicated`
- `runtime_name` defaults to the control-plane naming helper for the materialization id and is normalized before it is rendered into runtime resources
- `namespace` defaults to `kafka-sql` if absent

## `GET /materializations/{id}`

Returns the current control-plane view.

Suggested response shape:

```json
{
  "materialization": {
    "materialization_id": "mat-123",
    "name": "orders-materialization",
    "source_topic": "orders",
    "destination_uri": "abfs://lakehouse/orders",
    "runtime_mode": "dedicated",
    "namespace": "kafka-sql"
  },
  "status": {
    "desired_phase": "running",
    "observed_phase": "running",
    "phase_reason": "runtime_ready",
    "phase_message": "KasprApp is ready and healthy runtime heartbeat is recent.",
    "last_transition_time": "2026-07-10T12:01:00Z",
    "last_heartbeat": "2026-07-10T12:00:55Z",
    "last_resource_observation_time": "2026-07-10T12:00:58Z",
    "runtime_health": {
      "level": "healthy",
      "reason": null,
      "message": null
    }
  },
  "runtime_binding": {
    "kaspr_app_name": "kaspr-materializer-orders",
    "namespace": "kafka-sql",
    "managed_agent_names": [
      "writer",
      "schema-updates",
      "signals"
    ]
  }
}
```

Current implementation notes:

- `runtime_binding` may be `null` before the runtime has been applied
- detail responses do not currently inline runtime progress counters, pending upload backlog, or performance windows; those remain in runtime status events and persisted status state rather than the WebView detail payload

## `POST /materializations/{id}/pause`

Behavior:

- set desired phase to `paused`
- persist request state
- emit a control-plane request event
- return accepted response immediately

## `POST /materializations/{id}/resume`

Behavior:

- set desired phase to `running`
- persist request state
- emit a control-plane request event
- return accepted response immediately

## `DELETE /materializations/{id}`

Behavior:

- set desired phase to `deleting`
- persist request state
- emit a control-plane request event
- return accepted response immediately

## Error Model

Suggested synchronous validation error shape:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "source_topic is required",
    "details": {
      "field": "source_topic"
    }
  }
}
```

The WebView should only return synchronous validation or lookup errors. Workflow execution errors should be reflected in later `GET` status responses rather than blocking the mutation endpoint.