"""Deferred acknowledgement of source records, so a runtime commits only offsets that are durable.

The writer agent runs with Kaspr's ``noack``: Faust no longer acknowledges a record when the agent
has finished with it, and its offset becomes committable only once this module acknowledges it. A
record whose row went into a batch is acknowledged only after that batch is durable in the table
(a commit's offsets cover it); every other record - skipped, a tombstone, one the table already
holds - only has to be acknowledged in order. A member that stops, crashes or
loses its partitions therefore resumes from an offset whose records are all in the table, and reads
again whatever it had only staged.

Faust commits, per partition, the end of the first run of consecutive acknowledged offsets
(``Consumer._new_offset``), counting the gaps it saw while fetching - offsets compaction removed -
as acknowledged. One unacknowledged record holds the committed offset at or below itself, so this
ledger holds back only one record per batch: its first, the batch's *guard*. Every other record is
acknowledged at once, as Faust's automatic acknowledgement did.

Faust's commit logic honours a guard only when (twm-faust 1.17.22):

- the partition has a committed offset. ``_new_offset`` checks the run against the committed offset
  only when there is a truthy one, so in a new consumer group the first run of acknowledged offsets is
  committed wherever it starts, past an unacknowledged guard below it.
- the guard does not directly follow a gap. ``_add_gap`` records a gap with the fetched record itself
  inside it (``addi(from, to + 1)``), so that record counts as acknowledged once a later one is.

When either fails, the batch is *ordered*: the ledger holds every record from the batch's first one
on, and acknowledges them strictly in offset order once the batch is durable. That is correct
whatever Faust does with runs and gaps, at the cost of holding those messages, so the writer rolls an
ordered batch early (`ORDERED_BATCH_MAX_ROWS`). The check reads Faust's committed offsets and gaps;
a consumer without them gets ordered batches throughout.

Two further rules keep a session consistent:

- Every rebalance ends with Faust seeking each assigned partition back to its committed offset, so
  whatever this member held is read again. A ledger therefore belongs to one session, named by the
  app's rebalance count (`generation`); nothing is acknowledged while the app rebalances, nor for a
  record or ledger of an earlier generation. A record the writer received before a rebalance and
  finished after it is simply dropped - it is delivered again - and so is a stale ledger.
- Every record the writer finishes with must be registered, whatever happened to it: one never
  registered would hold the committed offset where it is for the rest of the session. Only a record
  interrupted by shutdown, or by a failure that restarts the member, is left unregistered, and is
  read again.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

#: Rows at which the writer rolls a batch whose records are held in order. Each held record keeps
#: its Faust message, payload included, until the batch is durable.
ORDERED_BATCH_MAX_ROWS = 5_000


@dataclass(slots=True)
class _HeldRecord:
    message: Any
    offset: int
    batch_id: str | None
    starts_batch: bool


@dataclass(slots=True)
class _BatchAcks:
    ordered: bool
    durable: bool = False
    #: The batch's first record, held until the batch is durable; guarded batches only.
    guard: Any = None


@dataclass(slots=True)
class _PartitionLedger:
    consumer: Any
    #: The app's rebalance count when this ledger's session began; None outside a Faust app.
    generation: int | None = None
    queue: deque[_HeldRecord] = field(default_factory=deque)
    batches: dict[str, _BatchAcks] = field(default_factory=dict)
    #: Set when records turn up already acknowledged: the agent was deployed without `noack`
    #: (an operator whose CRD lacks the field drops it), so Faust acknowledges on its own and
    #: holding anything would only cost memory.
    acknowledged_elsewhere: bool = False
    acknowledged: int = 0


#: One ledger per source topic-partition, named by the table it feeds and the partition.
_LEDGERS: dict[tuple[str, int], _PartitionLedger] = {}


def reset_acks() -> None:
    """Forget every ledger without acknowledging anything. Tests use this between cases."""
    _LEDGERS.clear()


def register_record(
    app,
    event,
    *,
    table: str,
    partition: int,
    generation: int | None,
    batch_id: str | None = None,
    starts_batch: bool = False,
) -> None:
    """Hand the writer's finished record to the ledger, which acknowledges it when it is safe.

    `generation` is the app's rebalance count when the writer received the record; `batch_id` names
    the batch the record's row went into, if it went into one; `starts_batch` says the row was that
    batch's first.
    """
    message = getattr(event, "message", None)
    if message is None or not callable(getattr(message, "ack", None)):
        return
    consumer = _consumer_for(app, event)
    if consumer is None:
        return
    key = (table, partition)
    current = current_generation(app)
    if current is not None and (generation != current or _rebalancing(app)):
        # Received in a session a rebalance has ended: Faust has sought the partition back to its
        # committed offset, which this unacknowledged record is not below, so it is read again.
        return
    ledger = _LEDGERS.get(key)
    if ledger is not None and ledger.generation != current:
        del _LEDGERS[key]
        ledger = None
    if ledger is None:
        ledger = _LEDGERS[key] = _PartitionLedger(consumer=consumer, generation=current)
    if ledger.acknowledged_elsewhere:
        return
    if _held_message_acknowledged_elsewhere(ledger):
        ledger.acknowledged_elsewhere = True
        ledger.queue.clear()
        ledger.batches.clear()
        LOGGER.warning(
            "Source records of table %s partition %s are acknowledged as soon as they are "
            "processed, so offsets commit before their rows are durable; the writer agent is "
            "running without noack.",
            table,
            partition,
        )
        return
    offset = getattr(message, "offset", None)
    if not isinstance(offset, int):
        return
    if batch_id is not None and starts_batch:
        ledger.batches[batch_id] = _BatchAcks(
            ordered=not guard_is_honoured(consumer, getattr(message, "tp", None), offset),
        )
    elif batch_id is not None and batch_id not in ledger.batches:
        # A row for a batch whose first record never reached the ledger: there is no guard to rely
        # on, so hold its records in order rather than let one be acknowledged unprotected.
        ledger.batches[batch_id] = _BatchAcks(ordered=True)
    ledger.queue.append(
        _HeldRecord(message=message, offset=offset, batch_id=batch_id, starts_batch=starts_batch)
    )
    _drain(app, ledger)


def batch_durable(
    app,
    *,
    table: str,
    partition: int,
    batch_id: str,
) -> None:
    """Acknowledge what a batch held back, now that a commit to its table holds its rows."""
    ledger = _LEDGERS.get((table, partition))
    if ledger is None:
        return
    if ledger.generation != current_generation(app):
        # The batch's session has ended; its records are read again.
        _LEDGERS.pop((table, partition), None)
        return
    batch = ledger.batches.get(batch_id)
    if batch is None:
        return
    batch.durable = True
    if batch.guard is not None and _acknowledging(app, ledger):
        _ack(ledger, batch.guard)
        batch.guard = None
    _drain(app, ledger)
    if batch.guard is None and not any(held.batch_id == batch_id for held in ledger.queue):
        ledger.batches.pop(batch_id, None)


def forget_partition(table: str, partition: int) -> int:
    """Drop a partition's held records without acknowledging them; return how many were held.

    Called when a rebalance has ended the session they belong to: Faust has sought the partition
    back to its committed offset, so they will be read again.
    """
    ledger = _LEDGERS.pop((table, partition), None)
    if ledger is None:
        return 0
    return len(ledger.queue) + sum(1 for batch in ledger.batches.values() if batch.guard is not None)


def batch_is_ordered(table: str, partition: int, batch_id: str) -> bool:
    """Whether a batch's records are being held in order, which the writer rolls early."""
    ledger = _LEDGERS.get((table, partition))
    if ledger is None:
        return False
    batch = ledger.batches.get(batch_id)
    return batch is not None and batch.ordered


def ack_snapshot(materialization_id: str | None = None) -> dict[str, int]:
    """Counts for the status heartbeat: what this member is holding back, and why, every table's together.

    A runtime process serves one materialization, so every ledger here is its own.
    """
    held_records = 0
    guards = 0
    ordered_batches = 0
    pending_batches = 0
    for ledger in _LEDGERS.values():
        held_records += len(ledger.queue)
        for batch in ledger.batches.values():
            if batch.durable:
                continue
            pending_batches += 1
            if batch.ordered:
                ordered_batches += 1
            if batch.guard is not None:
                guards += 1
    return {
        "held_records": held_records,
        "held_guards": guards,
        "unacknowledged_batches": pending_batches,
        "ordered_batches": ordered_batches,
    }


def guard_is_honoured(consumer, tp, offset: int) -> bool:
    """Whether Faust's commit logic stops at `offset` while that record is unacknowledged.

    Reads the consumer's committed offsets and fetch gaps, as Faust's own aiokafka driver reads the
    former; anything unexpected about them answers no, which only costs memory.
    """
    if tp is None:
        return False
    committed_offsets = getattr(consumer, "_committed_offset", None)
    if not isinstance(committed_offsets, Mapping):
        return False
    committed = committed_offsets.get(tp)
    if not isinstance(committed, int) or isinstance(committed, bool) or committed < 1:
        return False
    if committed > offset:
        return False
    gaps = getattr(consumer, "_gap", None)
    if not isinstance(gaps, Mapping):
        return False
    tree = gaps.get(tp)
    if not tree:
        return True
    try:
        return not tree.at(offset)
    except Exception:
        return False


def _drain(app, ledger: _PartitionLedger) -> None:
    if not _acknowledging(app, ledger):
        return
    queue = ledger.queue
    while queue:
        head = queue[0]
        if head.batch_id is None:
            _ack(ledger, head.message)
            queue.popleft()
            continue
        batch = ledger.batches.get(head.batch_id)
        if batch is None or batch.durable:
            _ack(ledger, head.message)
            queue.popleft()
            continue
        if batch.ordered:
            return
        if head.starts_batch:
            batch.guard = head.message
        else:
            _ack(ledger, head.message)
        queue.popleft()


def _ack(ledger: _PartitionLedger, message) -> None:
    try:
        message.ack(ledger.consumer)
    except Exception:
        LOGGER.exception("Could not acknowledge source record at offset %s", getattr(message, "offset", None))
        return
    ledger.acknowledged += 1


def _held_message_acknowledged_elsewhere(ledger: _PartitionLedger) -> bool:
    for batch in ledger.batches.values():
        if batch.guard is not None and getattr(batch.guard, "acked", False):
            return True
    if ledger.queue and getattr(ledger.queue[-1].message, "acked", False):
        return True
    return False


def _consumer_for(app, event):
    consumer = None if app is None else getattr(app, "consumer", None)
    if consumer is None:
        event_app = getattr(event, "app", None)
        consumer = None if event_app is None else getattr(event_app, "consumer", None)
    return consumer


def current_generation(app) -> int | None:
    """The app's rebalance count, which names the current session; None outside a Faust app."""
    generation = getattr(app, "rebalancing_count", None)
    if isinstance(generation, bool) or not isinstance(generation, int):
        return None
    return generation


def _acknowledging(app, ledger: _PartitionLedger) -> bool:
    return not _rebalancing(app) and ledger.generation == current_generation(app)


def _rebalancing(app) -> bool:
    return bool(getattr(app, "rebalancing", False))
