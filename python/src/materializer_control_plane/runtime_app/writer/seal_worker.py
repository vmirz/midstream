"""Seal one staged batch in a process of its own.

    python -m materializer_control_plane.runtime_app.writer.seal_worker <job.json>

The conversion is Python work that holds the GIL for seconds to tens of seconds per batch, so a
thread would still stall the runtime's event loop, which is also its Kafka consumer's. A child
process runs it on another core instead. Prints the result as JSON on success; exits non-zero with
the error on standard error otherwise.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

#: Niceness added to the seal process, so a member short of CPU keeps consuming before it seals.
SEAL_PROCESS_NICENESS = 10


def main(argv: list[str] | None = None) -> int:
    """Run one seal and return the process exit code."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: seal_worker <job.json>", file=sys.stderr)
        return 2
    try:
        os.nice(SEAL_PROCESS_NICENESS)
    except (AttributeError, OSError):
        pass
    from materializer_control_plane.runtime_app.writer.seal import SealJob, run_seal

    try:
        job = SealJob.from_dict(json.loads(Path(args[0]).read_text(encoding="utf-8")))
        output = run_seal(job)
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(output.to_dict(), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
