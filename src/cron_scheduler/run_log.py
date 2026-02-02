"""JSONL execution log for scheduled jobs.

Append-only log with auto-pruning, inspired by OpenClaw's run-log.ts.
One JSONL file per job name in data/runs/.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LOG_DIR = Path("data/runs")
MAX_BYTES = 2_000_000  # 2MB
MAX_LINES = 2000


def append_run(
    job_name: str,
    status: str,
    duration_seconds: float = 0,
    response_summary: str = "",
    error: Optional[str] = None,
    http_status: Optional[int] = None,
):
    """Append a run log entry for a job."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = job_name.replace(" ", "-").lower()
    log_path = LOG_DIR / f"{safe_name}.jsonl"

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "job_name": job_name,
        "status": status,
        "duration_seconds": round(duration_seconds, 2),
        "response_summary": response_summary[:500],
    }
    if error:
        entry["error"] = error[:500]
    if http_status is not None:
        entry["http_status"] = http_status

    with open(log_path, "a") as f:
        f.write(json.dumps(entry) + "\n")

    # Auto-prune if too large
    _prune_if_needed(log_path)


def read_recent(job_name: str, limit: int = 10) -> list[dict]:
    """Read the most recent log entries for a job."""
    safe_name = job_name.replace(" ", "-").lower()
    log_path = LOG_DIR / f"{safe_name}.jsonl"

    if not log_path.exists():
        return []

    lines = log_path.read_text().strip().split("\n")
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    return entries


def _prune_if_needed(log_path: Path):
    """Prune log file if it exceeds size limits."""
    try:
        if log_path.stat().st_size > MAX_BYTES:
            lines = log_path.read_text().strip().split("\n")
            if len(lines) > MAX_LINES:
                kept = lines[-MAX_LINES:]
                log_path.write_text("\n".join(kept) + "\n")
                logger.info(f"Pruned {log_path.name}: kept {len(kept)} of {len(lines)} entries")
    except Exception as e:
        logger.warning(f"Failed to prune {log_path}: {e}")
