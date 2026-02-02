"""Atomic JSON state persistence for job execution state.

Inspired by OpenClaw's store.ts: temp file → rename → backup.
Ensures the state file is never in a partially-written state.
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

STATE_PATH = Path("data/job_state.json")


@dataclass
class JobState:
    """Mutable execution state for a single job."""
    next_run_at: Optional[str] = None       # ISO timestamp
    running_since: Optional[str] = None     # ISO timestamp (acts as lock)
    last_run_at: Optional[str] = None       # ISO timestamp
    last_status: Optional[str] = None       # "ok" | "error" | "skipped"
    last_error: Optional[str] = None
    last_duration_seconds: float = 0
    last_response_summary: str = ""
    total_runs: int = 0
    total_errors: int = 0


@dataclass
class SchedulerState:
    """Full scheduler state including all job states."""
    version: int = 1
    jobs: dict[str, JobState] = field(default_factory=dict)
    last_saved: Optional[str] = None


def atomic_write(path: Path, data: str):
    """Write atomically: temp file in same dir → rename → backup."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        os.write(fd, data.encode())
        os.close(fd)
        os.rename(tmp_path, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_state(path: Optional[Path] = None) -> SchedulerState:
    """Load scheduler state from JSON file."""
    state_path = path or STATE_PATH

    if not state_path.exists():
        logger.info("No existing state file. Starting fresh.")
        return SchedulerState()

    try:
        raw = json.loads(state_path.read_text())
        state = SchedulerState(version=raw.get("version", 1))

        for name, job_raw in raw.get("jobs", {}).items():
            state.jobs[name] = JobState(
                next_run_at=job_raw.get("next_run_at"),
                running_since=job_raw.get("running_since"),
                last_run_at=job_raw.get("last_run_at"),
                last_status=job_raw.get("last_status"),
                last_error=job_raw.get("last_error"),
                last_duration_seconds=job_raw.get("last_duration_seconds", 0),
                last_response_summary=job_raw.get("last_response_summary", ""),
                total_runs=job_raw.get("total_runs", 0),
                total_errors=job_raw.get("total_errors", 0),
            )

        logger.info(f"Loaded state for {len(state.jobs)} jobs")
        return state

    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse state file: {e}. Starting fresh.")
        return SchedulerState()


def save_state(state: SchedulerState, path: Optional[Path] = None):
    """Save scheduler state atomically."""
    state_path = path or STATE_PATH
    state.last_saved = datetime.now(timezone.utc).isoformat()

    data = {
        "version": state.version,
        "last_saved": state.last_saved,
        "jobs": {name: asdict(js) for name, js in state.jobs.items()},
    }

    atomic_write(state_path, json.dumps(data, indent=2))
    logger.debug(f"State saved: {len(state.jobs)} jobs")


# Stuck job detection (OpenClaw pattern: 2 hours)
STUCK_THRESHOLD_SECONDS = 2 * 60 * 60


def clear_stuck_jobs(state: SchedulerState) -> list[str]:
    """Clear jobs that have been running for too long.

    Returns list of job names that were cleared.
    """
    cleared = []
    now = datetime.now(timezone.utc)

    for name, job_state in state.jobs.items():
        if job_state.running_since:
            try:
                started = datetime.fromisoformat(job_state.running_since)
                elapsed = (now - started).total_seconds()
                if elapsed > STUCK_THRESHOLD_SECONDS:
                    logger.warning(
                        f"Clearing stuck job '{name}' "
                        f"(running for {elapsed:.0f}s)"
                    )
                    job_state.running_since = None
                    job_state.last_status = "error"
                    job_state.last_error = f"Stuck: cleared after {elapsed:.0f}s"
                    job_state.total_errors += 1
                    cleared.append(name)
            except (ValueError, TypeError):
                job_state.running_since = None

    return cleared
