"""Cron Scheduler service entrypoint.

Provides:
- FastAPI health/status/run-now endpoints
- CRUD API for job management
- Web dashboard (static HTML)
- Scheduler startup on application load
- Graceful shutdown
"""

import asyncio
import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv
from croniter import croniter

from cron_scheduler.config import JobConfig, load_config, save_config
from cron_scheduler.scheduler import Scheduler
from cron_scheduler.run_log import read_recent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = os.getenv("AGENT_API_KEY", "")
SCHEDULER_API_KEY = os.getenv("SCHEDULER_API_KEY", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8092"))

# Static files path
STATIC_DIR = Path(__file__).parent / "static"

# Global scheduler instance
scheduler: Optional[Scheduler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start scheduler on app startup, stop on shutdown."""
    global scheduler

    config = load_config()
    api_key = API_KEY
    if not api_key:
        logger.warning("AGENT_API_KEY not set - scheduled jobs will fail authentication!")

    scheduler = Scheduler(config, api_key)
    await scheduler.start()

    yield

    await scheduler.stop()
    logger.info("Scheduler shutdown complete")


app = FastAPI(
    title="Cron Scheduler",
    description="Generic cron scheduler service. Sends HTTP POST on cron schedules.",
    version="0.1.0",
    lifespan=lifespan,
)


def _check_scheduler_key(key: Optional[str]):
    """Verify scheduler API key for protected endpoints."""
    if SCHEDULER_API_KEY and key != SCHEDULER_API_KEY:
        raise HTTPException(401, "Invalid scheduler API key")


# ─── Pydantic models for CRUD ────────────────────────────────────────────

class JobCreate(BaseModel):
    """Request body for creating/updating a job."""
    name: str
    description: str = ""
    enabled: bool = True
    schedule: str                     # Cron expression
    project: str                      # Working directory on Mac
    prompt: str                       # Message to send to agent
    model: str = "sonnet"
    max_turns: int = 15
    timeout_seconds: int = 600
    target_url: str = ""              # Override default target URL
    timezone: str = ""                # Override default timezone


class JobUpdate(BaseModel):
    """Request body for partial job updates."""
    description: Optional[str] = None
    enabled: Optional[bool] = None
    schedule: Optional[str] = None
    project: Optional[str] = None
    prompt: Optional[str] = None
    model: Optional[str] = None
    max_turns: Optional[int] = None
    timeout_seconds: Optional[int] = None
    target_url: Optional[str] = None
    timezone: Optional[str] = None


# ─── Dashboard ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the web dashboard."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


# ─── Health & Status ─────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "version": "0.1.0",
        "scheduler_running": scheduler._running if scheduler else False,
    }


# ─── Job Read Endpoints ──────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs():
    """List all jobs and their current status."""
    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")
    return scheduler.get_status()


@app.get("/api/jobs/{job_name}")
async def get_job(job_name: str):
    """Get a single job's configuration and status."""
    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    for job in scheduler.config.jobs:
        if job.name == job_name:
            from cron_scheduler.store import JobState
            js = scheduler.state.jobs.get(job.name, JobState())
            return {
                "name": job.name,
                "description": job.description,
                "enabled": job.enabled,
                "schedule": job.schedule,
                "project": job.project,
                "prompt": job.prompt,
                "model": job.model,
                "max_turns": job.max_turns,
                "timeout_seconds": job.timeout_seconds,
                "target_url": job.target_url,
                "timezone": job.timezone,
                "next_run_at": js.next_run_at,
                "running_since": js.running_since,
                "last_run_at": js.last_run_at,
                "last_status": js.last_status,
                "last_error": js.last_error,
                "last_duration_seconds": js.last_duration_seconds,
                "last_response_summary": js.last_response_summary,
                "total_runs": js.total_runs,
                "total_errors": js.total_errors,
            }

    raise HTTPException(404, f"Job not found: {job_name}")


@app.get("/api/jobs/{job_name}/logs")
async def job_logs(job_name: str, limit: int = 20):
    """Get recent execution logs for a job."""
    entries = read_recent(job_name, limit=min(limit, 100))
    return {"job_name": job_name, "count": len(entries), "entries": entries}


# ─── Job Write Endpoints ─────────────────────────────────────────────────

@app.post("/api/jobs")
async def create_job(body: JobCreate, x_api_key: str = Header(None)):
    """Create a new scheduled job."""
    _check_scheduler_key(x_api_key)

    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    # Validate cron expression
    try:
        croniter(body.schedule)
    except (ValueError, KeyError) as e:
        raise HTTPException(400, f"Invalid cron expression: {e}")

    # Check for duplicate name
    for job in scheduler.config.jobs:
        if job.name == body.name:
            raise HTTPException(409, f"Job already exists: {body.name}")

    # Create job config
    new_job = JobConfig(
        name=body.name,
        description=body.description,
        enabled=body.enabled,
        schedule=body.schedule,
        project=body.project,
        prompt=body.prompt,
        model=body.model,
        max_turns=body.max_turns,
        timeout_seconds=body.timeout_seconds,
        target_url=body.target_url or scheduler.config.default_target_url,
        timezone=body.timezone or scheduler.config.default_timezone,
    )

    # Add to config and state
    scheduler.config.jobs.append(new_job)
    scheduler.add_job(new_job)

    # Persist to YAML
    save_config(scheduler.config)

    return {"status": "ok", "message": f"Job '{body.name}' created"}


@app.put("/api/jobs/{job_name}")
async def update_job(job_name: str, body: JobUpdate, x_api_key: str = Header(None)):
    """Update an existing job's configuration."""
    _check_scheduler_key(x_api_key)

    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    # Find the job
    target_job = None
    for job in scheduler.config.jobs:
        if job.name == job_name:
            target_job = job
            break

    if not target_job:
        raise HTTPException(404, f"Job not found: {job_name}")

    # Validate cron expression if being updated
    if body.schedule is not None:
        try:
            croniter(body.schedule)
        except (ValueError, KeyError) as e:
            raise HTTPException(400, f"Invalid cron expression: {e}")

    # Apply updates
    if body.description is not None:
        target_job.description = body.description
    if body.enabled is not None:
        target_job.enabled = body.enabled
    if body.schedule is not None:
        target_job.schedule = body.schedule
    if body.project is not None:
        target_job.project = body.project
    if body.prompt is not None:
        target_job.prompt = body.prompt
    if body.model is not None:
        target_job.model = body.model
    if body.max_turns is not None:
        target_job.max_turns = body.max_turns
    if body.timeout_seconds is not None:
        target_job.timeout_seconds = body.timeout_seconds
    if body.target_url is not None:
        target_job.target_url = body.target_url
    if body.timezone is not None:
        target_job.timezone = body.timezone

    # Recompute next run if schedule or enabled changed
    if body.schedule is not None or body.enabled is not None:
        scheduler.recompute_job(target_job)

    # Persist to YAML
    save_config(scheduler.config)

    return {"status": "ok", "message": f"Job '{job_name}' updated"}


@app.delete("/api/jobs/{job_name}")
async def delete_job(job_name: str, x_api_key: str = Header(None)):
    """Delete a scheduled job."""
    _check_scheduler_key(x_api_key)

    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    # Find and remove
    for i, job in enumerate(scheduler.config.jobs):
        if job.name == job_name:
            scheduler.config.jobs.pop(i)
            scheduler.remove_job(job_name)
            save_config(scheduler.config)
            return {"status": "ok", "message": f"Job '{job_name}' deleted"}

    raise HTTPException(404, f"Job not found: {job_name}")


@app.post("/api/jobs/{job_name}/toggle")
async def toggle_job(job_name: str, x_api_key: str = Header(None)):
    """Toggle a job's enabled state."""
    _check_scheduler_key(x_api_key)

    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    for job in scheduler.config.jobs:
        if job.name == job_name:
            job.enabled = not job.enabled
            scheduler.recompute_job(job)
            save_config(scheduler.config)
            return {
                "status": "ok",
                "enabled": job.enabled,
                "message": f"Job '{job_name}' {'enabled' if job.enabled else 'disabled'}",
            }

    raise HTTPException(404, f"Job not found: {job_name}")


@app.post("/api/jobs/{job_name}/run-now")
async def run_now(job_name: str, x_api_key: str = Header(None)):
    """Manually trigger a job immediately."""
    _check_scheduler_key(x_api_key)

    if not scheduler:
        raise HTTPException(503, "Scheduler not initialised")

    success = await scheduler.run_now(job_name)
    if not success:
        raise HTTPException(404, f"Job not found: {job_name}")

    return {"status": "ok", "message": f"Job '{job_name}' executed"}


# ─── Legacy endpoints (backwards compat) ─────────────────────────────────

@app.get("/jobs")
async def list_jobs_legacy():
    """Legacy endpoint - redirects to /api/jobs."""
    return await list_jobs()


@app.get("/jobs/{job_name}/logs")
async def job_logs_legacy(job_name: str, limit: int = 10):
    """Legacy endpoint."""
    return await job_logs(job_name, limit)


@app.post("/jobs/{job_name}/run-now")
async def run_now_legacy(job_name: str, x_api_key: str = Header(None)):
    """Legacy endpoint."""
    return await run_now(job_name, x_api_key)


def run():
    """Entry point for running the server."""
    import uvicorn

    logger.info(f"Starting Cron Scheduler on {HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=int(PORT))


if __name__ == "__main__":
    run()
