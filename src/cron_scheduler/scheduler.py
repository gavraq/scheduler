"""Smart timer scheduler engine.

Event-driven scheduling inspired by OpenClaw's service/timer.ts:
- Computes next wake time from all enabled jobs
- Sleeps until the soonest due job (zero CPU when idle)
- Executes due jobs and re-arms the timer

Uses croniter for cron expression parsing (Python equivalent of croner).
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from croniter import croniter

from cron_scheduler.config import JobConfig, SchedulerConfig
from cron_scheduler.store import (
    JobState,
    SchedulerState,
    load_state,
    save_state,
    clear_stuck_jobs,
)
from cron_scheduler.client import execute_job
from cron_scheduler.run_log import append_run

logger = logging.getLogger(__name__)


class Scheduler:
    """Generic cron scheduler with smart timer pattern.

    The scheduler knows nothing about the jobs' content — it only
    knows about timing and HTTP delivery.
    """

    def __init__(self, config: SchedulerConfig, api_key: str):
        self.config = config
        self.api_key = api_key
        self.state = load_state()
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Initialise state for any new jobs
        for job in config.jobs:
            if job.name not in self.state.jobs:
                self.state.jobs[job.name] = JobState()

        # Compute initial next-run times
        for job in config.jobs:
            if job.enabled:
                js = self.state.jobs[job.name]
                if not js.next_run_at:
                    js.next_run_at = self._compute_next_run(job.schedule).isoformat()

        save_state(self.state)

    def _compute_next_run(self, schedule: str) -> datetime:
        """Compute the next run time from a cron expression."""
        cron = croniter(schedule, datetime.now(timezone.utc))
        return cron.get_next(datetime).replace(tzinfo=timezone.utc)

    def _soonest_due_job(self) -> Optional[tuple[JobConfig, datetime]]:
        """Find the enabled job with the earliest next_run_at."""
        soonest = None
        soonest_time = None

        for job in self.config.jobs:
            if not job.enabled:
                continue

            js = self.state.jobs.get(job.name)
            if not js or not js.next_run_at:
                continue

            # Skip jobs currently running
            if js.running_since:
                continue

            try:
                next_at = datetime.fromisoformat(js.next_run_at)
                if soonest_time is None or next_at < soonest_time:
                    soonest = job
                    soonest_time = next_at
            except (ValueError, TypeError):
                continue

        if soonest and soonest_time:
            return (soonest, soonest_time)
        return None

    async def _execute_job(self, job: JobConfig):
        """Execute a single job: send HTTP POST and update state."""
        js = self.state.jobs[job.name]
        start = time.time()
        js.running_since = datetime.now(timezone.utc).isoformat()
        save_state(self.state)

        logger.info(f"Executing job: {job.name}")

        result = await execute_job(
            target_url=job.target_url or self.config.default_target_url,
            api_key=self.api_key,
            project=job.project,
            prompt=job.prompt,
            model=job.model,
            max_turns=job.max_turns,
            timeout_seconds=job.timeout_seconds,
            job_name=job.name,
        )

        duration = time.time() - start
        js.running_since = None
        js.last_run_at = datetime.now(timezone.utc).isoformat()
        js.last_duration_seconds = round(duration, 2)

        if result.success:
            js.last_status = "ok"
            js.last_error = None
            js.last_response_summary = result.response_summary
            js.total_runs += 1
            logger.info(
                f"Job '{job.name}' completed successfully in {duration:.1f}s: "
                f"{result.response_summary}"
            )
        else:
            js.last_status = "error"
            js.last_error = result.error
            js.total_errors += 1
            logger.error(f"Job '{job.name}' failed: {result.error}")

        # Compute next run
        js.next_run_at = self._compute_next_run(job.schedule).isoformat()

        # Persist state and log
        save_state(self.state)
        append_run(
            job_name=job.name,
            status="ok" if result.success else "error",
            duration_seconds=duration,
            response_summary=result.response_summary,
            error=result.error,
            http_status=result.http_status,
        )

    async def _timer_loop(self):
        """Main timer loop: sleep until next due job, execute, repeat.

        This is the smart timer pattern from OpenClaw:
        - Find the soonest due job
        - Sleep until that time (zero CPU while idle)
        - Execute all due jobs
        - Re-arm the timer
        """
        logger.info("Scheduler timer loop started")

        while self._running:
            # Clear any stuck jobs
            cleared = clear_stuck_jobs(self.state)
            if cleared:
                save_state(self.state)
                logger.warning(f"Cleared stuck jobs: {cleared}")

            # Find next due job
            due = self._soonest_due_job()
            if not due:
                logger.info("No enabled jobs with scheduled runs. Sleeping 60s.")
                await asyncio.sleep(60)
                continue

            job, next_at = due
            now = datetime.now(timezone.utc)
            delay = max((next_at - now).total_seconds(), 0)

            if delay > 0:
                logger.info(
                    f"Next job: '{job.name}' at {next_at.strftime('%H:%M:%S')} "
                    f"(in {delay:.0f}s)"
                )
                await asyncio.sleep(delay)

            # Re-check after sleep (config may have changed)
            if not self._running:
                break

            # Execute all due jobs (there may be multiple)
            now = datetime.now(timezone.utc)
            for j in self.config.jobs:
                if not j.enabled:
                    continue
                js = self.state.jobs.get(j.name)
                if not js or not js.next_run_at or js.running_since:
                    continue
                try:
                    run_at = datetime.fromisoformat(js.next_run_at)
                    if run_at <= now:
                        await self._execute_job(j)
                except (ValueError, TypeError):
                    continue

    async def start(self):
        """Start the scheduler."""
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        enabled_count = sum(1 for j in self.config.jobs if j.enabled)
        logger.info(
            f"Starting scheduler with {enabled_count} enabled jobs "
            f"(of {len(self.config.jobs)} total)"
        )

        for job in self.config.jobs:
            if job.enabled:
                js = self.state.jobs.get(job.name)
                if js and js.next_run_at:
                    logger.info(f"  {job.name}: next run at {js.next_run_at}")

        self._task = asyncio.create_task(self._timer_loop())

    async def stop(self):
        """Stop the scheduler gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        save_state(self.state)
        logger.info("Scheduler stopped")

    async def run_now(self, job_name: str) -> bool:
        """Manually trigger a job immediately."""
        for job in self.config.jobs:
            if job.name == job_name:
                await self._execute_job(job)
                return True
        logger.error(f"Job not found: {job_name}")
        return False

    def add_job(self, job: JobConfig):
        """Add a new job to the scheduler's live state."""
        self.state.jobs[job.name] = JobState()
        if job.enabled:
            self.state.jobs[job.name].next_run_at = self._compute_next_run(job.schedule).isoformat()
        save_state(self.state)
        logger.info(f"Added job: {job.name}")

    def remove_job(self, job_name: str):
        """Remove a job from the scheduler's live state."""
        self.state.jobs.pop(job_name, None)
        save_state(self.state)
        logger.info(f"Removed job: {job_name}")

    def recompute_job(self, job: JobConfig):
        """Recompute next run time for a job after config change."""
        js = self.state.jobs.get(job.name)
        if not js:
            js = JobState()
            self.state.jobs[job.name] = js

        if job.enabled:
            js.next_run_at = self._compute_next_run(job.schedule).isoformat()
        else:
            js.next_run_at = None
        save_state(self.state)
        logger.info(f"Recomputed job '{job.name}': enabled={job.enabled}, next_run={js.next_run_at}")

    def get_status(self) -> dict:
        """Get current scheduler status."""
        jobs_status = []
        for job in self.config.jobs:
            js = self.state.jobs.get(job.name, JobState())
            jobs_status.append({
                "name": job.name,
                "description": job.description,
                "enabled": job.enabled,
                "schedule": job.schedule,
                "project": job.project,
                "model": job.model,
                "next_run_at": js.next_run_at,
                "running_since": js.running_since,
                "last_run_at": js.last_run_at,
                "last_status": js.last_status,
                "last_duration_seconds": js.last_duration_seconds,
                "total_runs": js.total_runs,
                "total_errors": js.total_errors,
            })

        return {
            "running": self._running,
            "jobs": jobs_status,
        }
