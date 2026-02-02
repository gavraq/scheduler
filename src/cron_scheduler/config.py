"""YAML configuration loading and validation for cron jobs."""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path("config/jobs.yaml")


@dataclass
class JobConfig:
    """Configuration for a single scheduled job."""
    name: str
    description: str
    enabled: bool
    schedule: str                      # Cron expression
    project: str                       # Working directory on Mac
    prompt: str                        # Message to send to agent
    model: str = "sonnet"
    max_turns: int = 15
    timeout_seconds: int = 600
    target_url: str = ""               # Override default target URL
    timezone: str = "Europe/London"


@dataclass
class SchedulerConfig:
    """Full scheduler configuration."""
    default_target_url: str = "http://localhost:8095/scheduled-job"
    default_timezone: str = "Europe/London"
    jobs: list[JobConfig] = field(default_factory=list)


def load_config(path: Optional[Path] = None) -> SchedulerConfig:
    """Load scheduler configuration from YAML file.

    Args:
        path: Path to YAML config file. Defaults to config/jobs.yaml.

    Returns:
        SchedulerConfig with all job definitions.
    """
    config_path = path or CONFIG_PATH

    if not config_path.exists():
        logger.warning(f"Config file not found: {config_path}")
        return SchedulerConfig()

    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    defaults = raw.get("defaults", {})
    default_url = defaults.get("target_url", "http://localhost:8095/scheduled-job")
    default_tz = defaults.get("timezone", "Europe/London")

    jobs = []
    for job_raw in raw.get("jobs", []):
        try:
            job = JobConfig(
                name=job_raw["name"],
                description=job_raw.get("description", ""),
                enabled=job_raw.get("enabled", True),
                schedule=job_raw["schedule"],
                project=job_raw["project"],
                prompt=job_raw["prompt"],
                model=job_raw.get("model", "sonnet"),
                max_turns=job_raw.get("max_turns", 15),
                timeout_seconds=job_raw.get("timeout_seconds", 600),
                target_url=job_raw.get("target_url", default_url),
                timezone=job_raw.get("timezone", default_tz),
            )
            jobs.append(job)
            logger.info(f"Loaded job: {job.name} (enabled={job.enabled}, schedule={job.schedule})")
        except KeyError as e:
            logger.error(f"Invalid job definition, missing field {e}: {job_raw.get('name', '?')}")

    config = SchedulerConfig(
        default_target_url=default_url,
        default_timezone=default_tz,
        jobs=jobs,
    )

    logger.info(f"Loaded {len(jobs)} jobs ({sum(1 for j in jobs if j.enabled)} enabled)")
    return config


def save_config(config: SchedulerConfig, path: Optional[Path] = None):
    """Save scheduler configuration back to YAML file.

    Args:
        config: SchedulerConfig to persist.
        path: Path to YAML config file. Defaults to config/jobs.yaml.
    """
    config_path = path or CONFIG_PATH

    data = {
        "defaults": {
            "target_url": config.default_target_url,
            "timezone": config.default_timezone,
        },
        "jobs": [],
    }

    for job in config.jobs:
        job_data = {
            "name": job.name,
            "description": job.description,
            "enabled": job.enabled,
            "schedule": job.schedule,
            "project": job.project,
            "prompt": job.prompt,
            "model": job.model,
            "max_turns": job.max_turns,
            "timeout_seconds": job.timeout_seconds,
        }
        # Only include target_url if it differs from default
        if job.target_url and job.target_url != config.default_target_url:
            job_data["target_url"] = job.target_url
        # Only include timezone if it differs from default
        if job.timezone != config.default_timezone:
            job_data["timezone"] = job.timezone
        data["jobs"].append(job_data)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    logger.info(f"Saved config: {len(config.jobs)} jobs")
