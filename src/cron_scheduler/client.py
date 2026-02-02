"""HTTP client for delivering job payloads to the Mac agent service."""

import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass
class JobResult:
    """Result from a job execution."""
    success: bool
    status: str                         # "ok" or "error"
    http_status: int = 0
    response_summary: str = ""
    duration_ms: int = 0
    cost_usd: Optional[float] = None
    skills_used: list[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.skills_used is None:
            self.skills_used = []


async def execute_job(
    target_url: str,
    api_key: str,
    project: str,
    prompt: str,
    model: str = "sonnet",
    max_turns: int = 15,
    timeout_seconds: int = 600,
    job_name: str = "",
) -> JobResult:
    """Send a job payload to the Mac agent service.

    Args:
        target_url: URL of the /scheduled-job endpoint
        api_key: API key for authentication
        project: Working directory on the Mac
        prompt: Message to send to the agent
        model: Model name (haiku/sonnet/opus)
        max_turns: Maximum agent turns
        timeout_seconds: HTTP timeout
        job_name: Job identifier for logging

    Returns:
        JobResult with execution details.
    """
    payload = {
        "project": project,
        "prompt": prompt,
        "model": model,
        "max_turns": max_turns,
        "timeout_seconds": timeout_seconds,
        "job_name": job_name,
    }

    logger.info(f"Executing job '{job_name}' → {target_url}")
    logger.debug(f"Payload: model={model}, max_turns={max_turns}, project={project}")

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds + 30) as client:
            response = await client.post(
                target_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                },
            )

        if response.status_code == 200:
            data = response.json()
            return JobResult(
                success=data.get("status") == "ok",
                status=data.get("status", "unknown"),
                http_status=response.status_code,
                response_summary=data.get("summary", ""),
                duration_ms=data.get("duration_ms", 0),
                cost_usd=data.get("cost_usd"),
                skills_used=data.get("skills_used", []),
                error=data.get("error"),
            )
        else:
            return JobResult(
                success=False,
                status="error",
                http_status=response.status_code,
                error=f"HTTP {response.status_code}: {response.text[:200]}",
            )

    except httpx.TimeoutException:
        logger.error(f"Job '{job_name}' timed out after {timeout_seconds}s")
        return JobResult(
            success=False,
            status="error",
            error=f"HTTP timeout after {timeout_seconds}s",
        )

    except httpx.ConnectError as e:
        logger.error(f"Job '{job_name}' connection failed: {e}")
        return JobResult(
            success=False,
            status="error",
            error=f"Connection failed: {e}",
        )

    except Exception as e:
        logger.error(f"Job '{job_name}' unexpected error: {e}", exc_info=True)
        return JobResult(
            success=False,
            status="error",
            error=f"Unexpected error: {e}",
        )
