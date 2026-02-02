FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Install dependencies
RUN uv sync --no-dev

# Copy config (can be overridden via volume mount)
COPY config/ config/

# Create data directory
RUN mkdir -p data/runs

# Environment defaults
ENV HOST=0.0.0.0
ENV PORT=8092
ENV AGENT_API_KEY=""
ENV SCHEDULER_API_KEY=""

EXPOSE 8092

CMD ["uv", "run", "python", "-m", "cron_scheduler.main"]
