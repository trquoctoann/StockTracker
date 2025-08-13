# syntax=docker/dockerfile:1

# Build stage
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY pyproject.toml ./
COPY requirements-dev.txt ./

# Install dependencies
RUN python -m pip install --upgrade pip && \
    pip install --user -e .[dev]

# Production stage
FROM python:3.11-slim AS production

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/root/.local/bin:$PATH"

WORKDIR /app

# Create non-root user
RUN groupadd -r app && useradd -r -g app app

# Copy installed packages from builder
COPY --from=builder /root/.local /root/.local

# Copy source code
COPY src ./src

# Change ownership
RUN chown -R app:app /app

USER app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD collector health || exit 1

ENTRYPOINT ["collector"]
CMD ["health"]

