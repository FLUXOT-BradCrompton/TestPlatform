# ============================================================
# Stage 1: Build — install dependencies into a virtual env
# ============================================================
FROM python:3.11-slim AS builder

# Install system build deps (needed for psycopg2, cryptography, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only requirements first for optimal layer caching
COPY requirements.txt .

# Create a virtual environment and install all dependencies into it
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --upgrade pip wheel && \
    pip install --no-cache-dir -r requirements.txt

# ============================================================
# Stage 2: Runtime image
# ============================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="Flux OT Platform API" \
      org.opencontainers.image.description="Industrial IoT Integration Platform for Mining and Oil & Gas" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="Flux OT" \
      org.opencontainers.image.licenses="Proprietary"

# Install minimal runtime system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create a non-root user and group
RUN groupadd --gid 1001 fluxot && \
    useradd --uid 1001 --gid fluxot --shell /bin/bash --create-home fluxot

WORKDIR /app

# Copy application source
COPY . /app/platform/api/
COPY ../../ /app/

# Ensure correct ownership
RUN chown -R fluxot:fluxot /app

# Drop privileges
USER fluxot

# Expose the API port
EXPOSE 8000

# Healthcheck — calls the liveness probe
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Use exec form to ensure signals propagate correctly to uvicorn
# API_WORKERS is set from PlatformConfig (default 4); override via env var.
CMD ["sh", "-c", \
     "uvicorn platform.api.main:app \
       --host ${API_HOST:-0.0.0.0} \
       --port ${API_PORT:-8000} \
       --workers ${API_WORKERS:-4} \
       --log-level ${LOG_LEVEL:-info} \
       --no-access-log \
       --proxy-headers \
       --forwarded-allow-ips='*'"]
