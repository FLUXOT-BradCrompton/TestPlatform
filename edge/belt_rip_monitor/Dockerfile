# =============================================================================
# Flux OT – Belt Rip Monitor Edge Service
# Multi-stage Python 3.11 Dockerfile
# =============================================================================

# ---- builder stage ----------------------------------------------------------
FROM python:3.11-slim AS builder

LABEL maintainer="Flux OT Platform Team <platform@fluxot.io>"
LABEL version="1.0.0"
LABEL skid-type="BELT_RIP"

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libssl-dev \
        libffi-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only the requirements first for better layer caching
COPY edge/belt_rip_monitor/requirements.txt /build/requirements.txt

# Install Python dependencies into a prefix we can copy wholesale
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- runtime stage ----------------------------------------------------------
FROM python:3.11-slim AS runtime

LABEL maintainer="Flux OT Platform Team <platform@fluxot.io>"
LABEL version="1.0.0"
LABEL skid-type="BELT_RIP"

# Runtime system deps (OpenSSL for TLS)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1001 fluxot \
    && useradd --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin fluxot

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY edge/ /app/edge/

# Ensure the edge package is on the Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Prometheus metrics port
EXPOSE 9090

# Switch to non-root user
USER fluxot

# Health check: verify the MQTT publisher is reachable via the metrics endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:9090/metrics || exit 1

# Default command
CMD ["python", "-m", "edge.belt_rip_monitor.service"]
