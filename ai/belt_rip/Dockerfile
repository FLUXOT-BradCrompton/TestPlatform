# ===========================================================================
# Flux OT — Belt Rip AI Inference Service
# Multi-stage Dockerfile
#
# Stage 1 (builder): installs Python dependencies including PyTorch CPU.
# Stage 2 (runtime): non-root, minimal image, copies app and virtualenv.
#
# GPU support: uncomment CUDA base image lines and remove CPU torch index.
# ===========================================================================

# ---------------------------------------------------------------------------
# Stage 1: dependency builder
# ---------------------------------------------------------------------------
# CPU-only default
FROM python:3.11-slim AS builder

# For GPU support, replace the FROM above with:
# FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04 AS builder
# RUN apt-get update && apt-get install -y python3.11 python3.11-venv python3-pip \
#     && ln -s /usr/bin/python3.11 /usr/bin/python

WORKDIR /build

# System build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libgomp1 \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create isolated virtualenv
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip + wheel first for faster installs
RUN pip install --no-cache-dir --upgrade pip wheel setuptools

# Install PyTorch CPU build first (separate index URL to avoid pulling CUDA variant)
RUN pip install --no-cache-dir \
    torch==2.3.0 \
    --index-url https://download.pytorch.org/whl/cpu

# For GPU support, replace the RUN above with:
# RUN pip install --no-cache-dir torch==2.3.0

# Install remaining requirements
COPY ai/belt_rip/requirements.txt /build/requirements.txt
RUN pip install --no-cache-dir -r /build/requirements.txt

# ---------------------------------------------------------------------------
# Stage 2: runtime image
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

LABEL maintainer="Flux OT Platform Team" \
      description="Belt Rip AI Inference Service — Flux OT Industrial IoT" \
      version="1.0.0"

# Runtime system deps (libgomp for scikit-learn, libpq for psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN groupadd --gid 1001 fluxot && \
    useradd --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin fluxot

# Copy virtualenv from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Application directory
WORKDIR /app

# Copy application source
# ai/common — shared utilities
COPY ai/__init__.py              /app/ai/__init__.py
COPY ai/common/                  /app/ai/common/
COPY ai/belt_rip/                /app/ai/belt_rip/

# Model storage mount point (populated via PVC at runtime)
RUN mkdir -p /models/belt_rip && chown -R fluxot:fluxot /models /app

# Switch to non-root user
USER fluxot

# Expose FastAPI port
EXPOSE 8080

# Environment defaults (overridden by OpenShift ConfigMap / Secret)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    MLFLOW_TRACKING_URI=http://mlflow:5000 \
    MODEL_NAME=belt_rip_detector \
    MODEL_STAGE=Production \
    LOCAL_MODEL_PATH=/models/belt_rip \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    LOG_LEVEL=INFO

# Health check using the /health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Startup: attempt to pre-download/verify model, then start uvicorn
# The model download is non-fatal — the service starts and serves 503
# on /predict until the model is loaded.
CMD ["uvicorn", "ai.belt_rip.inference_service:app", \
     "--host", "0.0.0.0", \
     "--port", "8080", \
     "--workers", "1", \
     "--log-level", "info", \
     "--timeout-keep-alive", "30"]
