# =============================================================================
# Flux OT – Road Crossing Control Edge Service
# Multi-stage Python 3.11 Dockerfile
# =============================================================================

# ---- builder stage ----------------------------------------------------------
FROM python:3.11-slim AS builder

LABEL maintainer="Flux OT Platform Team <platform@fluxot.io>"
LABEL version="1.0.0"
LABEL skid-type="ROAD_CROSSING"

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libssl-dev \
        libffi-dev \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY edge/road_crossing/requirements.txt /build/requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- runtime stage ----------------------------------------------------------
FROM python:3.11-slim AS runtime

LABEL maintainer="Flux OT Platform Team <platform@fluxot.io>"
LABEL version="1.0.0"
LABEL skid-type="ROAD_CROSSING"

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user
RUN groupadd --gid 1001 fluxot \
    && useradd --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin fluxot

WORKDIR /app

COPY --from=builder /install /usr/local
COPY edge/ /app/edge/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 9090

USER fluxot

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:9090/metrics || exit 1

CMD ["python", "-m", "edge.road_crossing.service"]
