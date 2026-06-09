# =============================================================================
# Flux OT – OPC-UA Simulation Server
# =============================================================================

FROM python:3.11-slim

LABEL maintainer="Flux OT Platform Team <platform@fluxot.io>"
LABEL version="1.0.0"
LABEL component="opcua-simulator"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Only asyncua is needed for the simulator
RUN pip install --no-cache-dir asyncua==1.1.5

# Copy application source
COPY edge/ /app/edge/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Non-root user
RUN groupadd --gid 1001 fluxot \
    && useradd --uid 1001 --gid 1001 --no-create-home --shell /sbin/nologin fluxot
USER fluxot

EXPOSE 4840

HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import asyncio; from asyncua import Client; asyncio.run(Client('opc.tcp://localhost:4840').connect())" || exit 1

CMD ["python", "-m", "edge.opcua_simulator.server"]
