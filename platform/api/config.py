from __future__ import annotations

from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlatformConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://fluxot:fluxot@localhost:5432/fluxot",
        description="Async PostgreSQL connection URL",
    )

    # Redis
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for pub/sub and caching",
    )

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    KAFKA_SECURITY_PROTOCOL: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol: PLAINTEXT or SASL_SSL",
    )
    KAFKA_SASL_MECHANISM: str = Field(
        default="SCRAM-SHA-512",
        description="SASL mechanism when using SASL_SSL",
    )
    KAFKA_SASL_USERNAME: Optional[str] = Field(
        default=None,
        description="Kafka SASL username",
    )
    KAFKA_SASL_PASSWORD: Optional[str] = Field(
        default=None,
        description="Kafka SASL password",
    )
    KAFKA_CA_CERT: Optional[str] = Field(
        default=None,
        description="Path to Kafka CA certificate file for TLS verification",
    )

    # MQTT
    MQTT_BROKER_HOST: str = Field(
        default="localhost",
        description="MQTT broker hostname",
    )
    MQTT_BROKER_PORT: int = Field(
        default=1883,
        description="MQTT broker port",
    )
    MQTT_USE_TLS: bool = Field(
        default=False,
        description="Enable TLS for MQTT connection",
    )
    MQTT_USERNAME: Optional[str] = Field(
        default=None,
        description="MQTT authentication username",
    )
    MQTT_PASSWORD: Optional[str] = Field(
        default=None,
        description="MQTT authentication password",
    )

    # API server
    API_HOST: str = Field(default="0.0.0.0", description="API server bind host")
    API_PORT: int = Field(default=8000, description="API server bind port")
    API_WORKERS: int = Field(default=4, description="Number of uvicorn worker processes")

    # Security / JWT
    SECRET_KEY: str = Field(
        default="changeme-in-production",
        description="HMAC secret key — must be changed in production",
    )
    JWT_ALGORITHM: str = Field(default="HS256", description="JWT signing algorithm")
    JWT_EXPIRE_MINUTES: int = Field(
        default=60, description="JWT access token lifetime in minutes"
    )

    # CORS
    CORS_ORIGINS: List[str] = Field(
        default=["*"],
        description="List of allowed CORS origins",
    )

    # AI service
    AI_SERVICE_URL: str = Field(
        default="http://ai-service:8080",
        description="Internal URL of the AI inference service",
    )

    # Internal API key used by Kafka consumer / internal ingestion endpoint
    INTERNAL_API_KEY: str = Field(
        default="changeme-internal-key",
        description="Shared secret for internal service-to-service calls",
    )

    # Observability
    LOG_LEVEL: str = Field(default="INFO", description="Python logging level")
    ENVIRONMENT: str = Field(
        default="production",
        description="Deployment environment: development / staging / production",
    )


# Module-level singleton — import this in other modules
settings = PlatformConfig()
