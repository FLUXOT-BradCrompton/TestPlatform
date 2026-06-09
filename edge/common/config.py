"""
Edge layer configuration using Pydantic Settings v2.
All settings can be overridden via environment variables.
"""

from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class EdgeConfig(BaseSettings):
    """
    Central configuration for any edge skid service.
    Values are read from environment variables (case-insensitive).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Identity
    SITE_ID: str = Field(default="SITE_001", description="Site identifier")
    SKID_ID: str = Field(default="SKID_001", description="Skid identifier")
    SKID_TYPE: str = Field(default="GENERIC", description="Skid type (e.g. BELT_RIP, ROAD_CROSSING)")

    # MQTT Broker
    MQTT_BROKER_HOST: str = Field(default="localhost", description="MQTT broker hostname or IP")
    MQTT_BROKER_PORT: int = Field(default=1883, description="MQTT broker port")
    MQTT_USE_TLS: bool = Field(default=False, description="Enable TLS for MQTT connection")
    MQTT_CA_CERT: Optional[str] = Field(default=None, description="Path to CA certificate file")
    MQTT_CLIENT_CERT: Optional[str] = Field(default=None, description="Path to client certificate file")
    MQTT_CLIENT_KEY: Optional[str] = Field(default=None, description="Path to client private key file")
    MQTT_USERNAME: Optional[str] = Field(default=None, description="MQTT username")
    MQTT_PASSWORD: Optional[str] = Field(default=None, description="MQTT password")
    MQTT_QOS: int = Field(default=1, description="MQTT QoS level (0/1/2)")

    # OPC-UA
    OPCUA_ENDPOINT: str = Field(default="opc.tcp://localhost:4840", description="OPC-UA server endpoint URL")
    OPCUA_USERNAME: Optional[str] = Field(default=None, description="OPC-UA username")
    OPCUA_PASSWORD: Optional[str] = Field(default=None, description="OPC-UA password")
    OPCUA_SECURITY_POLICY: str = Field(
        default="None",
        description="OPC-UA security policy: None, Basic256Sha256, Aes128Sha256RsaOaep",
    )
    OPCUA_SECURITY_MODE: str = Field(
        default="None",
        description="OPC-UA security mode: None, Sign, SignAndEncrypt",
    )
    OPCUA_CERT_PATH: Optional[str] = Field(default=None, description="Path to OPC-UA client certificate")
    OPCUA_KEY_PATH: Optional[str] = Field(default=None, description="Path to OPC-UA client private key")

    # Modbus
    MODBUS_HOST: Optional[str] = Field(default=None, description="Modbus TCP host (None disables Modbus)")
    MODBUS_PORT: int = Field(default=502, description="Modbus TCP port")
    MODBUS_UNIT_ID: int = Field(default=1, description="Modbus unit/slave ID")
    MODBUS_TIMEOUT: float = Field(default=3.0, description="Modbus request timeout in seconds")

    # Polling and buffering
    POLL_INTERVAL_MS: int = Field(default=500, description="Sensor poll interval in milliseconds")
    BUFFER_SIZE: int = Field(default=1000, description="Outbound message buffer size (msgs)")

    # Logging
    LOG_LEVEL: str = Field(default="INFO", description="Logging level: DEBUG/INFO/WARNING/ERROR/CRITICAL")

    # Heartbeat
    HEARTBEAT_INTERVAL_S: int = Field(default=30, description="Heartbeat publish interval in seconds")
