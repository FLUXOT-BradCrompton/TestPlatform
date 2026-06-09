"""
Async Modbus TCP/RTU adapter for edge skid services.

Uses pymodbus v3 async API with:
  - Exponential-backoff reconnection
  - TCP and RTU (serial) transport modes
  - Coil / discrete-input / holding-register / input-register reads
  - Coil and register write-back operations
  - Helper decoders: int16, uint16, float32 (big-endian IEEE 754)
"""

from __future__ import annotations

import asyncio
import logging
import struct
from typing import List, Optional

from pymodbus.client import AsyncModbusTcpClient, AsyncModbusSerialClient
from pymodbus.exceptions import ModbusException, ConnectionException

from .config import EdgeConfig

logger = logging.getLogger(__name__)


class ModbusAdapter:
    """
    Async Modbus adapter supporting TCP and RTU modes.

    Usage::

        adapter = ModbusAdapter(config, mode="TCP")
        await adapter.connect()
        regs = await adapter.read_holding_registers(0, 10)
        temp = adapter.decode_float32(regs[0:2])
        await adapter.disconnect()
    """

    _BACKOFF_BASE: float = 2.0
    _BACKOFF_MAX: float = 60.0

    def __init__(self, config: EdgeConfig, mode: str = "TCP") -> None:
        self._config = config
        self._mode = mode.upper()
        self._client: Optional[AsyncModbusTcpClient | AsyncModbusSerialClient] = None
        self._connected: bool = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _build_client(self) -> AsyncModbusTcpClient | AsyncModbusSerialClient:
        cfg = self._config
        if self._mode == "TCP":
            if not cfg.MODBUS_HOST:
                raise ValueError("MODBUS_HOST must be set for TCP mode")
            return AsyncModbusTcpClient(
                host=cfg.MODBUS_HOST,
                port=cfg.MODBUS_PORT,
                timeout=cfg.MODBUS_TIMEOUT,
            )
        elif self._mode == "RTU":
            # For RTU the MODBUS_HOST field is repurposed as the serial port path
            port = cfg.MODBUS_HOST or "/dev/ttyUSB0"
            return AsyncModbusSerialClient(
                port=port,
                baudrate=9600,
                bytesize=8,
                parity="N",
                stopbits=1,
                timeout=cfg.MODBUS_TIMEOUT,
            )
        else:
            raise ValueError(f"Unsupported Modbus mode: {self._mode}")

    async def connect(self) -> None:
        """Connect with exponential-backoff retry."""
        backoff = self._BACKOFF_BASE
        attempt = 0

        while True:
            attempt += 1
            try:
                self._client = self._build_client()
                await self._client.connect()
                if self._client.connected:
                    self._connected = True
                    logger.info(
                        "Modbus connected",
                        extra={
                            "host": self._config.MODBUS_HOST,
                            "port": self._config.MODBUS_PORT,
                            "mode": self._mode,
                            "attempt": attempt,
                        },
                    )
                    return
                raise ConnectionException("connect() returned but client not connected")
            except Exception as exc:
                logger.warning(
                    "Modbus connection failed, retrying",
                    extra={"error": str(exc), "backoff_s": backoff, "attempt": attempt},
                )
                if self._client:
                    try:
                        self._client.close()
                    except Exception:
                        pass
                    self._client = None
                self._connected = False
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._BACKOFF_MAX)

    async def disconnect(self) -> None:
        """Close the Modbus connection."""
        if self._client:
            self._client.close()
            self._client = None
        self._connected = False
        logger.info("Modbus disconnected")

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def read_coils(self, address: int, count: int) -> List[bool]:
        """Read *count* coils starting at *address*."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.read_coils(
                    address, count=count, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                return resp.bits[:count]
            except ModbusException as exc:
                logger.error("Modbus read_coils error", extra={"address": address, "error": str(exc)})
                self._connected = False
                raise

    async def read_discrete_inputs(self, address: int, count: int) -> List[bool]:
        """Read *count* discrete inputs starting at *address*."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.read_discrete_inputs(
                    address, count=count, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                return resp.bits[:count]
            except ModbusException as exc:
                logger.error("Modbus read_discrete_inputs error", extra={"address": address, "error": str(exc)})
                self._connected = False
                raise

    async def read_holding_registers(self, address: int, count: int) -> List[int]:
        """Read *count* holding registers starting at *address*."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.read_holding_registers(
                    address, count=count, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                return list(resp.registers)
            except ModbusException as exc:
                logger.error(
                    "Modbus read_holding_registers error",
                    extra={"address": address, "error": str(exc)},
                )
                self._connected = False
                raise

    async def read_input_registers(self, address: int, count: int) -> List[int]:
        """Read *count* input registers starting at *address*."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.read_input_registers(
                    address, count=count, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                return list(resp.registers)
            except ModbusException as exc:
                logger.error(
                    "Modbus read_input_registers error",
                    extra={"address": address, "error": str(exc)},
                )
                self._connected = False
                raise

    # ------------------------------------------------------------------
    # Write operations (writeback)
    # ------------------------------------------------------------------

    async def write_coil(self, address: int, value: bool) -> None:
        """Write a single coil."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.write_coil(
                    address, value, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                logger.debug("Modbus write_coil", extra={"address": address, "value": value})
            except ModbusException as exc:
                logger.error("Modbus write_coil error", extra={"address": address, "error": str(exc)})
                self._connected = False
                raise

    async def write_register(self, address: int, value: int) -> None:
        """Write a single holding register."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.write_register(
                    address, value, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                logger.debug("Modbus write_register", extra={"address": address, "value": value})
            except ModbusException as exc:
                logger.error("Modbus write_register error", extra={"address": address, "error": str(exc)})
                self._connected = False
                raise

    async def write_multiple_registers(self, address: int, values: List[int]) -> None:
        """Write multiple holding registers starting at *address*."""
        async with self._lock:
            self._assert_connected()
            try:
                resp = await self._client.write_registers(
                    address, values, slave=self._config.MODBUS_UNIT_ID
                )
                self._check_response(resp)
                logger.debug(
                    "Modbus write_multiple_registers",
                    extra={"address": address, "count": len(values)},
                )
            except ModbusException as exc:
                logger.error(
                    "Modbus write_multiple_registers error",
                    extra={"address": address, "error": str(exc)},
                )
                self._connected = False
                raise

    # ------------------------------------------------------------------
    # Decoders
    # ------------------------------------------------------------------

    @staticmethod
    def decode_int16(reg: int) -> int:
        """Convert an unsigned 16-bit register value to a signed int16."""
        if reg > 0x7FFF:
            return reg - 0x10000
        return reg

    @staticmethod
    def decode_uint16(reg: int) -> int:
        """Return the raw unsigned 16-bit register value (identity)."""
        return reg & 0xFFFF

    @staticmethod
    def decode_float32(regs: List[int]) -> float:
        """
        Decode two consecutive 16-bit registers as a big-endian IEEE 754
        single-precision float.

        *regs[0]* is the high word, *regs[1]* is the low word.
        """
        if len(regs) < 2:
            raise ValueError("decode_float32 requires exactly 2 register values")
        raw = struct.pack(">HH", regs[0] & 0xFFFF, regs[1] & 0xFFFF)
        (value,) = struct.unpack(">f", raw)
        return float(value)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _assert_connected(self) -> None:
        if not self._connected or self._client is None:
            raise ConnectionError("Modbus client is not connected")

    @staticmethod
    def _check_response(resp: Any) -> None:  # noqa: ANN001
        if resp is None:
            raise ModbusException("Null response from Modbus device")
        if hasattr(resp, "isError") and resp.isError():
            raise ModbusException(f"Modbus error response: {resp}")

    @property
    def is_connected(self) -> bool:
        return self._connected and self._client is not None and self._client.connected
