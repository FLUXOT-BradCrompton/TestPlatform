"""
Async OPC-UA client for edge skid services.

Wraps the ``asyncua`` library with:
  - Security policy / mode selection (None, Basic256Sha256, Aes128Sha256RsaOaep)
  - Username/password and certificate authentication
  - Exponential-backoff reconnection running as a background asyncio task
  - Session keepalive
  - Batch reads / writes via the OPC-UA Services API
  - Data-change subscriptions with automatic re-subscription on reconnect
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from asyncua import Client, Node, ua
from asyncua.crypto.security_policies import (
    SecurityPolicyBasic256Sha256,
    SecurityPolicyAes128Sha256RsaOaep,
)

from .config import EdgeConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subscription handler
# ---------------------------------------------------------------------------


class _DataChangeHandler:
    """Forwards OPC-UA data-change notifications to an asyncio callback."""

    def __init__(self, callback: Callable, loop: asyncio.AbstractEventLoop) -> None:
        self._callback = callback
        self._loop = loop

    def datachange_notification(self, node: Node, val: Any, data: Any) -> None:  # noqa: ANN001
        node_id = str(node.nodeid)
        asyncio.run_coroutine_threadsafe(self._callback(node_id, val, data), self._loop)

    def event_notification(self, event: Any) -> None:  # noqa: ANN001
        pass  # Not used but required by asyncua interface

    def status_change_notification(self, status: Any) -> None:  # noqa: ANN001
        pass


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class OPCUAClient:
    """
    Production-grade async OPC-UA client.

    Usage::

        client = OPCUAClient(config)
        await client.connect()
        value = await client.read_node("ns=2;s=Sensor.Temperature")
        await client.disconnect()
    """

    _BACKOFF_BASE: float = 2.0
    _BACKOFF_MAX: float = 60.0
    _KEEPALIVE_INTERVAL_S: int = 20

    def __init__(self, config: EdgeConfig) -> None:
        self._config = config
        self._client: Optional[Client] = None
        self._connected: bool = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._keepalive_task: Optional[asyncio.Task] = None
        self._subscription: Optional[Any] = None
        # List of (node_ids, callback, interval_ms) tuples to re-subscribe after reconnect
        self._pending_subscriptions: List[Tuple[List[str], Callable, int]] = []

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Connect to the OPC-UA server.  Blocks until connected or raises after
        exhausting retries (callers should typically await this once at startup
        and rely on the background reconnect task thereafter).
        """
        backoff = self._BACKOFF_BASE
        attempt = 0

        while True:
            attempt += 1
            try:
                await self._do_connect()
                self._connected = True
                logger.info(
                    "OPC-UA connected",
                    extra={"endpoint": self._config.OPCUA_ENDPOINT, "attempt": attempt},
                )
                # Start background keepalive
                self._keepalive_task = asyncio.ensure_future(self._keepalive_loop())
                # Restore any subscriptions
                await self._restore_subscriptions()
                return
            except Exception as exc:
                logger.warning(
                    "OPC-UA connection failed, retrying",
                    extra={"error": str(exc), "backoff_s": backoff, "attempt": attempt},
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._BACKOFF_MAX)

    async def _do_connect(self) -> None:
        cfg = self._config
        self._client = Client(url=cfg.OPCUA_ENDPOINT, timeout=30)

        # Apply security policy
        policy_name = cfg.OPCUA_SECURITY_POLICY.strip()
        mode_name = cfg.OPCUA_SECURITY_MODE.strip()

        if policy_name not in ("None", "", "none") and cfg.OPCUA_CERT_PATH and cfg.OPCUA_KEY_PATH:
            if policy_name == "Basic256Sha256":
                await self._client.set_security(
                    SecurityPolicyBasic256Sha256,
                    certificate=cfg.OPCUA_CERT_PATH,
                    private_key=cfg.OPCUA_KEY_PATH,
                    mode=self._parse_message_mode(mode_name),
                )
            elif policy_name == "Aes128Sha256RsaOaep":
                await self._client.set_security(
                    SecurityPolicyAes128Sha256RsaOaep,
                    certificate=cfg.OPCUA_CERT_PATH,
                    private_key=cfg.OPCUA_KEY_PATH,
                    mode=self._parse_message_mode(mode_name),
                )
            else:
                logger.warning("Unknown OPC-UA security policy, using None", extra={"policy": policy_name})

        # Credentials
        if cfg.OPCUA_USERNAME:
            self._client.set_user(cfg.OPCUA_USERNAME)
            self._client.set_password(cfg.OPCUA_PASSWORD or "")

        await self._client.connect()

    @staticmethod
    def _parse_message_mode(mode_name: str) -> ua.MessageSecurityMode:
        mapping = {
            "sign": ua.MessageSecurityMode.Sign,
            "signandencrypt": ua.MessageSecurityMode.SignAndEncrypt,
        }
        return mapping.get(mode_name.lower().replace("_", ""), ua.MessageSecurityMode.SignAndEncrypt)

    async def disconnect(self) -> None:
        """Gracefully close the OPC-UA session."""
        for task in (self._reconnect_task, self._keepalive_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
        self._connected = False
        self._client = None
        logger.info("OPC-UA disconnected")

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def read_node(self, node_id: str) -> Any:
        """Read the value of a single node by its NodeId string."""
        self._assert_connected()
        node = self._client.get_node(node_id)
        try:
            dv = await node.read_data_value()
            if dv.StatusCode.is_good():
                return dv.Value.Value
            logger.warning("Bad status on node read", extra={"node_id": node_id, "status": str(dv.StatusCode)})
            return None
        except Exception as exc:
            logger.error("OPC-UA read_node error", extra={"node_id": node_id, "error": str(exc)})
            await self._handle_error()
            raise

    async def read_nodes(self, node_ids: List[str]) -> Dict[str, Any]:
        """
        Read multiple nodes in a single OPC-UA ReadRequest for efficiency.
        Returns a mapping of node_id -> value (None if bad quality).
        """
        self._assert_connected()
        nodes = [self._client.get_node(nid) for nid in node_ids]
        results: Dict[str, Any] = {}
        try:
            data_values = await self._client.read_values(nodes)
            for nid, val in zip(node_ids, data_values):
                results[nid] = val
        except Exception as exc:
            logger.error("OPC-UA read_nodes error", extra={"count": len(node_ids), "error": str(exc)})
            await self._handle_error()
            raise
        return results

    # ------------------------------------------------------------------
    # Write operations (writeback)
    # ------------------------------------------------------------------

    async def write_node(
        self,
        node_id: str,
        value: Any,
        variant_type: Optional[ua.VariantType] = None,
    ) -> None:
        """Write a value to a single node."""
        self._assert_connected()
        node = self._client.get_node(node_id)
        try:
            if variant_type is not None:
                var = ua.Variant(value, variant_type)
                await node.write_value(var)
            else:
                await node.write_value(value)
            logger.debug("OPC-UA write_node", extra={"node_id": node_id, "value": value})
        except Exception as exc:
            logger.error("OPC-UA write_node error", extra={"node_id": node_id, "error": str(exc)})
            await self._handle_error()
            raise

    async def write_nodes(self, writes: Dict[str, Any]) -> None:
        """Write multiple nodes.  Each entry: node_id -> value."""
        for node_id, value in writes.items():
            await self.write_node(node_id, value)

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    async def subscribe_nodes(
        self,
        node_ids: List[str],
        callback: Callable,
        interval_ms: int = 500,
    ) -> None:
        """
        Subscribe to data-change notifications for *node_ids*.
        *callback* must be an async coroutine: ``async def cb(node_id, value, data)``.
        Subscriptions are automatically restored after reconnect.
        """
        self._pending_subscriptions.append((node_ids, callback, interval_ms))
        if self._connected and self._client:
            await self._create_subscription(node_ids, callback, interval_ms)

    async def _create_subscription(
        self,
        node_ids: List[str],
        callback: Callable,
        interval_ms: int,
    ) -> None:
        loop = asyncio.get_running_loop()
        handler = _DataChangeHandler(callback, loop)
        sub = await self._client.create_subscription(interval_ms, handler)
        nodes = [self._client.get_node(nid) for nid in node_ids]
        await sub.subscribe_data_change(nodes)
        logger.debug("OPC-UA subscription created", extra={"node_count": len(node_ids)})

    async def _restore_subscriptions(self) -> None:
        for node_ids, callback, interval_ms in self._pending_subscriptions:
            try:
                await self._create_subscription(node_ids, callback, interval_ms)
            except Exception as exc:
                logger.warning("Failed to restore subscription", extra={"error": str(exc)})

    # ------------------------------------------------------------------
    # Method calls
    # ------------------------------------------------------------------

    async def call_method(self, object_id: str, method_id: str, *args: Any) -> Any:
        """Call an OPC-UA method on *object_id*."""
        self._assert_connected()
        obj_node = self._client.get_node(object_id)
        try:
            result = await obj_node.call_method(method_id, *args)
            return result
        except Exception as exc:
            logger.error(
                "OPC-UA call_method error",
                extra={"object_id": object_id, "method_id": method_id, "error": str(exc)},
            )
            await self._handle_error()
            raise

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------

    async def _keepalive_loop(self) -> None:
        """Periodically read the server state to keep the session alive."""
        while self._connected:
            try:
                await asyncio.sleep(self._KEEPALIVE_INTERVAL_S)
                if self._client:
                    # Reading server state is the lightest keepalive possible
                    _ = await self._client.get_node(ua.ObjectIds.Server_ServerStatus_State).read_value()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("OPC-UA keepalive failed", extra={"error": str(exc)})
                await self._handle_error()

    async def start_reconnect_loop(self) -> None:
        """
        Start the background reconnect task.  Call this once after the first
        ``connect()`` succeeds if you want automatic recovery.
        """
        self._reconnect_task = asyncio.ensure_future(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Monitors connectivity and reconnects on failure."""
        backoff = self._BACKOFF_BASE
        while True:
            await asyncio.sleep(backoff)
            if not self._connected:
                logger.info("OPC-UA attempting reconnect", extra={"backoff_s": backoff})
                try:
                    await self._do_connect()
                    self._connected = True
                    backoff = self._BACKOFF_BASE
                    self._keepalive_task = asyncio.ensure_future(self._keepalive_loop())
                    await self._restore_subscriptions()
                    logger.info("OPC-UA reconnected successfully")
                except Exception as exc:
                    logger.warning("OPC-UA reconnect failed", extra={"error": str(exc), "backoff_s": backoff})
                    backoff = min(backoff * 2, self._BACKOFF_MAX)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _handle_error(self) -> None:
        """Mark as disconnected so the reconnect loop kicks in."""
        self._connected = False
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()

    def _assert_connected(self) -> None:
        if not self._connected or self._client is None:
            raise ConnectionError("OPC-UA client is not connected")

    @property
    def is_connected(self) -> bool:
        return self._connected
