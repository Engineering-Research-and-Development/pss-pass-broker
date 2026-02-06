import asyncio
import time
from multiprocessing import Queue
from multiprocessing.synchronize import Event
from loguru import logger
from typing import Any
from asyncua import Node, ua
from asyncua.common.subscription import DataChangeNotif

from src.core.message import Message
from src.core.sourceconnection import OpcuaSourceConnection
from src.sources.interfaces import ISource


class OpcUaSource(ISource):
    """
    Message source for collecting data from an OPC UA server by subscribing to node changes.
    """

    def __init__(self, config: dict):
        self.config = config
        self._message_queue: Queue | None = None
        self._connector = OpcuaSourceConnection(config)
        self._internal_shutdown_event = asyncio.Event()

    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """The entry point for the dedicated process."""
        if not self.config.get("nodes_to_subscribe"):
            logger.warning("OPC-UA: No nodes configured. Process will not start.")
            return

        self._message_queue = message_queue
        logger.info("OPC-UA source process started.")
        while not stop_event.is_set():
            try:
                self._internal_shutdown_event.clear()
                asyncio.run(self._main_async_task(stop_event))
            except ConnectionError as e:
                logger.warning(
                    f"OPC-UA connection lost: {e}. Reconnecting in 5 seconds..."
                )
            except KeyboardInterrupt:
                logger.info("OPC-UA: Process interrupted by user.")
                break
            except Exception:
                logger.exception(
                    "OPC-UA: An unhandled exception in the main loop. Restarting..."
                )

            if not stop_event.is_set():
                time.sleep(5)  # Wait before attempting to reconnect

    async def _main_async_task(self, stop_event: Event):
        """The main async method that orchestrates the client's lifecycle."""
        await self._connector.connect_and_run(
            self, stop_event, self._internal_shutdown_event
        )

    # --- Callbacks ---

    def datachange_notification(self, node: Node, val: Any, data: DataChangeNotif):
        """Callback to handle data change and put message on the queue."""
        if not self._message_queue:
            return
        try:
            data_value = data.monitored_item.Value
            standardized_message = Message(
                source_id=self.config["id"],
                topic=node.nodeid.to_string(),
                payload=str(val).encode("utf-8"),
                timestamp=data_value.ServerTimestamp,
                metadata={"quality": data_value.StatusCode.name},
            )
            self._message_queue.put(standardized_message)
        except Exception:
            node_id_str = node.nodeid.to_string() if node else "Unknown"
            logger.exception(
                f"Error processing OPC-UA data change notification for node {node_id_str}."
            )

    def status_change_notification(self, status: ua.StatusChangeNotification):
        """
        Callback to handle asyncua subscription when its status changes.
        """
        status_code = status.Status
        logger.debug(f"OPC-UA subscription status changed to: {status_code}")

        if status_code.is_bad():
            logger.error(
                f"OPC-UA subscription reported a bad status: {status_code}. "
                "Signaling for a full reconnect."
            )
            self._internal_shutdown_event.set()
