import asyncio
import socket
import paho.mqtt.client as mqtt

from abc import ABC, abstractmethod
from asyncua import Client, ua
from asyncua.common.subscription import (
    DataChangeNotificationHandler,
    StatusChangeNotificationHandler,
)
from loguru import logger
from multiprocessing.synchronize import Event
from src.core.heartbeat import MqttHeartbeat, OpcuaHeartbeat


class ISourceConnection(ABC):
    """
    Manages the connection lifecycle of a data source.
    """

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def connect(self) -> mqtt.Client | None:
        """Establishes the connection to the endpoint."""
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> None:
        """Stops the component and cleans up resources."""
        raise NotImplementedError


class MqttSourceConnection(ISourceConnection):
    """Handles the connection logic for an MQTT broker."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.config["client_id"]
        )
        self.heartbeat = MqttHeartbeat(self.client)
        self.heartbeat_interval = self.config.get("heartbeat", {}).get(
            "interval_seconds", 30
        )

    def connect(self) -> mqtt.Client | None:
        try:
            logger.info(f"Attempting to connect to {self.config['broker_host']}...")
            self.client.connect(
                self.config["broker_host"],
                self.config["broker_port"],
                self.config["keepalive"],
            )
            self.client.subscribe(self.config["topic_subscribe"])
            self.client.loop_start()

            logger.success("MQTT: Successfully connected!")
            self.heartbeat.start(self.heartbeat_interval)

            return self.client
        except (socket.gaierror, ConnectionRefusedError, TimeoutError) as e:
            logger.critical(
                f"MQTT connection failed: Could not reach broker at {self.config['broker_host']}:{self.config['broker_port']}. "
                f"Error: {e}. Check configuration or broker status."
            )
            return None
        except Exception:
            logger.exception(
                "An unexpected, non-connection error occurred during MQTT setup"
            )
            return None

    def disconnect(self) -> None:
        logger.info("MQTT: Disconnecting from broker...")
        self.heartbeat.stop()
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            logger.warning(f"MQTT: Exception during disconnection: {e}")


class OpcuaSourceConnection:
    """Handles the async connection, subscription and health logic for an OPC UA server."""

    def __init__(self, config: dict):
        self.config = config
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config.get("nodes_to_subscribe", [])
        self.publishing_interval = config.get("publishing_interval_ms", 500)
        self.heartbeat_interval = self.config.get("heartbeat", {}).get(
            "interval_seconds", 30
        )

    async def connect_and_run(
        self,
        data_change_handler: DataChangeNotificationHandler
        | StatusChangeNotificationHandler,
        stop_event: Event,
        external_shutdown_event: asyncio.Event,
    ):
        """
        The main async method that orchestrates the client's lifecycle.
        It connects, creates a subscription, and keeps running until stopped.
        """
        heartbeat: OpcuaHeartbeat | None = None
        heartbeat_shutdown_event = asyncio.Event()
        client = Client(url=self.server_url)

        try:
            async with client:
                logger.success(f"OPC-UA: Successfully connected to {self.server_url}.")

                loop = asyncio.get_running_loop()
                heartbeat = OpcuaHeartbeat(client, loop, heartbeat_shutdown_event)
                heartbeat.start(self.heartbeat_interval)

                subscription = await client.create_subscription(
                    self.publishing_interval, data_change_handler
                )
                nodes = [
                    client.get_node(node_id) for node_id in self.nodes_to_subscribe
                ]
                await subscription.subscribe_data_change(nodes)
                logger.success(
                    f"OPC-UA: Subscription successful for {len(nodes)} nodes."
                )

                logger.info("OPC-UA: Listening for data changes...")
                while (
                    not stop_event.is_set()
                    and not heartbeat_shutdown_event.is_set()
                    and not external_shutdown_event.is_set()
                ):
                    await asyncio.sleep(1)

                if heartbeat_shutdown_event.is_set():
                    logger.warning("OPC-UA: Shutdown triggered by heartbeat failure.")
                    raise ConnectionError("Connection lost due to heartbeat failure.")

                if external_shutdown_event.is_set():
                    logger.warning(
                        "OPC-UA: Shutdown triggered by subscription status failure."
                    )
                    raise ConnectionError("Subscription became invalid.")

        except (OSError, asyncio.TimeoutError, ConnectionError) as e:
            logger.critical(f"OPC-UA: Connection failed or lost. Error: {e}")
            raise ConnectionError(f"OPC-UA connection failed: {e}") from e
        except ua.UaError as e:
            logger.error(f"OPC-UA: Subscription failed. Error: {e}. Check Node IDs.")
            raise ConnectionError(f"OPC-UA subscription failed: {e}") from e
        except Exception as e:
            logger.exception("OPC-UA: A critical error occurred.")
            raise ConnectionError(f"An unexpected OPC-UA error occurred: {e}") from e
        finally:
            if heartbeat:
                heartbeat.stop()
            logger.warning("OPC-UA: Client is disconnecting.")
