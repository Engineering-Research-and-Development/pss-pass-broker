import threading
import paho.mqtt.client as mqtt
import asyncio
from asyncua import Client
from abc import ABC, abstractmethod
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential


class Heartbeat(ABC):
    """
    Provides heartbeat functionality for each individual module.
    This class manages the timer and retry logic, while subclasses
    must provide the specific health check and reconnection logic.
    """

    def __init__(self):
        self._heartbeat_timer: threading.Timer | None = None
        self._stop_event = threading.Event()

    @property
    @abstractmethod
    def _is_healthy(self) -> bool:
        """Must return True if the component is healthy."""
        raise NotImplementedError

    @abstractmethod
    def _perform_reconnect(self) -> bool:
        """Must contain the logic to reconnect."""
        raise NotImplementedError

    def start(self, interval_seconds: int):
        """Starts the heartbeat timer."""
        logger.debug(
            f"Starting heartbeat for {self.__class__.__name__} with {interval_seconds}s interval."
        )
        self._stop_event.clear()
        self._schedule_next_check(interval_seconds)

    def stop(self):
        """Stops the heartbeat timer."""
        logger.debug(f"Stopping heartbeat for {self.__class__.__name__}.")
        self._stop_event.set()
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    def _schedule_next_check(self, interval_seconds: int):
        """Schedules the next execution of the heartbeat check."""
        if self._stop_event.is_set():
            return

        self._heartbeat_timer = threading.Timer(
            interval_seconds, self._run_heartbeat_check, [interval_seconds]
        )
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _run_heartbeat_check(self, interval_seconds: int):
        """The core logic executed by the timer."""
        component_name = self.__class__.__name__
        if self._is_healthy:
            logger.debug(f"Heartbeat check PASSED for {component_name}.")
        else:
            logger.warning(
                f"Heartbeat check FAILED for {component_name}. Attempting to reconnect..."
            )

            reconnect_retrier = Retrying(
                stop=stop_after_attempt(5),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                reraise=True,
            )

            try:
                reconnect_retrier(self._perform_reconnect)
                logger.success(
                    f"Successfully triggered reconnect for {component_name} after heartbeat failure."
                )
            except Exception:
                logger.critical(
                    f"Could not reconnect {component_name} after multiple attempts. "
                    f"The component remains disconnected."
                )

        # Schedules next check if not stopped
        self._schedule_next_check(interval_seconds)


class MqttHeartbeat(Heartbeat):
    def __init__(self, mqtt_client: mqtt.Client):
        super().__init__()
        self.client = mqtt_client

    def _is_healthy(self) -> bool:
        return self.client.is_connected()

    def _perform_reconnect(self) -> bool:
        logger.info("MQTT: Attempting to perform reconnection...")
        error_code = self.client.reconnect()
        if error_code != 0:
            logger.error(f"MQTT reconnect failed with code: {error_code}")
            raise ConnectionError(
                f"MQTT reconnect attempt failed with code {error_code}"
            )
        return True


class OpcuaHeartbeat(Heartbeat):
    def __init__(
        self,
        client: Client,
        loop: asyncio.AbstractEventLoop,
        shutdown_event: asyncio.Event,
    ):
        super().__init__()
        self.client = client
        self.loop = loop
        self.shutdown_event = shutdown_event

    def _is_healthy(self) -> bool:
        if not self.client or not self.loop.is_running():
            return False

        try:
            health_check_node = self.client.get_node("i=2256")  # ServerStatus node

            future = asyncio.run_coroutine_threadsafe(
                health_check_node.read_value(), self.loop
            )
            future.result(timeout=5)
            return True
        except Exception as e:
            logger.warning(f"OPC-UA heartbeat health check failed: {e}")
            return False

    def _perform_reconnect(self) -> bool:
        if not self.loop.is_running():
            logger.error("Cannot trigger reconnect: opcua loop is not running.")
            return False

        logger.info("OPC-UA: Signaling main async task to shut down for reconnect.")
        # Schedule the setting of the event on the main loop
        self.loop.call_soon_threadsafe(lambda *args: self.shutdown_event.set(), ())
        return True
