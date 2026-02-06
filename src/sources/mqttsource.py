import time
from multiprocessing import Queue
from multiprocessing.synchronize import Event
from loguru import logger

from src.sources.interfaces import ISource
from src.core.message import Message
from src.core.sourceconnection import MqttSourceConnection


class MqttSource(ISource):
    """
    Message source for collecting data from an MQTT broker by subscribing to topics.
    """

    def __init__(self, config: dict):
        self.config = config
        self._message_queue: Queue | None = None
        self._connector = MqttSourceConnection(config)

    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """Main process loop for the MQTT source."""
        self._message_queue = message_queue

        while not stop_event.is_set():
            client = self._connector.connect()

            if not client:
                logger.warning("MQTT connection failed. Retrying in 5 seconds...")
                time.sleep(5)
                continue

            client.on_message = self._internal_on_message
            logger.info("MQTT source is connected and running.")

            while not stop_event.is_set():
                if not client.is_connected():
                    logger.warning("MQTT connection lost. Attempting to reconnect...")
                    break
                time.sleep(1)

            # if here then it means that the connection was lost or stop signal received
            self._connector.disconnect()

        logger.info("MQTT source has stopped.")

    def _internal_on_message(self, client, userdata, msg):
        """Internal callback that works as an adapter and translator between Source and Publisher."""
        if self._message_queue:
            standardized_message = Message(
                source_id=self.config["id"],
                topic=msg.topic,
                payload=msg.payload,
            )
            self._message_queue.put(standardized_message)
