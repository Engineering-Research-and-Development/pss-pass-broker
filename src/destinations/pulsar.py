import pulsar
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential, RetryError
from src.core.message import Message
from src.destinations.interfaces import IDestination


class PulsarDestination(IDestination):
    """
    Defines the Apache Pulsar cluster destination to which the messages will be published.
    """

    def __init__(self, config: dict):
        self.config = config
        self.client: pulsar.Client | None = None
        self.producers: dict[str, pulsar.Producer] = {}

        publishing_config = self.config.get("publishing", {})
        self.retrier = Retrying(
            stop=stop_after_attempt(publishing_config.get("retry_attempts", 5)),
            wait=wait_exponential(multiplier=1, min=2, max=10),
        )
        self.dlq_topic = publishing_config.get("dlq_topic")
        self.dlq_producer: pulsar.Producer | None = None

    def connect(self) -> bool:
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass

        try:
            self.client = pulsar.Client(self.config["service_url"])
            # If the broker is not available, this call will timeout and raise an exception.
            self.client.get_topic_partitions(
                "persistent://public/default/non-existent-topic-for-health-check"
            )

            if self.dlq_topic:
                self.dlq_producer = self.client.create_producer(self.dlq_topic)
                logger.debug(
                    f"Successfully created DLQ producer for topic: {self.dlq_topic}"
                )

            logger.success(
                f"Successfully connected to Pulsar service at {self.config['service_url']}"
            )

            return True
        except (pulsar.ConnectError, pulsar.Timeout) as e:
            logger.critical(
                f"Pulsar connection failed: Could not reach broker at {self.config['service_url']}. "
                f"Error: {e.__class__.__name__}. Check configuration or broker status."
            )
            if self.client:
                self.client.close()
            return False
        except Exception:
            logger.exception("An unexpected error occurred while connecting to Pulsar")
            if self.client:
                self.client.close()
            return False

    def _create_producer(self, topic: str) -> pulsar.Producer:
        logger.debug(f"Attempting to create producer for Pulsar topic '{topic}'...")
        try:
            producer = self.client.create_producer(topic)
            logger.info(f"Created new Pulsar producer for topic: {topic}")
            return producer
        except Exception as e:
            logger.warning(
                f"Failed attempt to create producer for Pulsar topic '{topic}'. "
                f"Error: {e.__class__.__name__}. Retrying...."
            )
            raise

    def _get_producer(self, topic: str) -> pulsar.Producer | None:
        if topic in self.producers:
            return self.producers[topic]

        if not self.client:
            logger.error("Pulsar client is not connected. Cannot create new producer.")
            return None

        try:
            producer = self.retrier(self._create_producer, topic)
            self.producers[topic] = producer
            return producer
        except RetryError as e:
            logger.critical(
                f"Could not create a producer for topic '{topic}' after max attempts. "
                f"Final error: {e}. This may indicate a persistent Pulsar issue."
            )
            return None

    @staticmethod
    def _send_message(producer: pulsar.Producer, payload: bytes):
        try:
            logger.debug(
                f"Attempting to send message to Pulsar topic '{producer.topic()}'..."
            )
            producer.send(payload)
            logger.success(
                f"Message successfully sent to Pulsar topic '{producer.topic()}'."
            )
        except Exception as e:
            logger.warning(
                f"Failed to send message to Pulsar topic '{producer.topic()}'. "
                f"Error: {e.__class__.__name__}. Retrying..."
            )
            raise

    def _send_to_dlq(self, message: Message, reason: str):
        if not self.dlq_producer:
            logger.error(
                f"DLQ producer is not available. Message from topic '{message.source_id}' will be lost."
            )
            return

        try:
            properties = {
                "original_source": message.source_id,
                "original_topic": message.topic,
                "failure_reason": reason,
                "timestamp_utc": message.timestamp.isoformat(),
            }
            self.dlq_producer.send(message.payload, properties=properties)
            logger.warning(
                f"Message from topic '{message.topic}' successfully sent to DLQ '{self.dlq_topic}'. Reason: {reason}"
            )
        except Exception:
            logger.critical(
                f"CRITICAL: Failed to send message from topic '{message.topic}' to DLQ '{self.dlq_topic}'. DATA LOSS OCCURRED."
            )

    def publish(self, message: Message, destination_topic: str):
        producer = self._get_producer(destination_topic)

        if not producer:
            reason = f"Could not get a producer for topic '{destination_topic}'"
            logger.error(reason)
            self._send_to_dlq(message, reason)
            return

        try:
            self.retrier(self._send_message, producer, message.payload)
        except RetryError as e:
            reason = f"Max retries exceeded for topic '{destination_topic}'. Final error: {e}"
            logger.critical(reason)
            self._send_to_dlq(message, reason)
        except Exception:
            reason = f"An unexpected, non-retryable error occurred during message publishing for topic '{message.topic}'"
            logger.exception(reason)
            self._send_to_dlq(message, reason)

    def stop(self):
        logger.info("Closing all Pulsar producers...")
        logger.debug(f"Closing {len(self.producers)} Pulsar producers.")
        for topic, producer in self.producers.items():
            try:
                producer.close()
            except Exception:
                logger.warning(f"Error closing producer for topic {topic}")

        if self.dlq_producer:
            try:
                self.dlq_producer.close()
            except Exception:
                logger.warning(f"Error closing DLQ producer for topic {self.dlq_topic}")

        if self.client:
            try:
                self.client.close()
                logger.info("Pulsar: Stopped.")
            except Exception as e:
                logger.warning(f"Exception during Pulsar client closing: {e}")
