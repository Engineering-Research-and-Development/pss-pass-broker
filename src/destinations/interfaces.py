from abc import ABC, abstractmethod
from src.core.message import Message


class IDestination(ABC):
    """Defines a contract for any message destination (Pulsar, Kafka...)."""

    @abstractmethod
    def connect(self) -> bool:
        """Establishes the connection to the endpoint."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stops the component and cleans up resources."""
        raise NotImplementedError

    @abstractmethod
    def publish(self, message: Message, destination_topic: str) -> None:
        """Publishes a standardized Message object to a specific destination topic."""
        raise NotImplementedError
