from abc import ABC, abstractmethod
from src.core.message import Message


class IConnectable(ABC):
    """Defines a contract for components that have a connect/stop lifecycle."""

    @abstractmethod
    def connect(self) -> bool:
        """Establishes the connection to the endpoint."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stops the component and cleans up resources."""
        raise NotImplementedError


class IDestination(IConnectable):
    """Defines a contract for any message destination (Pulsar, Kafka...)."""

    @abstractmethod
    def publish(self, message: Message, destination_topic: str) -> None:
        """Publishes a standardized Message object to a specific destination topic."""
        raise NotImplementedError
