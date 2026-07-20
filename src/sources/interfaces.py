from abc import ABC, abstractmethod
from multiprocessing import Queue
from multiprocessing.synchronize import Event


class ISource(ABC):
    """
    Defines the contract for any message source (e.g., MQTT, OPC UA).
    Each source runs in its own independent process.
    """

    @abstractmethod
    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """
        Primary process loop for the source.

        Args:
            message_queue: A multiprocessing.Queue to send messages back to the
                           main process.
            stop_event: A multiprocessing.Event to signal when the process
                        should terminate gracefully.
        """
        raise NotImplementedError
