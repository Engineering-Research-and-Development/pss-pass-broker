from abc import ABC, abstractmethod
from multiprocessing import Queue
from multiprocessing.synchronize import Event


class IProcess(ABC):
    """
    Defines a component that can be run within a dedicated process.
    """

    @abstractmethod
    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """
        This method contains the component's primary process logic loop.

        Args:
            message_queue: A multiprocessing.Queue to send messages back to the
                           main process.
            stop_event: A multiprocessing.Event to signal when the process
                        should terminate gracefully.
        """
        raise NotImplementedError
