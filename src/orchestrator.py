import re
import queue
import multiprocessing as mp
from multiprocessing import Queue
from multiprocessing.synchronize import Event
from loguru import logger

from src.destinations.interfaces import IDestination
from src.sources.interfaces import ISource
from src.core.message import Message


def source_process_worker(source: ISource, message_queue: mp.Queue, stop_event: Event):
    """
    Entry point for each source process.
    It calls the source's run method and handles graceful shutdown.
    """
    try:
        source.run(message_queue, stop_event)
    except KeyboardInterrupt:
        logger.debug(f"Process for {source.__class__.__name__} received interrupt.")
    except Exception:
        logger.exception(
            f"Unhandled exception in process for {source.__class__.__name__}"
        )
    return


class Orchestrator:
    def __init__(
        self,
        sources: dict[str, ISource],
        pipelines: dict[str, IDestination],
        destinations: list[IDestination],
    ):
        self._sources = sources
        self._pipelines = pipelines
        self._destinations = destinations

        self._message_queue: Queue = mp.Queue()
        self._stop_event: Event = mp.Event()
        self._processes: list[mp.Process] = []

        self._invalid_chars_re = re.compile(r"[^a-zA-Z0-9_-]")

    def run(self):
        logger.info("Starting the Bridge...")

        for dest in self._destinations:
            if not dest.connect():
                logger.critical(
                    f"Critical error connecting {dest.__class__.__name__}. Exiting."
                )
                return

        for source_id, source_instance in self._sources.items():
            if source_id not in self._pipelines:
                logger.warning(
                    f"Source '{source_id}' has no pipeline configured. Skipping."
                )
                continue

            process = mp.Process(
                target=source_process_worker,
                args=(source_instance, self._message_queue, self._stop_event),
            )
            self._processes.append(process)
            process.start()
            logger.info(f"Started process for source: {source_id}")

        logger.success("Bridge is running. All source processes started.")

        try:
            self._message_loop()
        except KeyboardInterrupt:
            logger.info("Keyboard interruption detected. Shutting down...")
        finally:
            self.stop()

    def _message_loop(self):
        """Continuously fetches messages from the queue and forwards them."""
        while not self._stop_event.is_set():
            try:
                message: Message = self._message_queue.get()
                self._forward_message(message)
            except queue.Empty:
                continue
            except Exception:
                logger.exception("An error occurred in the main message loop.")

    def _forward_message(self, message: Message):
        """Forwards a single message to its configured destination."""
        destination = self._pipelines.get(message.source_id)
        try:
            destination_topic = self._determine_destination_topic(message)
            logger.info(
                f"Forwarding message from '{message.source_id}' to Pulsar topic '{destination_topic}'"
            )
            destination.publish(message, destination_topic)
        except Exception:
            logger.exception(
                f"An unhandled error occurred while forwarding a message to {destination.__class__.__name__}."
            )

    def _determine_destination_topic(self, message: Message):
        """Returns the corrected destination topic"""
        # eg: persistent://public/default/test-data
        normalized_topic = self._invalid_chars_re.sub("-", message.topic)
        return f"persistent://public/default/{normalized_topic}"

    def stop(self):
        logger.info("Shutting down the bridge...")
        self._stop_event.set()

        for process in self._processes:
            if process.is_alive():
                logger.debug(f"Terminating process {process.pid}...")
                process.terminate()
                process.join(timeout=5)

        for dest in self._destinations:
            dest.stop()

        self._message_queue.close()
        self._message_queue.join_thread()

        logger.success("Bridge shut down successfully.")
