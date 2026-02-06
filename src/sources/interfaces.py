from abc import ABC
from src.core.process import IProcess


class ISource(IProcess, ABC):
    """
    Defines the contract for any message source (e.g., MQTT, OPC UA)
    that the bridge can connect to. Each source wil be run in
    its own independent process.
    """

    pass
