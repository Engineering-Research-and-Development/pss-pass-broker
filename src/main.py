import sys
import os
import yaml
import argparse
from loguru import logger

from src.orchestrator import Orchestrator
from src.sources.mqttsource import MqttSource
from src.sources.opcuasource import OpcUaSource
from src.destinations.pulsar import PulsarDestination


def create_parser():
    parser = argparse.ArgumentParser(
        description="Bridge-MQTT-Pulsar",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--config",
        type=str,
        default="./config.yaml",
        metavar="FILE",
        help="YAML config file",
    )

    return parser


def load_config(path: str):
    config_path = os.path.join(os.getcwd(), path)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            logger.info("Config loaded successfully.")
            return config
    except FileNotFoundError:
        logger.critical(f"Config file not found in '{config_path}'")
        exit(1)
    except yaml.YAMLError as e:
        logger.critical(f"Syntax error in YAML file '{config_path}': {e}")
        exit(1)


def main():
    parser = create_parser()
    args = parser.parse_args()
    config = load_config(args.config)

    log_level = config.get("logging", {}).get("level", "INFO").upper()
    logger.remove()
    logger.add(sys.stderr, level=log_level, colorize=True)
    logger.info(f"Logger level set to: {log_level}")

    source_factories = {"mqtt": MqttSource, "opcua": OpcUaSource}
    dest_factories = {"pulsar": PulsarDestination}

    sources = {}
    for name, source_config in config.get("sources", {}).items():
        if source_config.get("enabled"):
            if name in source_factories:
                source_config["id"] = name
                sources[name] = source_factories[name](source_config)
                logger.debug(f"Source '{name}' created.")
            else:
                logger.warning(f"Unknown source type '{name}' in config.")

    destinations = {}
    for name, dest_config in config.get("destinations", {}).items():
        if dest_config.get("enabled"):
            if name in dest_factories:
                destinations[name] = dest_factories[name](dest_config)
                logger.debug(f"Destination '{name}' created.")
            else:
                logger.warning(f"Unknown destination type '{name}' in config.")

    pipelines = {}
    pipeline_configs = config.get("pipelines", [])
    if not pipeline_configs:
        logger.warning("No pipelines are defined in the configuration. Exiting.")
        return

    for p_config in pipeline_configs:
        source_id = p_config.get("source")
        dest_id = p_config.get("destination")

        source_instance = sources.get(source_id)
        dest_instance = destinations.get(dest_id)

        if source_instance and dest_instance:
            pipelines[source_id] = dest_instance
            logger.success(f"Pipeline configured: {source_id} -> {dest_id}")
        else:
            logger.error(
                f"Skipping pipeline '{p_config.get('name', 'unnamed')}': "
                f"Source '{source_id}' or Destination '{dest_id}' is not configured or enabled."
            )

    if not pipelines:
        logger.critical("No valid pipelines could be constructed. Exiting.")
        return

    unique_destinations = list(set(pipelines.values()))

    bridge = Orchestrator(sources, pipelines, unique_destinations)
    bridge.run()


if __name__ == "__main__":
    main()
