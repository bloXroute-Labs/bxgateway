import os
import functools
from json import JSONDecodeError
from collections import defaultdict

from bxutils import logging
from bxutils.logging import log_config
from bxutils.logging import log_level

from bxcommon.models.config.gateway_node_config_model import GatewayNodeConfigModel
from bxcommon.utils import model_loader
from bxcommon.connections.abstract_node import AbstractNode
from bxcommon.utils import config

from bxgateway import gateway_constants

logger = logging.get_logger(__name__)

config_file_last_updated = defaultdict(float)
last_config_model = defaultdict(GatewayNodeConfigModel)


def read_config_file(full_path: str) -> GatewayNodeConfigModel:
    try:
        file_mod_time = os.stat(full_path).st_mtime
    except FileNotFoundError:
        logger.warning("config file not found {}", full_path)
        return GatewayNodeConfigModel()
    if config_file_last_updated.get(full_path) != file_mod_time:
        try:
            with open(full_path, "r") as f:
                raw_input = f.read()
                config_model = model_loader.load_model_from_json(model_class=GatewayNodeConfigModel, model_params=raw_input)
                config_file_last_updated[full_path] = file_mod_time
                last_config_model[full_path] = config_model
        except (TypeError, JSONDecodeError):
            logger.warning("Invalid Configuration payload in file {} \n{}  ", full_path)
            return GatewayNodeConfigModel()
        except Exception as e:
            logger.warning("File Handling Error {} {}", full_path, e)
            return GatewayNodeConfigModel()
        return config_model
    else:
        return last_config_model[full_path]


def compare_and_update(new_value, target, setter, item=None):
    if new_value is not None:
        if new_value != target:
            setter(new_value)
            logger.info("ConfigUpdate Item: {}, New: {} Old: {}", item, repr(new_value), repr(target))
            return


def update_node_config(node: AbstractNode):
    default_node_config = read_config_file(config.get_relative_file(gateway_constants.CONFIG_FILE_NAME))
    override_node_config = read_config_file(config.get_relative_file(gateway_constants.CONFIG_OVERRIDE_NAME))
    node_config = GatewayNodeConfigModel()
    node_config.merge(default_node_config)
    node_config.merge(override_node_config)

    logger.debug({"type": "update_node_config", "data": node_config})
    log_config_model = node_config.log_config
    if log_config_model is not None:
        try:
            if log_config_model.log_level is not None:
                compare_and_update(log_level.from_string(log_config_model.log_level),
                                   logger.level,
                                   setter=functools.partial(log_config.set_level, ["bxcommon", "bxgateway"]),
                                   item="log_level")
        except (AttributeError, KeyError):
            logger.warning("Invalid LogLevel provided configuration Ignore {}", log_config_model.log_level)
            
    if node_config.cron_config is not None:
        compare_and_update(node_config.cron_config.config_update_interval,
                           node.opts.config_update_interval,
                           item="config_update_interval",
                           setter=lambda val: node.opts.__setattr__("config_update_interval", val)
                           )
        compare_and_update(node_config.cron_config.info_stats_interval,
                           node.opts.info_stats_interval,
                           item="info_stats_interval",
                           setter=lambda val: node.opts.__setattr__("info_stats_interval", val))
        compare_and_update(node_config.cron_config.throughput_stats_interval,
                           node.opts.throughput_stats_interval,
                           item="throughput_stats_interval",
                           setter=lambda val: node.opts.__setattr__("throughput_stats_interval", val))
        compare_and_update(node_config.cron_config.memory_stats_interval,
                           node.opts.memory_stats_interval,
                           item="memory_stats_interval",
                           setter=lambda val: node.opts.__setattr__("memory_stats_interval", val))
