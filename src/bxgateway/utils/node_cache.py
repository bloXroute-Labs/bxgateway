import json
import os
from dataclasses import dataclass
from typing import List, Optional
from argparse import Namespace

from bxutils import logging

from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.utils import model_loader, config
from bxcommon.utils.class_json_encoder import ClassJsonEncoder

logger = logging.get_logger(__name__)


@dataclass
class CacheNetworkInfo:
    source_version: str
    relay_peers: List[OutboundPeerModel]
    blockchain_network: List[BlockchainNetworkModel]


def update(opts: Namespace, potential_relay_peers: List[OutboundPeerModel]):
    try:
        cookie_file_path = config.get_data_file(opts.cookie_file_path)
        os.makedirs(os.path.dirname(cookie_file_path), exist_ok=True)
        with open(cookie_file_path, "r") as cookie_file:
            data = json.load(cookie_file)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        data = {}

    # if gateway was upgraded, its version number has changed and its relays are no longer relevant
    if "source_version" in data and data["source_version"] != opts.source_version:
        data = {}

    cache_network_info = CacheNetworkInfo(
        source_version=opts.source_version,
        relay_peers=[relay for relay in potential_relay_peers],
        blockchain_network=[blockchain for blockchain in opts.blockchain_networks
                            if opts.blockchain_network_num == blockchain.network_num]
    )
    cache_info = {
        "source_version": opts.source_version,
        "relay_peers": [relay.__dict__ for relay in cache_network_info.relay_peers],
        "blockchain_network": [blockchain_network.__dict__ for blockchain_network in
                               cache_network_info.blockchain_network]
    }
    data.update(cache_info)
    try:
        with open(config.get_data_file(opts.cookie_file_path), "w") as cookie_file:
            json.dump(data, cookie_file, indent=4, cls=ClassJsonEncoder)
    except Exception as ex:
        logger.error(f"Failed when tried to write to cache file: {opts.cookie_file_path} with exception: {ex}")


def read(opts: Namespace) -> Optional[CacheNetworkInfo]:
    cache_file_info = None
    if not opts.enable_node_cache:
        return cache_file_info

    try:
        relative_path = config.get_data_file(opts.cookie_file_path)
        if os.path.exists(relative_path):
            with open(relative_path, "r") as cookie_file:
                cache_file_info = model_loader.load_model(CacheNetworkInfo, json.load(cookie_file))
    except Exception as ex:
        logger.error(f"Failed when tried to read from cache file: {opts.cookie_file_path} with exception: {ex}")
    finally:
        return cache_file_info
