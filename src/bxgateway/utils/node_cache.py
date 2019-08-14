import os
import json
from dataclasses import dataclass
from typing import List, Optional

from bxcommon.utils import model_loader, logger
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel


@dataclass
class CacheNetworkInfo:
    relay_peers: List[OutboundPeerModel]
    blockchain_network: List[BlockchainNetworkModel]


def update(opts, potential_relay_peers: List[OutboundPeerModel]):
    try:
        with open(opts.cookie_file_path, "r") as cookie_file:
            data = json.load(cookie_file)
    except FileNotFoundError:
            data = {}
    cache_network_info = CacheNetworkInfo(
        relay_peers=[relay for relay in potential_relay_peers],
        blockchain_network=[blockchain for blockchain in opts.blockchain_networks
                            if opts.blockchain_network_num == blockchain.network_num]
    )
    cache_info = {
        "relay_peers": [relay.__dict__ for relay in cache_network_info.relay_peers],
        "blockchain_network": [blockchain_network.__dict__ for blockchain_network in cache_network_info.blockchain_network]
    }
    data.update(cache_info)
    try:
        with open(opts.cookie_file_path, "w") as cookie_file:
            json.dump(data, cookie_file, indent=4)
    except Exception as ex:
        logger.error(f"Failed when tried to write to cache file: {opts.cookie_file_path} with exception: {ex}")


def read(opts) -> Optional[CacheNetworkInfo]:
    cache_file_info = None
    try:
        if os.path.exists(opts.cookie_file_path):
            with open(opts.cookie_file_path, "r") as cookie_file:
                return model_loader.load_model(CacheNetworkInfo, json.load(cookie_file))
    except Exception as ex:
        logger.error(f"Failed when tried to read from cache file: {opts.cookie_file_path} with exception: {ex}")
    finally:
        return cache_file_info
