#!/bin/env python
#
# Copyright (C) 2018, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Startup script for Gateway nodes
#
import argparse
import os
import random
import sys

from bxcommon import node_runner, constants
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.utils import cli, convert, config, ip_resolver
from bxgateway import btc_constants, gateway_constants, eth_constants
from bxgateway.connections.gateway_node_factory import get_gateway_node_type
from bxgateway.testing.test_modes import TestModes
from bxgateway.utils import node_cache
from bxgateway.utils.eth import crypto_utils

MAX_NUM_CONN = 8192
PID_FILE_NAME = "bxgateway.pid"


def convert_net_magic(magic):
    if magic in btc_constants.BTC_MAGIC_NUMBERS:
        return btc_constants.BTC_MAGIC_NUMBERS[magic]
    else:
        return int(magic)


def generate_default_nonce():
    return random.randint(0, sys.maxsize)


def parse_peer_string(peer_string):
    """
    Parses string of format ip:port,ip:port,ip:port,... to list of OutboundPeerModels.
    """
    peers = []
    for ip_port_string in peer_string.split(","):
        if ip_port_string:
            ip_port_list = ip_port_string.strip().split(":")
            ip = ip_port_list[0]
            port = int(ip_port_list[1])
            peers.append(OutboundPeerModel(ip, port))
    return peers


def get_sdn_hostname(sdn_url: str) -> str:
    new_sdn_url = sdn_url
    if "://" in sdn_url:
        new_sdn_url = sdn_url.split("://")[1]

    return new_sdn_url


def get_default_eth_private_key():
    gateway_key_file_name = config.get_data_file(eth_constants.GATEWAY_PRIVATE_KEY_FILE_NAME)

    if os.path.exists(gateway_key_file_name):
        with open(gateway_key_file_name, "r") as key_file:
            private_key = key_file.read().strip()
    else:
        private_key = crypto_utils.generate_random_private_key_hex_str()
        with open(gateway_key_file_name, "w") as key_file:
            key_file.write(private_key)

    return private_key


def get_opts() -> argparse.Namespace:
    config.set_working_directory(os.path.dirname(__file__))

    # Parse gateway specific command line parameters
    arg_parser = cli.get_argument_parser()
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int)
    arg_parser.add_argument("--blockchain-protocol", help="Blockchain protocol. e.g Bitcoin, Ethereum", type=str,
                            required=True)
    arg_parser.add_argument("--blockchain-network", help="Blockchain network. e.g Mainnet, Testnet", type=str,
                            required=True)
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip",
                            type=ip_resolver.blocking_resolve_ip,
                            default="127.0.0.1")
    arg_parser.add_argument("--peer-gateways",
                            help="Optional gateway peer ip/ports that will always be connected to. "
                                 "Should be in the format ip1:port1,ip2:port2,...",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--min-peer-gateways",
                            help="Minimum number of peer gateways before node will contact SDN for more.",
                            type=int,
                            default=1)
    arg_parser.add_argument("--remote-blockchain-ip", help="Remote blockchain node ip to proxy messages from",
                            type=ip_resolver.blocking_resolve_ip)
    arg_parser.add_argument("--remote-blockchain-port", help="Remote blockchain node port to proxy messages from",
                            type=int)
    arg_parser.add_argument("--connect-to-remote-blockchain",
                            help="If gateway should proxy messages from a remote bloXroute owned blockchain node",
                            type=convert.str_to_bool,
                            default=True)
    arg_parser.add_argument("--encrypt-blocks",
                            help="If gateway should encrypt blocks",
                            type=convert.str_to_bool,
                            default=False)
    arg_parser.add_argument("--peer-relays",
                            help="(TEST ONLY) Optional relays peer ip/ports that will always be connected to. "
                                 "Should be in the format ip1:port1,ip2:port2,...",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--test-mode",
                            help="(TEST ONLY) Test modes to run. Possible values: {0}".format(
                                [TestModes.DROPPING_TXS]
                            ),
                            default="",
                            nargs="*")
    arg_parser.add_argument("--sync-tx-service", help="sync tx service in gateway", type=convert.str_to_bool,
                            default=True)

    # Bitcoin specific
    arg_parser.add_argument("--blockchain-version", help="Bitcoin protocol version", type=int)
    arg_parser.add_argument("--blockchain-nonce", help="Bitcoin nonce", default=generate_default_nonce())
    arg_parser.add_argument("--blockchain-net-magic", help="Bitcoin net.magic parameter",
                            type=convert_net_magic)
    arg_parser.add_argument("--blockchain-services", help="Bitcoin services parameter", type=int)
    arg_parser.add_argument("--enable-node-cache", help="Retrieve peers from cookie if unavailable",
                            type=convert.str_to_bool,
                            default=True)

    # Ethereum specific
    arg_parser.add_argument("--node-public-key", help="Public key of Ethereum node for encrypted communication",
                            type=str)
    arg_parser.add_argument("--private-key", help="Private key for encrypted communication with Ethereum node",
                            type=str)
    arg_parser.add_argument("--network-id", help="Ethereum network id", type=int)
    arg_parser.add_argument("--genesis-hash", help="Genesis block hash of Ethereum network", type=str)
    arg_parser.add_argument("--chain-difficulty", help="Difficulty of genesis block Ethereum network (hex)", type=str)
    arg_parser.add_argument("--no-discovery", help="Disable discovery of Ethereum node and wait for node to connect",
                            type=bool, default=False)
    arg_parser.add_argument("--remote-public-key",
                            help="Public key of remote bloXroute owned Ethereum node for encrypted communication "
                                 "during chainstate sync ",
                            type=str)
    arg_parser.add_argument(
        "--compact-block",
        help="Specify either the gateway supports compact block message or not",
        type=convert.str_to_bool,
        default=constants.ACCEPT_COMPACT_BLOCK
    )
    arg_parser.add_argument(
        "--compact-block-min-tx-count",
        help="Minimal number of short transactions in compact block to attempt decompression.",
        type=int, default=btc_constants.BTC_COMPACT_BLOCK_DECOMPRESS_MIN_TX_COUNT
    )
    arg_parser.add_argument(
        "--dump-short-id-mapping-compression",
        help="If true, the gateway will dump all short ids and txhashes compressed into a block",
        type=convert.str_to_bool,
        default=False
    )
    arg_parser.add_argument(
        "--dump-short-id-mapping-compression-path",
        help="Folder to dump compressed short ids to",
        default="/app/bxgateway/debug/compressed-short-ids"
    )
    arg_parser.add_argument(
        "--tune-send-buffer-size",
        help="If true, then the gateway will increase the send buffer's size for the blockchain connection",
        default=False,
        type=convert.str_to_bool
    )
    arg_parser.add_argument(
        "--max-block-interval",
        help="Maximum time gateway holds a block while waiting for confirmation of receipt from blockchain node",
        type=int,
        default=gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S
    )
    arg_parser.add_argument(
        "--cookie-file-path",
        help="Cookie file path",
        type=str,
    )
    arg_parser.add_argument(
        "--blockchain-message-ttl",
        help="Duration to queue up messages for if blockchain node connection is broken",
        type=int,
        default=gateway_constants.DEFAULT_BLOCKCHAIN_MESSAGE_TTL_S
    )
    arg_parser.add_argument(
        "--remote-blockchain-message-ttl",
        help="Duration to queue up messages for if remote blockchain node connection is broken",
        type=int,
        default=gateway_constants.DEFAULT_REMOTE_BLOCKCHAIN_MESSAGE_TTL_S
    )
    arg_parser.add_argument(
        "--stay-alive-duration",
        help="Duration Gateway should stay alive for without an active blockchain or relay connection",
        type=int,
        default=gateway_constants.DEFAULT_STAY_ALIVE_DURATION_S
    )
    arg_parser.add_argument(
        "--initial-liveliness-check",
        help="Duration Gateway should stay alive for without an initial blockchain or relay connection",
        type=int,
        default=gateway_constants.INITIAL_LIVELINESS_CHECK_S
    )
    arg_parser.add_argument(
        "--config-update-interval",
        help="update the node configuration on cron, 0 to disable",
        type=int,
        default=gateway_constants.CONFIG_UPDATE_INTERVAL_S
    )
    arg_parser.add_argument("--require-blockchain-connection",
                            help="Close gateway if connection with blockchain node can't be established "
                                 "when the flag is set to True",
                            type=convert.str_to_bool,
                            default=True)

    opts = cli.parse_arguments(arg_parser)

    config.set_data_directory(opts.data_dir)

    if not opts.blockchain_network:
        cache_file_info = node_cache.read(opts)
        if cache_file_info is not None:
            opts.blockchain_network = cache_file_info.blockchain_network

    opts.outbound_peers = opts.peer_gateways + opts.peer_relays

    if opts.connect_to_remote_blockchain and opts.remote_blockchain_ip and opts.remote_blockchain_port:
        opts.remote_blockchain_peer = OutboundPeerModel(opts.remote_blockchain_ip, opts.remote_blockchain_port)
    else:
        opts.remote_blockchain_peer = None

    if not opts.cookie_file_path:
        opts.cookie_file_path = gateway_constants.COOKIE_FILE_PATH_TEMPLATE.format(
            "{}_{}".format(get_sdn_hostname(opts.sdn_url), opts.external_ip))

    if opts.private_key is None:
        opts.private_key = get_default_eth_private_key()

    return opts


def main():
    logger_names = node_runner.LOGGER_NAMES.copy()
    logger_names.append("bxgateway")
    opts = get_opts()
    node_type = get_gateway_node_type(opts.blockchain_protocol, opts.blockchain_network)
    node_runner.run_node(config.get_data_file(PID_FILE_NAME), opts, node_type, logger_names=logger_names)


if __name__ == "__main__":
    main()
