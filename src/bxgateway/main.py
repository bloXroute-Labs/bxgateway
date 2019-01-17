#!/bin/env python
#
# Copyright (C) 2018, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Startup script for Gateway nodes
#
import argparse
import random
import sys

from bxcommon import node_runner
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.utils import cli, config, convert
from bxgateway import btc_constants
from bxgateway.connections.gateway_node_factory import get_gateway_node_type
from bxgateway.testing.test_modes import TestModes

MAX_NUM_CONN = 8192
PID_FILE_NAME = "bxgateway.pid"


def convert_net_magic(magic):
    if magic in btc_constants.BTC_MAGIC_NUMBERS:
        return btc_constants.BTC_MAGIC_NUMBERS[magic]
    else:
        return int(magic)


def generate_default_nonce():
    return random.randint(0, sys.maxint)


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


def get_opts():
    common_args = cli.get_args()

    # Get more options specific to gateways.
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int, required=True)
    arg_parser.add_argument("--blockchain-protocol", help="Blockchain protocol. E.g Bitcoin, Ethereum", type=str,
                            required=True)
    arg_parser.add_argument("--blockchain-network", help="Blockchain network. E.g Mainnet, Testnet", type=str,
                            required=True)
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip", type=config.blocking_resolve_ip,
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
                            type=config.blocking_resolve_ip)
    arg_parser.add_argument("--remote-blockchain-port", help="Remote blockchain node port to proxy messages from",
                            type=int)
    arg_parser.add_argument("--is-internal-blockchain", help="If the node is internally owned.", type=convert.str_to_bool,
                            default=False)
    arg_parser.add_argument("--connect-to-remote-blockchain",
                            help="If gateway should proxy messages from a remote bloXroute owned blockchain node",
                            type=convert.str_to_bool,
                            default=True)
    arg_parser.add_argument("--peer-relays",
                            help="(TEST ONLY) Optional relays peer ip/ports that will always be connected to. "
                                 "Should be in the format ip1:port1,ip2:port2,...",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--test-mode",
                            help="(TEST ONLY) Test modes to run. Possible values: {0}".format(
                                [TestModes.DISABLE_ENCRYPTION, TestModes.DROPPING_TXS]
                            ),
                            default="",
                            nargs="*")

    # Bitcoin specific
    arg_parser.add_argument("--blockchain-version", help="Blockchain protocol version", type=int)
    arg_parser.add_argument("--blockchain-nonce", help="Blockchain nonce", default=generate_default_nonce())
    arg_parser.add_argument("--blockchain-net-magic", help="Blockchain net.magic parameter",
                            type=convert_net_magic)
    arg_parser.add_argument("--blockchain-services", help="Blockchain services parameter", type=int)

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

    gateway_args, unknown = arg_parser.parse_known_args()

    args = cli.merge_args(gateway_args, common_args)
    args.outbound_peers = args.peer_gateways + args.peer_relays
    if args.connect_to_remote_blockchain and args.remote_blockchain_ip and args.remote_blockchain_port:
        args.remote_blockchain_peer = OutboundPeerModel(args.remote_blockchain_ip, args.remote_blockchain_port)
    else:
        args.remote_blockchain_peer = None

    return args


if __name__ == "__main__":
    opts = get_opts()
    node_type = get_gateway_node_type(opts.blockchain_protocol, opts.blockchain_network)
    node_runner.run_node(PID_FILE_NAME, opts, node_type)
