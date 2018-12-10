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
from bxcommon.utils import cli, config
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


def parse_network_number(opts):
    # TODO: This list will be loaded from SDN
    networks_list = [
        (1, "Bitcoin", "Mainnet"),
        (2, "Bitcoin", "Testnet"),
        (3, "Ethereum", "Mainnet"),
        (4, "Ethereum", "Ropsten")
    ]

    network_num = None

    for network in networks_list:
        if opts.blockchain_protocol == network[1] and opts.blockchain_network == network[2]:
            network_num = network[0]
            break

    if network_num is None:
        all_networks = ", ".join(map(lambda network: "'{}' - '{}'".format(network[1], network[2]), networks_list))
        error_msg = "Network number does not exist for blockchain protocol '{}' and network '{}'. Valid options: {}." \
            .format(opts.blockchain_protocol, opts.blockchain_network, all_networks)
        print(error_msg)

        exit(1)

    opts.__dict__["network_num"] = network_num


def get_opts():
    common_args = cli.get_args()

    # Get more options specific to gateways.
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip", type=config.blocking_resolve_ip,
                            default="127.0.0.1")
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int, default=9333)
    arg_parser.add_argument("--peer-gateways",
                            help="Optional gateway peer ip/ports that will always be connected to. "
                                 "Should be in the format ip1:port1,ip2:port2,...",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--min-peer-gateways",
                            help="Minimum number of peer gateways before node will contact SDN for more.",
                            type=int,
                            default=1)
    arg_parser.add_argument("--blockchain-protocol", help="Blockchain protocol. E.g Bitcoin, Ethereum", type=str,
                            default="Bitcoin")
    arg_parser.add_argument("--blockchain-network", help="Blockchain network. E.g Mainnet, Testnet", type=str,
                            default="Mainnet")
    arg_parser.add_argument("--outbound-ip", help="(TEST ONLY) Override parameter for an outbound peer to connect to")
    arg_parser.add_argument("--outbound-port", help="(TEST ONLY) Override parameter for an outbound peer to connect to",
                            type=int)
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
    arg_parser.add_argument("--remote-public-key", help="Public key of Ethereum node for encrypted communication",
                            type=str)
    arg_parser.add_argument("--private-key", help="Private key for encrypted communication with Ethereum node",
                            type=str)
    arg_parser.add_argument("--network-id", help="Ethereum network id", type=int)
    arg_parser.add_argument("--genesis-hash", help="Genesis block hash of Ethereum network", type=str)
    arg_parser.add_argument("--chain-difficulty", help="Difficulty of genesis block Ethereum network (hex)", type=str)
    arg_parser.add_argument("--no-discovery", help="Disable discovery of Ethereum node and wait for node to connect",
                            type=bool, default=False)

    gateway_args, unknown = arg_parser.parse_known_args()

    args = cli.merge_args(gateway_args, common_args)
    args.outbound_peers = args.peer_gateways + args.peer_relays

    parse_network_number(args)
    return args


if __name__ == '__main__':
    opts = get_opts()
    node_type = get_gateway_node_type(opts.blockchain_protocol, opts.blockchain_network)
    node_runner.run_node(PID_FILE_NAME, opts, node_type)
