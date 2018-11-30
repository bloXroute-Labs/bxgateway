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

from bxcommon import constants, node_runner
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.utils import cli, config
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.gateway_node_factory import get_gateway_node_type
from bxgateway.testing.test_modes import TestModes

MAX_NUM_CONN = 8192
PID_FILE_NAME = "bxgateway.pid"


def convert_net_magic(magic):
    if magic in constants.btc_magic_numbers:
        return constants.btc_magic_numbers[magic]
    else:
        return int(magic)


def generate_default_nonce():
    return random.randint(0, sys.maxint)


def get_opts():
    common_args = cli.get_args()

    # Get more options specific to gateways.
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip", type=config.blocking_resolve_ip,
                            default="127.0.0.1")
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int, default=9333)
    arg_parser.add_argument("--blockchain-protocol", help="Blockchain protocol. E.g Bitcoin, Ethereum", type=str,
                            default="Bitcoin")
    arg_parser.add_argument("--blockchain-network", help="Blockchain network. E.g Mainnet, Testnet", type=str,
                            default="Mainnet")
    arg_parser.add_argument("--outbound-ip", help="(TEST ONLY) Override parameter for an outbound peer to connect to")
    arg_parser.add_argument("--outbound-port", help="(TEST ONLY) Override parameter for an outbound peer to connect to",
                            type=int)
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
    arg_parser.add_argument("--private-key", help="Private key for encrypted communication with Ethereum node", type=str)
    arg_parser.add_argument("--network-id", help="Ethereum network id", type=int)
    arg_parser.add_argument("--genesis-hash", help="Genesis block hash of Ethereum network", type=str)
    arg_parser.add_argument("--chain-difficulty", help="Difficulty of genesis block Ethereum network (hex)", type=str)
    arg_parser.add_argument("--no-discovery", help="Disable discovery of Ethereum node and wait for node to connect",
                            type=bool, default=False)

    gateway_args, unknown = arg_parser.parse_known_args()

    args = cli.merge_args(gateway_args, common_args)

    if gateway_args.outbound_ip is not None and gateway_args.outbound_port is not None:
        args.outbound_peers = [OutboundPeerModel(gateway_args.outbound_ip, gateway_args.outbound_port)]

    return args


if __name__ == '__main__':
    opts = get_opts()
    node_type = get_gateway_node_type(opts.blockchain_protocol, opts.blockchain_network)
    node_runner.run_node(PID_FILE_NAME, opts, node_type)
