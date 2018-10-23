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
from bxcommon.utils import cli, config
from bxgateway.connections.gateway_node import GatewayNode

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

    arg_parser = argparse.ArgumentParser()

    # Get more options specific to gateways.
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip", type=config.blocking_resolve_ip,
                            required=False, default="127.0.0.1")
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int, required=False,
                            default=9333)
    arg_parser.add_argument("--blockchain-version", help="Blockchain protocol version", type=int)
    arg_parser.add_argument("--blockchain-nonce", help="Blockchain nonce", required=False,
                            default=generate_default_nonce())
    arg_parser.add_argument("--blockchain-net-magic", help="Blockchain net.magic parameter",
                            type=convert_net_magic)
    arg_parser.add_argument("--blockchain-services", help="Blockchain services parameter", type=int)

    arg_parser.add_argument("--test-mode", help="Test modes to run. Possible values: {0}", required=False)

    gateway_args, unknown = arg_parser.parse_known_args()

    for key, val in gateway_args.__dict__.items():
        common_args.__dict__[key] = val

    return common_args


if __name__ == '__main__':
    node_runner.run_node(PID_FILE_NAME, get_opts(), GatewayNode)
