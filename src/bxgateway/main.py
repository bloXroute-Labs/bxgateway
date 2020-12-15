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
import functools
from typing import Optional, List

from bxcommon import node_runner, constants
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.utils import cli, convert, config, ip_resolver
from bxcommon.models.node_type import NodeType

from bxutils import logging_messages_utils

from bxgateway import btc_constants, gateway_constants, ont_constants
from bxgateway.connections.gateway_node_factory import get_gateway_node_type
from bxgateway.testing.test_modes import TestModes
from bxcommon.utils.blockchain_utils.eth import crypto_utils, eth_common_constants
from bxgateway.gateway_opts import GatewayOpts
from bxgateway import argument_parsers
from bxgateway.utils.gateway_start_args import GatewayStartArgs
from bxgateway import gateway_init_tasks

MAX_NUM_CONN = 8192
PID_FILE_NAME = "bxgateway.pid"


def convert_net_magic(magic):
    if magic in btc_constants.BTC_MAGIC_NUMBERS:
        return btc_constants.BTC_MAGIC_NUMBERS[magic]
    elif magic in ont_constants.ONT_MAGIC_NUMBERS:
        return ont_constants.ONT_MAGIC_NUMBERS[magic]
    else:
        return int(magic)


def generate_default_nonce():
    return random.randint(0, sys.maxsize)


def parse_peer_string(peer_string):
    """
    Parses string of format ip:port:node_type,ip:port,ip:port:node_type,... to list of OutboundPeerModels.
    """
    peers = []
    for ip_port_string in peer_string.split(","):
        if ip_port_string:
            ip, port_str, node_type_str = ip_port_string.strip().split(":")
            peers.append(OutboundPeerModel(ip, int(port_str), node_type=NodeType[node_type_str.upper()]))
    return peers


def get_default_eth_private_key():
    gateway_key_file_name = config.get_data_file(eth_common_constants.GATEWAY_PRIVATE_KEY_FILE_NAME)

    if os.path.exists(gateway_key_file_name):
        with open(gateway_key_file_name, "r") as key_file:
            private_key = key_file.read().strip()
    else:
        private_key = crypto_utils.generate_random_private_key_hex_str()
        with open(gateway_key_file_name, "w") as key_file:
            key_file.write(private_key)

    return private_key


def get_argument_parser() -> argparse.ArgumentParser:
    # Parse gateway specific command line parameters
    arg_parser = argparse.ArgumentParser(parents=[cli.get_argument_parser()],
                                         description="Command line interface for the bloXroute Gateway.",
                                         usage="bloxroute_gateway --blockchain-protocol [PROTOCOL] [additional "
                                               "arguments]")
    cli.add_argument_parser_rpc(arg_parser)

    arg_parser.add_argument("--blockchain-protocol", help="Blockchain protocol. e.g BitcoinCash, Ethereum", type=str)
    arg_parser.add_argument("--blockchain-network", help="Blockchain network. e.g Mainnet, Testnet", type=str)
    arg_parser.add_argument("--blockchain-port", help="Blockchain node port", type=int)
    arg_parser.add_argument("--blockchain-ip", help="Blockchain node ip",
                            type=ip_resolver.blocking_resolve_ip,
                            default=None)
    arg_parser.add_argument("--peer-gateways",
                            help="Optional gateway peer ip/ports that will always be connected to. "
                                 "Should be in the format ip1:port1:GATEWAY,ip2:port2:GATEWAY",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--min-peer-gateways",
                            help="Minimum number of peer gateways before node will contact SDN for more.",
                            type=int,
                            default=0)
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
                                 "Should be in the format ip1:port1:RELAY,ip2:port2:RELAY,...",
                            type=parse_peer_string,
                            default="")
    arg_parser.add_argument("--test-mode",
                            help="(TEST ONLY) Test modes to run. Possible values: {0}".format(
                                [TestModes.DROPPING_TXS]
                            ),
                            default="",
                            nargs="*")

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
    arg_parser.add_argument("--enode", help="Ethereum enode. ex) enode://<eth node public key>@<eth node "
                                            "ip>:<port>?discport=0",
                            action=argument_parsers.ParseEnode,
                            type=str,
                            default=None)

    # Blockchain peers specified by --blockchain-peers and --blockchain-peers-file would be aggregated together
    arg_parser.add_argument("--blockchain-peers",
                            help="A comma separated list of node peer info. "
                                 "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
                                 "For other blockchain protocols, the format is <ip>:<port>",
                            action=argument_parsers.ParseBlockchainPeers,
                            type=str,
                            default=None)
    arg_parser.add_argument("--blockchain-peers-file",
                            help="A new line separated list of node peer info. "
                                 "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
                                 "For other blockchain protocols, the format is <ip>:<port>",
                            type=str,
                            default=None)
    # IPC
    arg_parser.add_argument("--ipc",
                            help="Boolean indicating if IPC should be enabled.",
                            type=convert.str_to_bool,
                            default=False)
    arg_parser.add_argument("--ipc-file",
                            help="IPC filename that represents a unix domain socket",
                            type=str,
                            default="bxgateway.ipc")
    arg_parser.add_argument("--should-restart-on-high-memory",
                            help="Should a gateway restart itself if memory exceeds 2GB",
                            type=convert.str_to_bool,
                            default=True)
    # Ontology specific
    # TODO: Remove test only arguments
    arg_parser.add_argument("--http-info-port", help="(TEST ONLY)Ontology http server port to view node information",
                            type=int, default=config.get_env_default(GatewayStartArgs.HTTP_INFO_PORT))
    arg_parser.add_argument("--consensus-port", help="Ontology consensus port", type=int,
                            default=config.get_env_default(GatewayStartArgs.CONSENSUS_PORT))
    arg_parser.add_argument("--relay", help="(TEST ONLY)Ontology relay state", type=convert.str_to_bool,
                            default=config.get_env_default(GatewayStartArgs.RELAY_STATE))
    arg_parser.add_argument("--is-consensus", help="(TEST ONLY)Ontology consensus node", type=convert.str_to_bool,
                            default=config.get_env_default(GatewayStartArgs.IS_CONSENSUS))

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
        default=False,
    )
    arg_parser.add_argument(
        "--dump-short-id-mapping-compression-path",
        help="Folder to dump compressed short ids to",
        default="/app/bxgateway/debug/compressed-short-ids",
    )
    arg_parser.add_argument(
        "--tune-send-buffer-size",
        help="If true, then the gateway will increase the send buffer's size for the blockchain connection",
        default=True,
        type=convert.str_to_bool,
    )
    arg_parser.add_argument(
        "--max-block-interval-s",
        help="Maximum time gateway holds a block while waiting for confirmation of receipt from blockchain node",
        type=int,
        default=gateway_constants.MAX_INTERVAL_BETWEEN_BLOCKS_S,
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
        default=gateway_constants.DEFAULT_BLOCKCHAIN_MESSAGE_TTL_S,
    )
    arg_parser.add_argument(
        "--remote-blockchain-message-ttl",
        help="Duration to queue up messages for if remote blockchain node connection is broken",
        type=int,
        default=gateway_constants.DEFAULT_REMOTE_BLOCKCHAIN_MESSAGE_TTL_S,
    )
    arg_parser.add_argument(
        "--stay-alive-duration",
        help="Duration Gateway should stay alive for without an active blockchain or relay connection",
        type=int,
        default=gateway_constants.DEFAULT_STAY_ALIVE_DURATION_S,
    )
    arg_parser.add_argument(
        "--initial-liveliness-check",
        help="Duration Gateway should stay alive for without an initial blockchain or relay connection",
        type=int,
        default=gateway_constants.INITIAL_LIVELINESS_CHECK_S,
    )
    arg_parser.add_argument(
        "--config-update-interval",
        help="update the node configuration on cron, 0 to disable",
        type=int,
        default=gateway_constants.CONFIG_UPDATE_INTERVAL_S,
    )
    arg_parser.add_argument(
        "--require-blockchain-connection",
        help="Close gateway if connection with blockchain node can't be established "
             "when the flag is set to True",
        type=convert.str_to_bool,
        default=True,
    )
    default_tx_flag = config.get_env_default(
        GatewayStartArgs.DEFAULT_TX_FLAG
    )
    arg_parser.add_argument(
        "--default-tx-flag",
        help=f"transaction flag to use when distributing transactions to the Bdn network (default: {default_tx_flag})",
        type=TransactionFlag.from_string,
        choices=list(TransactionFlag),
        default=default_tx_flag,
    )
    arg_parser.add_argument(
        "--ws",
        help=f"Enable RPC websockets server (default: False)",
        type=convert.str_to_bool,
        default=False,
    )
    arg_parser.add_argument(
        "--ws-host",
        help=f"Websockets server listening host (default: {gateway_constants.WS_DEFAULT_HOST})",
        type=str,
        default=gateway_constants.WS_DEFAULT_HOST,
    )
    arg_parser.add_argument(
        "--ws-port",
        help=f"Websockets server listening port (default: {gateway_constants.WS_DEFAULT_PORT}",
        type=int,
        default=gateway_constants.WS_DEFAULT_PORT,
    )
    arg_parser.add_argument(
        "--eth-ws-uri",
        help="Ethereum websockets endpoint for syncing transaction content",
        type=str,
    )
    arg_parser.add_argument(
        "--process-node-txs-in-extension",
        help="If true, then the gateway will process transactions received from blockchain node using C++ extension",
        default=True,
        type=convert.str_to_bool,
    )
    # TODO temp arg, need to be removed
    arg_parser.add_argument(
        "--enable-eth-extensions",
        help="If true, run compression and decompression using C++ extensions code",
        default=True,
        type=convert.str_to_bool,
    )
    arg_parser.add_argument(
        "--request-recovery",
        help="If true, gateway will send transaction recovery request to relay when unable to decompress a block",
        default=True,
        type=convert.str_to_bool,
    )
    arg_parser.add_argument(
        "--enable-block-compression",
        help="If set, overrides value from the SDN, and might be changed if SDN sent an update",
        type=convert.str_to_bool,
        nargs='?',
    )
    arg_parser.add_argument(
        "--filter-txs-factor",
        help="Ethereum only. Sets the factor of the average gas price to filter transactions below. "
             "(i.e. 0 => send all transactions (average * 0), 1 => send all transactions with average or "
             "higher gas price, etc.)",
        type=float,
        default=0
    )

    return arg_parser


def get_opts(args: Optional[List[str]] = None) -> GatewayOpts:
    config.set_working_directory(os.path.dirname(__file__))

    opts = GatewayOpts.from_opts(cli.parse_arguments(get_argument_parser(), args))
    config.set_data_directory(opts.data_dir)
    if opts.private_key is None:
        opts.private_key = get_default_eth_private_key()

    return opts


def main() -> None:
    logger_names = node_runner.LOGGER_NAMES.copy()
    logger_names.append("bxgateway")
    logging_messages_utils.logger_names = set(logger_names)
    opts = get_opts()
    get_node_class = functools.partial(get_gateway_node_type, opts.blockchain_protocol)

    node_runner.run_node(
        config.get_data_file(PID_FILE_NAME),
        opts,
        get_node_class,
        NodeType.EXTERNAL_GATEWAY,
        logger_names=logger_names,
        node_init_tasks=gateway_init_tasks.init_tasks
    )


if __name__ == "__main__":
    main()
