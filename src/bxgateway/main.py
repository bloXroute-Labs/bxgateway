#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Startup script for nodes
#
import socket

from bxcommon.utils import config, logger
from bxgateway.connections.gateway_node import GatewayNode

# Extra parameters for gateway that are parsed from the config file.
GATEWAY_PARAMS = [
    'node_params',
    'node_addr'
]
MAX_NUM_CONN = 8192
CONFIG_FILE_NAME = "config.cfg"
PID_FILE_NAME = "bxgateway.pid"

if __name__ == '__main__':
    config.log_pid(PID_FILE_NAME)

    arg_parser = config.get_base_arg_parser()
    arg_parser.add_argument("-b", "--blockchain-node",
                            help="Blockchain node ip and port to connect to, space delimited, typically localhost")
    arg_parser.add_argument("--blockchain-net-magic", help="Blockchain net.magic parameter")
    arg_parser.add_argument("--blockchain-services", help="Blockchain services parameter")
    arg_parser.add_argument("--bloxroute-version", help="Bloxroute version number")
    arg_parser.add_argument("--blockchain-version", help="Blockchain protocol version")

    opts = arg_parser.parse_args()

    # The local name is the section of the config.cfg we will read
    # It can be specified with -c or will be the local ip of the machine
    my_ip = opts.config_name or config.get_my_ip()

    # Parse the config corresponding to the ip in the config file.
    config_parser, params = config.parse_config_file(CONFIG_FILE_NAME, my_ip, GATEWAY_PARAMS)

    ip, port = config.parse_addr(opts, params)

    config.init_logging(ip, port, opts, params)

    # Initialize the node and register the peerfile update signal to USR2 signal.
    relay_nodes = config.parse_peers(opts.peers or params['peers'])

    node_param_list = {}
    if params['node_params']:
        node_param_list = [x.strip() for x in params['node_params'].split(",")]

    node_params = {}

    if node_param_list:
        for param in node_param_list:
            node_params[param] = config.getparam(config_parser, my_ip, param)

    if opts.blockchain_node:
        params['node_addr'] = opts.blockchain_node
    if opts.blockchain_net_magic:
        node_params['magic'] = opts.blockchain_net_magic
    if opts.blockchain_services:
        node_params['services'] = opts.blockchain_services
    if opts.bloxroute_version:
        node_params['bloxroute_version'] = opts.bloxroute_version
    if opts.blockchain_version:
        node_params['protocol_version'] = opts.blockchain_version
    if opts.bloxroute_version:
        node_params['version'] = opts.bloxroute_version

    tokens = params['node_addr'].strip().split()
    node_ip = socket.gethostbyname(tokens[0])
    node_port = int(tokens[1])
    node_addr = (node_ip, node_port)

    node = GatewayNode(ip, port, relay_nodes, node_addr, node_params)

    # Start main loop
    try:
        logger.debug("running node")
        node.run()
    finally:
        logger.fatal("node run method returned")
        logger.fatal("Log closed")
        logger.log_close()
