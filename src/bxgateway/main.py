#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Startup script for nodes
#

import argparse

from bxcommon.util import startup_util
from connections import *

# All parameters that are parsed from the config file.
ALL_PARAMS = [
    'my_ip',
    'my_port',
    'peers',
    'my_idx',
    'manager_idx',
    'log_path',
    'log_stdout',
    'node_params',
    'node_addr'
]
MAX_NUM_CONN = 8192

if __name__ == '__main__':
    # Log our pid to a file.
    with open("relay.pid", "w") as f:
        f.write(str(os.getpid()))

    # Returns a dictionary of args like "-n" to arrays of values
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-name",
                        help="Name of section to read from config.cfg. By default will read a section using this node's"
                             " local ip. Not needed if you specify the other options.")
    parser.add_argument("-n", "--network-ip", help="Network ip of this node")
    parser.add_argument("-p", "--peers", help="Peering string to override peers of config.cfg")
    parser.add_argument("-P", "--port", help="What port to listen on")
    parser.add_argument("-l", "--log-path", help="Path to store logfiles in")
    parser.add_argument("-o", "--to-stdout", help="Log to stdout. Doesn't generate logfiles in this mode")
    parser.add_argument("-b", "--blockchain-node",
                        help="Blockchain node ip and port to connect to, space delimited, typically localhost")
    parser.add_argument("--blockchain-net-magic", help="Blockchain net.magic parameter")
    parser.add_argument("--blockchain-services", help="Blockchain services parameter")
    parser.add_argument("--bloxroute-version", help="Bloxroute version number")
    parser.add_argument("--blockchain-version", help="Blockchain protocol version")

    opts = parser.parse_args()

    # The local name is the section of the config.cfg we will read
    # It can be specified with -c or will be the local ip of the machine
    my_local_name = opts.config_name or startup_util.get_my_ip()

    # Parse the config file.
    configFileName = "config.cfg"
    config, params = startup_util.parse_config_file(configFileName, my_local_name, ALL_PARAMS)

    # Set basic variables.
    # XXX: Add assert statements to make sure these make sense.
    ip = opts.network_ip or params['my_ip']
    assert ip is not None, "Your IP address is not specified in config.cfg or as --network-ip. Check that the '-n' " \
                           "argument reflects the name of a section in config.cfg!"

    port = int(opts.port or params['my_port'])

    log_setmyname("%s:%d" % (ip, port))
    log_path = opts.log_path or params['log_path']
    use_stdout = opts.to_stdout or params['log_stdout']
    log_init(log_path, use_stdout)
    log_debug("My own IP for config purposes is {0}".format(my_local_name))

    # Initialize the node and register the peerfile update signal to USR2 signal.
    relay_nodes = startup_util.parse_peers(opts.peers or params['peers'])

    node_param_list = {}
    if params['node_params']:
        node_param_list = [x.strip() for x in params['node_params'].split(",")]

    node_params = {}

    if node_param_list:
        for param in node_param_list:
            node_params[param] = startup_util.getparam(config, my_local_name, param)

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

    node = Client(ip, port, relay_nodes, node_addr, node_params)

    # Start main loop
    try:
        log_debug("running node")
        node.run()
    finally:
        log_crash("node run method returned")
        log_crash("Log closed")
        log_close()
