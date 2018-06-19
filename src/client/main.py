#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Startup script for nodes
#
# Authors: Soumya Basu, Emin Gun Sirer
#

import select
import socket
from connections import *
from utils import *
import argparse
import signal
import os.path
import os
import resource
import ConfigParser
import cProfile, pstats
import sys
import StringIO
import argparse
from pympler import asizeof

# All parameters that are parsed from the config file.
ALL_PARAMS = ['my_ip', 'my_port', 'peers', 'my_idx', 'is_client', 'manager_idx', 'node_params', 'node_addr', 'log_path',
              'log_stdout']
MAX_NUM_CONN = 8192

# Some websites are blocked in certain jurisdictions, so we try multiple websites to see whichever one works.
WEBSITES_TO_TRY = ['www.google.com', 'www.alibaba.com']


# Returns the local internal IP address of the node.
# If the node is behind a NAT or proxy, then this is not the externally visible IP address.
def getmyip():
    for website in WEBSITES_TO_TRY:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((website, 80))
            return s.getsockname()[0]
        except socket.timeout:
            continue

    raise Exception("Could not find any local name!")


# Parse the config filename and return a params dictionary with the params from ALL_PARAMS
def parse_config_file(filename, localname):
    config = ConfigParser.ConfigParser()
    config.read(filename)

    params = {}
    for pname in ALL_PARAMS:
        params[pname] = getparam(config, localname, pname)

    return config, params


# Gets the param "pname" from the config file.
# If the param exists under the localname, we use that one. Otherwise, we use
# the param under default.
def getparam(config, localname, pname):
    try:
        return config.get(localname, pname)
    except (ConfigParser.NoOptionError, ConfigParser.NoSectionError) as e:
        try:
            return config.get("default", pname)
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError) as e:
            return None
    else:
        return None


# Parse the peers file and returns a dictionary of {cls : list of ip, port pairs}
# to tell the node which connection types to instantiate.
# Parse the peers string and return two lists:
#   1) A list of relays that are internal nodes in our network
#   2) A list of trusted peers that we will connect to.
def parse_peers(peers_string):
    nodes = {}

    if peers_string is not None:
        for line in peers_string.split(","):
            tokens = line.strip().split()

            peer_ip = None
            while peer_ip is None:
                try:
                    peer_ip = socket.gethostbyname(tokens[0])
                except socket.error as e:
                    print("Caught socket error while resolving name! Retrying...")
                    time.sleep(0.1)
                    peer_ip = None

            peer_port = int(tokens[1])
            peer_idx = int(tokens[2])
            nodes[peer_idx] = (peer_ip, peer_port)

    return nodes


if __name__ == '__main__':
    # Log our pid to a file.
    with open("relay.pid", "w") as f:
        f.write(str(os.getpid()))

    # Local name:
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config-name",
                        help="Name of section to read from config.cfg. By default will read a section using this node's local ip. Not needed if you specify the other options.")
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
    mylocalname = opts.config_name or getmyip()

    # Parse the config file.
    configFileName = "config.cfg"
    config, params = parse_config_file(configFileName, mylocalname)

    # XXX: The client is always the client. All references to is_client should be removed
    is_client = True  # params['is_client'] == 'True'

    if not is_client:
        resource.setrlimit(resource.RLIMIT_NOFILE, (MAX_NUM_CONN, 2 * MAX_NUM_CONN))
        resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

    # Set basic variables.
    # XXX: Add assert statements to make sure these make sense.
    ip = opts.network_ip or params['my_ip']
    assert ip != None, "Your IP address is None!"
    port = int(opts.port or params['my_port'])

    log_setmyname("%s:%d" % (ip, port))
    log_path = opts.log_path or params['log_path']
    use_stdout = opts.to_stdout or params['log_stdout']
    log_init(log_path, use_stdout)
    log_debug("main", "My own IP for config purposes is {0}".format(mylocalname))

    # Initialize the node and register the peerfile update signal to USR2 signal.
    relay_nodes = parse_peers(opts.peers or params['peers'])

    node_param_list = {}
    if params['node_params']:
        node_param_list = [x.strip() for x in params['node_params'].split(",")]

    if is_client:
        node_params = {}

        if node_param_list:
            for param in node_param_list:
                node_params[param] = getparam(config, mylocalname, param)

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
    else:
        idx = int(opts.index or params['my_idx'])
        midx = int(opts.manager or params['manager_idx'])
        node = Server(ip, port, relay_nodes, midx, idx)

    # Start main loop
    try:
        log_debug("main", "running node")
        node.run()
    finally:
        log_crash("main", "node run method returned")
        log_crash("main", "Log closed")
        log_close()
