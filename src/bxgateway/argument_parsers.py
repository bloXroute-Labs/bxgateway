import argparse
import sys
from typing import Tuple

from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxgateway import log_messages
from bxutils import logging


logger = logging.get_logger(__name__)


# Make sure enode is at least as long as the public key
def enode_is_valid_length(enode: str) -> bool:
    return len(enode) >= 2 * eth_common_constants.PUBLIC_KEY_LEN


def get_enode_parts(enode: str) -> Tuple[str, str, str]:
    enode_and_pub_key, ip_and_port = enode.split("@")
    if enode_and_pub_key.startswith("enode://"):
        pub_key = enode_and_pub_key[8:]
    else:
        pub_key = enode_and_pub_key
    ip, port_and_disc = ip_and_port.split(":")
    port = port_and_disc.split("?")[0]
    return pub_key, ip, port


def get_ip_port_string_parts(ip_port_string: str) -> Tuple[str, str]:
    ip_port_list = ip_port_string.strip().split(":")
    ip = ip_port_list[0]
    port = ip_port_list[1]
    return ip, port


def parse_enode(enode: str) -> BlockchainPeerInfo:
    if not enode_is_valid_length(enode):
        logger.fatal(log_messages.ETH_PARSER_INVALID_ENODE_LENGTH, enode, len(enode), exc_info=False)
        sys.exit(1)
    try:
        pub_key, ip, port = get_enode_parts(enode)
        if not port.isnumeric():
            logger.fatal(log_messages.PARSER_INVALID_PORT, port, exc_info=False)
            sys.exit(1)
    except ValueError:
        logger.fatal(log_messages.ETH_PARSER_INVALID_ENODE, enode, exc_info=False)
        sys.exit(1)
    else:
        return BlockchainPeerInfo(ip, int(port), pub_key)


def parse_ip_port(ip_port_string: str) -> BlockchainPeerInfo:
    ip, port = get_ip_port_string_parts(ip_port_string)
    if not port.isnumeric():
        logger.fatal(log_messages.PARSER_INVALID_PORT, port, exc_info=False)
        sys.exit(1)
    return BlockchainPeerInfo(ip, int(port))


def parse_peer(blockchain_protocol: str, peer: str) -> BlockchainPeerInfo:
    if blockchain_protocol.lower() == BlockchainProtocol.ETHEREUM.value:
        return parse_enode(peer)
    else:
        return parse_ip_port(peer)


class ParseEnode(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        assert isinstance(values, str)
        blockchain_peer = parse_enode(values)
        # Node public key gets validated in validate_eth_opts
        namespace.node_public_key = blockchain_peer.node_public_key
        # blockchain IP gets validated in __init__()
        namespace.blockchain_ip = blockchain_peer.ip
        namespace.blockchain_port = blockchain_peer.port


class ParseBlockchainPeers(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        assert isinstance(values, str)
        blockchain_peers = set()
        blockchain_protocol = namespace.blockchain_protocol
        for peer in values.split(","):
            blockchain_peer = parse_peer(blockchain_protocol, peer)
            blockchain_peers.add(blockchain_peer)
        namespace.blockchain_peers = blockchain_peers
