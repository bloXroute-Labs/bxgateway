import argparse
import sys

from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxutils import logging


logger = logging.get_logger(__name__)


class ParseEnode(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        assert isinstance(values, str)
        enode = values

        # Make sure enode is at least as long as the public key
        if len(enode) < 2 * eth_common_constants.PUBLIC_KEY_LEN:
            logger.fatal("Invalid enode. "
                         "Invalid enode length: {}", len(enode), exc_info=False)
            sys.exit(1)
        try:
            enode_and_pub_key, ip_and_port = enode.split("@")
            if enode_and_pub_key.startswith("enode://"):
                pub_key = enode_and_pub_key[8:]
            else:
                pub_key = enode_and_pub_key
            ip, port_and_disc = ip_and_port.split(":")
            port = port_and_disc.split("?")[0]
        except ValueError:
            logger.fatal("Invalid enode: {}", enode, exc_info=False)
            sys.exit(1)
        else:
            # Node public key gets validated in validate_eth_opts
            namespace.node_public_key = pub_key
            # blockchain IP gets validated in __init__()
            namespace.blockchain_ip = ip
            # Port validation
            if not port.isnumeric():
                logger.fatal("Invalid port: {}", port, exc_info=False)
                sys.exit(1)
            namespace.blockchain_port = int(port)
