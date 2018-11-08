from abc import ABCMeta

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.utils import logger


class GatewayConnection(AbstractConnection):
    __metaclass__ = ABCMeta

    def __init__(self, sock, address, node, from_me=False):
        super(GatewayConnection, self).__init__(sock, address, node, from_me)

        logger.debug("initialized connection to {0}".format(self.peer_desc))

        self.is_server = False  # This isn't a server message

    # Dumps state using debug
    def dump_state(self):
        logger.debug("Connection {0} state dump".format(self.peer_desc))
        logger.debug("Connection state: {0}".format(self.state))

        logger.debug("Inputbuf size: {0}".format(self.inputbuf.length))
        logger.debug("Outputbuf size: {0}".format(self.outputbuf.length))
