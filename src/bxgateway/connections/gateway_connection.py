from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger


class GatewayConnection(AbstractConnection):
    def __init__(self, sock, address, node, from_me=False, setup=False):
        super(GatewayConnection, self).__init__(sock, address, node, from_me, setup)

        logger.debug("initialized connection to {0}".format(self.peer_desc))

        self.is_server = False  # This isn't a server message

    # Send some bytes to the peer of this connection from the next cut through message or from the outputbuffer.
    def send(self):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        byteswritten = self.send_bytes_on_buffer(self.outputbuf)
        logger.debug(
            "{0} bytes sent to {1}. {2} bytes left.".format(byteswritten, self.peer_desc, self.outputbuf.length))

    # Dumps state using debug
    def dump_state(self):
        logger.debug("Connection {0} state dump".format(self.peer_desc))
        logger.debug("Connection state: {0}".format(self.state))

        logger.debug("Inputbuf size: {0}".format(self.inputbuf.length))
        logger.debug("Outputbuf size: {0}".format(self.outputbuf.length))