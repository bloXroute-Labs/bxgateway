from typing import TYPE_CHECKING

from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxutils import logging

from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection

if TYPE_CHECKING:
    from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode

logger = logging.get_logger(__name__)


class BtcLossyRelayConnection(BtcRelayConnection):
    def __init__(self, sock: SocketConnectionProtocol, node: "BtcGatewayNode"):
        super(BtcLossyRelayConnection, self).__init__(sock, node)

        logger.debug("Test mode: Client is started in test mode. Simulating dropped transactions.")

        self.tx_drop_counter = 0
        self.tx_assign_drop_counter = 0
        self.tx_unknown_txs_drop_counter = 0

    def msg_tx(self, msg):

        self.tx_drop_counter += 1

        # Drop every 10th message
        if self.tx_drop_counter > 0 and self.tx_drop_counter % 10 == 0:
            self.tx_drop_counter = 0
            logger.debug("Test mode: Dropping transaction message.")
        else:
            super(BtcLossyRelayConnection, self).msg_tx(msg)

    def msg_txs(self, msg):
        self.tx_unknown_txs_drop_counter += 1

        # Drop every 3rd message
        if self.tx_unknown_txs_drop_counter > 0 and self.tx_unknown_txs_drop_counter % 3 == 0:
            self.tx_unknown_txs_drop_counter = 0
            logger.debug("Test mode: Dropping unknown txs message.")
        else:
            super(BtcLossyRelayConnection, self).msg_txs(msg)
