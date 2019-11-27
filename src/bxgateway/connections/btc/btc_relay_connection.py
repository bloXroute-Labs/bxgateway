from typing import TYPE_CHECKING

from bxcommon.network.socket_connection_protocol import SocketConnectionProtocol
from bxgateway.messages.btc import btc_messages_util
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class BtcRelayConnection(AbstractRelayConnection):

    def __init__(self, sock: SocketConnectionProtocol, node: "AbstractGatewayNode"):
        super(BtcRelayConnection, self).__init__(sock, node)

    def msg_tx(self, msg):
        if msg.tx_val() != TxMessage.EMPTY_TX_VAL:
            hash_val = btc_messages_util.get_txid(msg.tx_val())

            if hash_val != msg.tx_hash():
                self.log_error("Received malformed transaction message from the BDN."
                               "Expected hash: {}. Actual: {}", hash_val, msg.tx_hash())
                return

        super(BtcRelayConnection, self).msg_tx(msg)
