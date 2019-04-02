from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, logger
from bxgateway.btc_constants import BTC_SHA_HASH_LEN
from bxgateway.connections.abstract_relay_connection import AbstractRelayConnection
import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcRelayConnection(AbstractRelayConnection):

    def __init__(self, sock, address, node, from_me=False):
        super(BtcRelayConnection, self).__init__(sock, address, node, from_me=from_me)

        self.message_converter = converter_factory.create_btc_message_converter(
            node.opts.blockchain_net_magic,
            node.opts
        )

    def msg_tx(self, msg):
        if msg.tx_val() != TxMessage.EMPTY_TX_VAL:
            hash_val = BtcObjectHash(crypto.bitcoin_hash(msg.tx_val()), length=BTC_SHA_HASH_LEN)

            if hash_val != msg.tx_hash():
                logger.error("Got ill formed tx message from the bloXroute network")
                return

        super(BtcRelayConnection, self).msg_tx(msg)
