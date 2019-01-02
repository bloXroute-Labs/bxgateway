from mock import MagicMock

from bxcommon.constants import NULL_TX_SID
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import get_gateway_opts
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.crypto import symmetric_decrypt
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.services.blockchain_sync_service import BlockchainSyncService
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcNodeConnectionTests(AbstractTestCase):
    HASH = BtcObjectHash(binary=crypto.double_sha256("123"))

    def setUp(self):
        self.gateway_node = BtcGatewayNode(get_gateway_opts(8001, include_default_btc_args=True))
        self.gateway_node.tx_service = MagicMock()
        self.gateway_node.blockchain_sync_service = MagicMock(spec=BlockchainSyncService)
        self.sut = BtcNodeConnection(MockSocketConnection(), ("127.0.0.1", 8001), self.gateway_node)

    def test_msg_block(self):
        txns = [TxBtcMessage(0, 0, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]
        message = BlockBtcMessage(0, 0, self.HASH, self.HASH, 0, 0, 0, txns)
        self.gateway_node.broadcast = MagicMock()
        self.sut.connection_protocol.send_key = MagicMock()

        # find an sid every other call
        def get_txid(*_args, **_kwargs):
            if get_txid.counter % 2 == 0:
                return NULL_TX_SID
            else:
                return get_txid.counter

        get_txid.counter = 0

        self.sut.connection_protocol.msg_block(message)

        self.gateway_node.broadcast.assert_called_once()
        ((broadcast_message, _), _) = self.gateway_node.broadcast.call_args
        block_hash = bytes(broadcast_message.msg_hash().binary)

        cache_item = self.gateway_node.in_progress_blocks._cache.get(block_hash)
        self.assertEqual(cache_item.payload, symmetric_decrypt(cache_item.key, broadcast_message.blob().tobytes()))

        self.sut.connection_protocol.send_key.assert_called_once()
        ((send_block_hash,), _) = self.sut.connection_protocol.send_key.call_args
        self.assertEqual(block_hash, send_block_hash)

