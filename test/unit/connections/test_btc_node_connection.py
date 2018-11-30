from argparse import Namespace

from mock import MagicMock

from bxcommon.constants import BTC_HDR_COMMON_OFF, NULL_TX_SID
from bxcommon.messages.btc.block_btc_message import BlockBTCMessage
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import crypto
from bxcommon.utils.crypto import symmetric_decrypt
from bxcommon.utils.object_hash import BTCObjectHash
from bxgateway.connections.btc.btc_node_connection import BTCNodeConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class BtcNodeConnectionTests(AbstractTestCase):
    HASH = BTCObjectHash(binary=crypto.double_sha256("123"))

    def setUp(self):
        opts = Namespace()
        opts.__dict__ = {
            "blockchain_net_magic": 12345,
            "blockchain_version": 23456,
            "blockchain_nonce": 0,
            "blockchain_services": 1,
            "bloxroute_version": "bloxroute 1.5",
            "sid_expire_time": 30,
            "external_ip": "127.0.0.1",
            "external_port": 8001,
            "test_mode": []
        }
        self.gateway_node = AbstractGatewayNode(opts)
        self.gateway_node.tx_service = MagicMock()
        self.sut = BTCNodeConnection(MockSocketConnection(), ("127.0.0.1", 8001), self.gateway_node)

    def test_msg_block(self):
        txns = [TxBTCMessage(0, 0, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]
        message = BlockBTCMessage(0, 0, self.HASH, self.HASH, 0, 0, 0, txns)
        self.gateway_node.broadcast = MagicMock()
        self.sut.send_key = MagicMock()

        # find an sid every other call
        def get_txid(*_args, **_kwargs):
            if get_txid.counter % 2 == 0:
                return NULL_TX_SID
            else:
                return get_txid.counter

        get_txid.counter = 0
        self.gateway_node.tx_service.get_txid = get_txid

        self.sut.msg_block(message)

        self.gateway_node.broadcast.assert_called_once()
        ((broadcast_message, _), _) = self.gateway_node.broadcast.call_args
        block_hash = bytes(broadcast_message.msg_hash().binary)

        cache_item = self.gateway_node.in_progress_blocks._cache.get(block_hash)
        self.assertEqual(cache_item.payload, symmetric_decrypt(cache_item.key, broadcast_message.blob().tobytes()))

        self.sut.send_key.assert_called_once()
        ((send_block_hash,), _) = self.sut.send_key.call_args
        self.assertEqual(block_hash, send_block_hash)
