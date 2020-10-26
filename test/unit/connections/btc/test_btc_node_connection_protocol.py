from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash

from bxgateway.btc_constants import NODE_WITNESS_SERVICE_FLAG, BTC_SHA_HASH_LEN
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_node_connection_protocol import BtcNodeConnectionProtocol
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, InventoryType, GetDataBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class BtcNodeConnectionProtocolTest(AbstractTestCase):

    def setUp(self):
        opts = gateway_helpers.get_gateway_opts(8000, include_default_btc_args=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.node = MockGatewayNode(opts)
        self.node.neutrality_service = MagicMock()

        self.connection = BtcNodeConnection(MockSocketConnection(node=self.node, ip_address=LOCALHOST, port=123), self.node)
        gateway_helpers.add_blockchain_peer(self.node, self.connection)

        self.tx_hash = BtcObjectHash(buf=helpers.generate_bytearray(32), length=BTC_SHA_HASH_LEN)
        self.block_hash = BtcObjectHash(buf=helpers.generate_bytearray(32), length=BTC_SHA_HASH_LEN)

        self.sut = BtcNodeConnectionProtocol(self.connection)

        while self.connection.outputbuf.length > 0:
            initial_bytes = self.connection.get_bytes_to_send()
            self.connection.advance_sent_bytes(len(initial_bytes))

    def test_non_segwit_version_received(self):
        self.assertFalse(self.sut.request_witness_data)

        version_msg = self._create_version_msg(0)
        self.sut.msg_version(version_msg)
        self.assertFalse(self.sut.request_witness_data)

    def test_segwit_version_received(self):
        self.assertFalse(self.sut.request_witness_data)

        version_msg = self._create_version_msg(0 | NODE_WITNESS_SERVICE_FLAG)
        self.sut.msg_version(version_msg)
        self.assertTrue(self.sut.request_witness_data)

    def test_get_data_only_new_data(self):
        seen_block_hash = BtcObjectHash(buf=helpers.generate_bytearray(BTC_SHA_HASH_LEN), length=BTC_SHA_HASH_LEN)
        not_seen_block_hash = BtcObjectHash(buf=helpers.generate_bytearray(BTC_SHA_HASH_LEN), length=BTC_SHA_HASH_LEN)
        self.node.blocks_seen.add(seen_block_hash)

        inv_message = InvBtcMessage(magic=123, inv_vects=[
            (InventoryType.MSG_TX, seen_block_hash),
            (InventoryType.MSG_BLOCK, not_seen_block_hash),
            (InventoryType.MSG_BLOCK, seen_block_hash)
        ])
        self.sut.msg_inv(inv_message)

        get_data_msg_bytes = self.sut.connection.get_bytes_to_send()
        get_data_msg = GetDataBtcMessage(buf=get_data_msg_bytes)
        self.assertEqual(2, get_data_msg.count())
        self.assertIn((InventoryType.MSG_TX, seen_block_hash), get_data_msg)
        self.assertIn((InventoryType.MSG_BLOCK, not_seen_block_hash), get_data_msg)

    def test_get_data_segwit(self):
        self._test_get_data(True)

    def test_get_data_non_segwit(self):
        self._test_get_data(False)

    def _test_get_data(self, segwit):
        self.sut.request_witness_data = segwit

        inv_msg = self._create_inv_msg()
        self.sut.msg_inv(inv_msg)

        get_data_msg_bytes = self.sut.connection.get_bytes_to_send()
        get_data_msg = GetDataBtcMessage(buf=get_data_msg_bytes)
        self.assertEqual(2, get_data_msg.count())

        item_index = 0
        for inv_item in get_data_msg:
            if (item_index == 0):
                self.assertEqual(InventoryType.MSG_WITNESS_TX if segwit else InventoryType.MSG_TX, inv_item[0])
                self.assertEqual(self.tx_hash, inv_item[1])
            else:
                self.assertEqual(InventoryType.MSG_WITNESS_BLOCK if segwit else InventoryType.MSG_BLOCK, inv_item[0])
                self.assertEqual(self.block_hash, inv_item[1])

            item_index += 1

    def _create_version_msg(self, service):
        return VersionBtcMessage(magic=123, version=234, dst_ip=LOCALHOST, dst_port=12345, src_ip=LOCALHOST,
                                 src_port=12345, nonce=1, start_height=0, user_agent=b"dummy_user_agent",
                                 services=service)

    def _create_inv_msg(self):
        return InvBtcMessage(magic=123, inv_vects=[(InventoryType.MSG_TX, self.tx_hash),
                                                   (InventoryType.MSG_BLOCK, self.block_hash)])
