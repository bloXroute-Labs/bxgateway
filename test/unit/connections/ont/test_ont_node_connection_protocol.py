from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils.blockchain_utils.ont.ont_object_hash import OntObjectHash

from bxgateway.connections.ont.ont_node_connection import OntNodeConnection
from bxgateway.connections.ont.ont_node_connection_protocol import OntNodeConnectionProtocol
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage, InventoryOntType
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage
from bxgateway.ont_constants import ONT_HASH_LEN
from bxgateway.testing.mocks.mock_ont_gateway_node import MockOntGatewayNode


class OntNodeConnectionProtocolTest(AbstractTestCase):

    def setUp(self):
        opts = gateway_helpers.get_gateway_opts(8000, include_default_ont_args=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.node = MockOntGatewayNode(opts)
        self.node.neutrality_service = MagicMock()

        self.connection = OntNodeConnection(
            MockSocketConnection(1, node=self.node, ip_address=LOCALHOST, port=123), self.node)
        gateway_helpers.add_blockchain_peer(self.node, self.connection)

        self.tx_hash = OntObjectHash(buf=helpers.generate_bytearray(32), length=ONT_HASH_LEN)
        self.block_hash = OntObjectHash(buf=helpers.generate_bytearray(32), length=ONT_HASH_LEN)

        self.sut = OntNodeConnectionProtocol(self.connection)

        while self.connection.outputbuf.length > 0:
            initial_bytes = self.connection.get_bytes_to_send()
            self.connection.advance_sent_bytes(len(initial_bytes))

    def test_version_and_verack(self):
        ver_msg = VersionOntMessage(123, 234, 20330, 20330, 20330, bytes(32), 123, 0,
                                    True, True, "v1.0.0".encode("utf-8"))
        self.sut.msg_version(ver_msg)

        verack_msg_bytes = self.sut.connection.get_bytes_to_send()
        verack_msg = VerAckOntMessage(buf=verack_msg_bytes)
        self.assertEqual(True, verack_msg.is_consensus())

    def test_inv_and_get_data(self):
        seen_block_hash = OntObjectHash(buf=helpers.generate_bytearray(ONT_HASH_LEN), length=ONT_HASH_LEN)
        not_seen_block_hash = OntObjectHash(buf=helpers.generate_bytearray(ONT_HASH_LEN), length=ONT_HASH_LEN)
        self.node.blocks_seen.add(seen_block_hash)

        inv_msg_seen_block = InvOntMessage(123, InventoryOntType.MSG_BLOCK, [seen_block_hash, seen_block_hash])
        self.sut.msg_inv(inv_msg_seen_block)
        get_data_msg_msg_seen_bytes = self.sut.connection.get_bytes_to_send()
        self.assertEqual(0, len(get_data_msg_msg_seen_bytes))

        inv_msg = InvOntMessage(123, InventoryOntType.MSG_BLOCK, [seen_block_hash, not_seen_block_hash])
        self.sut.msg_inv(inv_msg)
        get_data_msg_bytes = self.sut.connection.get_bytes_to_send()
        get_data_msg = GetDataOntMessage(buf=get_data_msg_bytes)
        data_msg_inv_type, data_msg_block = get_data_msg.inv_type()
        self.assertEqual(InventoryOntType.MSG_BLOCK.value, data_msg_inv_type)
        self.assertEqual(not_seen_block_hash, data_msg_block)



