import struct

from mock import patch, MagicMock

from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest

from bxcommon import constants
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash

from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage, TxIn, TxOut
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxgateway.testing import spies


def get_object_hash(bytearr):
    return BtcObjectHash(bytearr, 0, 32)


# Transforms a bytearray with a BTCMessage into something else.
class MessageTransformations:
    def change_msgtype(self, msgbytes, newmsgtype):
        new_msgtype = bytearray(newmsgtype) + bytearray(12 - len(newmsgtype))
        to_return = bytearray(msgbytes)
        to_return[4:16] = new_msgtype
        return to_return

    def change_payloadlen(self, msgbytes, newpayloadlen):
        to_return = bytearray(msgbytes)
        struct.pack_into("<L", to_return, 16, newpayloadlen)
        return to_return

    def change_checksum(self, msgbytes, newchecksum):
        to_return = bytearray(msgbytes)
        struct.pack_into("<L", to_return, 20, newchecksum)
        return to_return

    def change_payload(self, msgbytes, newpayload):
        to_return = bytearray(msgbytes)[:24] + bytearray(newpayload)
        return to_return


@patch("bxcommon.constants.OUTPUT_BUFFER_MIN_SIZE", 0)
@patch("bxcommon.constants.OUTPUT_BUFFER_BATCH_MAX_HOLD_TIME", 0)
class TestBadSendingBtcTest(AbstractBtcGatewayIntegrationTest):
    def setUp(self):
        self.local_node_fileno = 1
        self.remote_node_fileno = 2

        self.gateway_node = spies.make_spy_node(BtcGatewayNode, 8000, include_default_btc_args=True)
        self.btc_node_connection = spies.make_spy_connection(BtcNodeConnection, self.local_node_fileno, 8001,
                                                             self.gateway_node)
        self.btc_remote_node_connection = spies.make_spy_connection(BtcRemoteConnection, self.remote_node_fileno, 8002,
                                                                    self.gateway_node)
        self.gateway_node.sdn_connection = MagicMock()
        self.gateway_node.node_conn = self.btc_node_connection
        self.gateway_node.remote_node_conn = self.btc_remote_node_connection
        self.gateway_node.connection_pool.add(self.local_node_fileno, LOCALHOST, 8001, self.btc_node_connection)
        self.gateway_node.connection_pool.add(self.remote_node_fileno, LOCALHOST, 8002, self.btc_remote_node_connection)
        self.gateway_node.account_id = "1234"

        self.handshake = \
            bytearray(VersionBtcMessage(100, 1000, "127.0.0.1", 8333, "0.0.0.0", 8333,
                                        13, 0, bytearray("hello", "utf-8"), 0).rawbytes()) + \
            bytearray(VerAckBtcMessage(100).rawbytes())

        inps = [TxIn(prev_outpoint_hash=bytearray("0" * 32, "utf-8"), prev_out_index=10, sig_script=bytearray(1000),
                     sequence=32)]
        outs = [TxOut(value=10, pk_script=bytearray(1000))]

        self.txmsg = bytearray(TxBtcMessage(magic=100, version=0, tx_in=inps, tx_out=outs, lock_time=12345).rawbytes())
        self.blockmsg = bytearray(BlockBtcMessage(magic=100, version=0, prev_block=get_object_hash(bytearray(32)),
                                                  merkle_root=get_object_hash(bytearray(32)), timestamp=1023, bits=1,
                                                  txns=[bytearray(64)] * 32000, nonce=34).rawbytes())
        self.transform = MessageTransformations()

    def test_null_bytes(self):
        bytes_to_send = bytearray(100)
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_connection_closed()

    def test_handshake_null_bytes(self):
        bytes_to_send = self.handshake + bytearray(100)
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_connection_closed()

    def test_bad_payload_len(self):
        bytes_to_send = self.handshake + self.transform.change_payloadlen(self.txmsg, 5901) + self.blockmsg
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_connection_closed()

    def test_bad_checksum(self):
        bytes_to_send = self.handshake + self.transform.change_checksum(self.txmsg, 1)
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_recovery_after_bad_message()

    def test_bad_payload(self):
        bytes_to_send = self.handshake + self.transform.change_payload(self.txmsg, bytearray(len(self.txmsg) - 24))
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_recovery_after_bad_message()

    def test_unknown_message(self):
        bytes_to_send = self.handshake + self.transform.change_msgtype(self.txmsg, "dummy_msg".encode("utf-8"))
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, bytes_to_send)
        self._verify_recovery_after_bad_message()

    def test_drops_connection_after_multiple_recoverable_bad_messages(self):
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, self.handshake)
        bytes_to_send = self.gateway_node.get_bytes_to_send(self.local_node_fileno)

        # send bad message
        for i in range(constants.MAX_BAD_MESSAGES):
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno,
                                         self.transform.change_msgtype(self.txmsg, "dummy_msg".encode("utf-8")))
        self._verify_recovery_after_bad_message()

        # send more bad messages
        for i in range(constants.MAX_BAD_MESSAGES + 1):
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno,
                                         self.transform.change_msgtype(self.txmsg, "dummy_msg".encode("utf-8")))
        self._verify_connection_closed()

    def _verify_recovery_after_bad_message(self):
        self.assertTrue(self.btc_node_connection.is_alive())

        gateway_bytes_to_send = self.gateway_node.get_bytes_to_send(self.local_node_fileno)
        self.assertTrue(len(gateway_bytes_to_send) > 0)
        self.gateway_node.on_bytes_sent(self.local_node_fileno, len(gateway_bytes_to_send))

        self.assertTrue(self.btc_node_connection.num_bad_messages > 0)
        helpers.receive_node_message(self.gateway_node, self.local_node_fileno, self.txmsg)
        self.assertEqual(0, self.btc_node_connection.num_bad_messages)

    def _verify_connection_closed(self):
        self.assertFalse(self.btc_node_connection.is_alive())
