import time
from typing import List

from mock import MagicMock

from bxgateway.testing.abstract_btc_gateway_integration_test import AbstractBtcGatewayIntegrationTest

from bxcommon.constants import LOCALHOST
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.messages.bloxroute.broadcast_message import BroadcastMessage
from bxcommon.messages.bloxroute.get_txs_message import GetTxsMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.transaction_info import TransactionInfo
from bxcommon.test_utils import helpers
from bxcommon.utils import crypto
from bxcommon.utils.alarm_queue import AlarmQueue

from bxgateway import btc_constants, gateway_constants
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc import btc_message_converter_factory
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BlockRecoveryTest(AbstractBtcGatewayIntegrationTest):
    TRANSACTIONS_COUNT = 10

    def setUp(self):
        super().setUp()

        self.node1.alarm_queue = AlarmQueue()
        self.node2.alarm_queue = AlarmQueue()

        self.network_num = 1
        self.magic = 12345
        self.version = 23456
        self.prev_block_hash = bytearray(crypto.double_sha256(b"123"))
        self.prev_block = BtcObjectHash(self.prev_block_hash, length=crypto.SHA256_HASH_LEN)
        self.merkle_root_hash = bytearray(crypto.double_sha256(b"234"))
        self.merkle_root = BtcObjectHash(self.merkle_root_hash, length=crypto.SHA256_HASH_LEN)
        self.bits = 2
        self.nonce = 3

        opts = self.gateway_1_opts()
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        self.btc_message_converter = btc_message_converter_factory.create_btc_message_converter(self.magic, opts)

        self.btc_transactions = [
            TxBtcMessage(self.magic, self.version, [], [], i)
            for i in range(self.TRANSACTIONS_COUNT)
        ]
        self.btc_transactions_for_block = [
            tx_btc_message.rawbytes()[btc_constants.BTC_HDR_COMMON_OFF:]
            for tx_btc_message in self.btc_transactions
        ]
        self.transactions = [
            self.btc_message_converter.tx_to_bx_txs(tx_btc_message, self.network_num)[0][0]
            for tx_btc_message in self.btc_transactions
        ]
        self.transactions_with_short_ids = [
            TxMessage(tx_message.tx_hash(), tx_message.network_num(), "", i + 1, tx_message.tx_val())
            for i, tx_message in enumerate(self.transactions)
        ]
        self.transactions_with_no_content = [
            TxMessage(tx_message.tx_hash(), tx_message.network_num(), "", i + 1)
            for i, tx_message in enumerate(self.transactions)
        ]
        self.transactions_by_short_id = {tx_message.short_id(): tx_message
                                         for tx_message in self.transactions_with_short_ids}
        self.block = BlockBtcMessage(self.magic, self.version, self.prev_block, self.merkle_root, int(time.time()),
                                     self.bits, self.nonce, self.btc_transactions_for_block)

    def gateway_1_opts(self):
        return helpers.get_gateway_opts(9000, peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                        include_default_btc_args=True, encrypt_blocks=False)

    def gateway_2_opts(self):
        return helpers.get_gateway_opts(9001, peer_gateways=[OutboundPeerModel(LOCALHOST, 7002)],
                                        include_default_btc_args=True, encrypt_blocks=False)

    def _send_compressed_block_to_node_1(self):
        for tx_message_with_short_id in self.transactions_with_short_ids:
            helpers.receive_node_message(self.node2, self.relay_fileno, tx_message_with_short_id.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        helpers.receive_node_message(self.node2, self.blockchain_fileno, self.block.rawbytes())

        _block_hold_bytes = helpers.get_queued_node_bytes(self.node2, self.relay_fileno,
                                                          BlockHoldingMessage.MESSAGE_TYPE)
        broadcast_bytes = helpers.get_queued_node_bytes(self.node2, self.relay_fileno, BroadcastMessage.MESSAGE_TYPE)

        helpers.receive_node_message(self.node1, self.relay_fileno, broadcast_bytes)

        get_txs_bytes = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message = GetTxsMessage(buf=get_txs_bytes.tobytes())

        self._assert_no_block_node_1()
        return get_txs_message

    def _assert_no_block_node_1(self):
        empty_bytes = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertEqual(0, len(empty_bytes))

    def _assert_sent_block_node_1(self):
        found_block = False

        bytes_to_send = self.node1.get_bytes_to_send(self.blockchain_fileno)
        while bytes_to_send:
            found_block |= BlockBtcMessage.MESSAGE_TYPE in bytes_to_send.tobytes()
            if found_block:
                break
            self.node1.on_bytes_sent(self.blockchain_fileno, len(bytes_to_send))
            bytes_to_send = self.node1.get_bytes_to_send(self.blockchain_fileno)
        self.assertTrue(found_block)
        self.assertEqual(self.block.rawbytes(), bytes_to_send)

    def _build_txs_message(self, short_ids: List[int]):
        txs: List[TransactionInfo] = []
        for short_id in short_ids:
            transaction = self.transactions_by_short_id[short_id]
            txs.append(TransactionInfo(transaction.tx_hash(), transaction.tx_val(), transaction.short_id()))
        return TxsMessage(txs)

    def test_recover_all_short_ids(self):
        for tx_message in self.transactions:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        get_txs_message = self._send_compressed_block_to_node_1()

        self.assertEqual(10, len(get_txs_message.get_short_ids()))
        txs_message = self._build_txs_message(get_txs_message.get_short_ids())

        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())
        self._assert_sent_block_node_1()

    def test_recover_missing_tx_contents(self):
        for tx_message in self.transactions_with_no_content[:3]:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        for tx_message in self.transactions[3:]:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        get_txs_message = self._send_compressed_block_to_node_1()

        self.assertEqual(10, len(get_txs_message.get_short_ids()))
        txs_message = self._build_txs_message(get_txs_message.get_short_ids())

        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())
        self._assert_sent_block_node_1()

    def test_recover_multiple_iterations(self):
        for tx_message in self.transactions:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        get_txs_message = self._send_compressed_block_to_node_1()
        txs_message = self._build_txs_message(get_txs_message.get_short_ids()[:5])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()
        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[0])
        self.node1.alarm_queue.fire_alarms()

        get_txs_bytes_2 = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message_2 = GetTxsMessage(buf=get_txs_bytes_2.tobytes())
        self.assertEqual(5, len(get_txs_message_2.get_short_ids()))

        txs_message = self._build_txs_message(get_txs_message_2.get_short_ids()[:-1])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        # retry again in a longer interval
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[0])
        self.node1.alarm_queue.fire_alarms()

        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[1])
        self.node1.alarm_queue.fire_alarms()

        get_txs_bytes_3 = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message_3 = GetTxsMessage(buf=get_txs_bytes_3.tobytes())
        self.assertEqual(1, len(get_txs_message_3.get_short_ids()))

        txs_message = self._build_txs_message(get_txs_message_3.get_short_ids())
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        self._assert_sent_block_node_1()

    def test_recover_give_up(self):
        gateway_constants.BLOCK_RECOVERY_MAX_RETRY_ATTEMPTS = 3

        for tx_message in self.transactions:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        self._send_compressed_block_to_node_1()
        txs_message = self._build_txs_message([])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        # retry, first attempt
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[0])
        self.node1.alarm_queue.fire_alarms()

        get_txs_bytes_2 = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message_2 = GetTxsMessage(buf=get_txs_bytes_2.tobytes())
        self.assertEqual(10, len(get_txs_message_2.get_short_ids()))

        txs_message = self._build_txs_message([])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        # retry, attempt 2
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[1])
        self.node1.alarm_queue.fire_alarms()

        get_txs_bytes_3 = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message_3 = GetTxsMessage(buf=get_txs_bytes_3.tobytes())
        self.assertEqual(10, len(get_txs_message_3.get_short_ids()))

        txs_message = self._build_txs_message([])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        # retry, attempt 3
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[2])
        self.node1.alarm_queue.fire_alarms()

        get_txs_bytes_4 = helpers.get_queued_node_bytes(self.node1, self.relay_fileno, GetTxsMessage.MESSAGE_TYPE)
        get_txs_message_4 = GetTxsMessage(buf=get_txs_bytes_4.tobytes())
        self.assertEqual(10, len(get_txs_message_4.get_short_ids()))

        txs_message = self._build_txs_message([])
        helpers.receive_node_message(self.node1, self.relay_fileno, txs_message.rawbytes())

        # retry given up
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

        time.time = MagicMock(return_value=time.time() + gateway_constants.BLOCK_RECOVERY_RECOVERY_INTERVAL_S[3] * 20)
        self.node1.alarm_queue.fire_alarms()

        # no bytes, even after timeout
        bytes_to_send = self.node1.get_bytes_to_send(self.relay_fileno)
        self.assertEqual(0, len(bytes_to_send))
        self._assert_no_block_node_1()

    def test_recover_from_single_tx_short_id(self):
        for tx_message in self.transactions_with_short_ids[:-1]:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        _get_txs_message = self._send_compressed_block_to_node_1()

        helpers.receive_node_message(self.node1, self.relay_fileno, self.transactions_with_short_ids[-1].rawbytes())
        self._assert_sent_block_node_1()

    def test_recover_from_single_tx_short_id_no_content(self):
        for tx_message in self.transactions_with_short_ids[:-1]:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.receive_node_message(self.node1, self.relay_fileno, self.transactions[-1].rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        _get_txs_message = self._send_compressed_block_to_node_1()

        helpers.receive_node_message(self.node1, self.relay_fileno, self.transactions_with_short_ids[-1].rawbytes())
        self._assert_sent_block_node_1()

    def test_recover_from_single_tx_val(self):
        for tx_message in self.transactions_with_short_ids[:-1]:
            helpers.receive_node_message(self.node1, self.relay_fileno, tx_message.rawbytes())
        helpers.receive_node_message(self.node1, self.relay_fileno, self.transactions_with_no_content[-1].rawbytes())
        helpers.clear_node_buffer(self.node1, self.blockchain_fileno)

        _get_txs_message = self._send_compressed_block_to_node_1()

        helpers.receive_node_message(self.node1, self.relay_fileno, self.transactions_with_short_ids[-1].rawbytes())
        self._assert_sent_block_node_1()
