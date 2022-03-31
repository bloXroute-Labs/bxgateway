from bxcommon import constants
from bxcommon.constants import MSG_TYPE_LEN
from bxcommon.messages.bloxroute.block_holding_message import BlockHoldingMessage
from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.message_factory_test_case import MessageFactoryTestCase
from bxcommon.utils import crypto
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import BtcObjectHash
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.btc.data_btc_message import GetBlocksBtcMessage
from bxgateway.messages.gateway.block_propagation_request import BlockPropagationRequestMessage
from bxgateway.messages.gateway.block_received_message import BlockReceivedMessage
from bxgateway.messages.gateway.blockchain_sync_request_message import BlockchainSyncRequestMessage
from bxgateway.messages.gateway.blockchain_sync_response_message import (
    BlockchainSyncResponseMessage,
)
from bxgateway.messages.gateway.confirmed_block_message import ConfirmedBlockMessage
from bxgateway.messages.gateway.confirmed_tx_message import ConfirmedTxMessage
from bxgateway.messages.gateway.gateway_hello_message import GatewayHelloMessage
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxgateway.messages.gateway.request_tx_stream_message import RequestTxStreamMessage


class GatewayMessageFactoryTest(MessageFactoryTestCase):
    HASH = Sha256Hash(crypto.double_sha256(b"123"))
    TX_VAL = helpers.generate_bytearray(250)
    BLOCK_VAL = helpers.generate_bytearray(1000)
    BLOCK = helpers.generate_bytearray(29) + b"\x01"

    def get_message_factory(self):
        return gateway_message_factory

    def test_message_preview_success_all_gateway_types(self):
        self.get_message_preview_successfully(
            GatewayHelloMessage(123, 1, "127.0.0.1", 40000, 1),
            GatewayMessageType.HELLO,
            GatewayHelloMessage.PAYLOAD_LENGTH,
        )
        self.get_message_preview_successfully(
            BlockReceivedMessage(self.HASH),
            GatewayMessageType.BLOCK_RECEIVED,
            BlockReceivedMessage.PAYLOAD_LENGTH,
        )
        self.get_message_preview_successfully(
            BlockPropagationRequestMessage(self.BLOCK),
            GatewayMessageType.BLOCK_PROPAGATION_REQUEST,
            len(self.BLOCK) + constants.CONTROL_FLAGS_LEN,
        )
        self.get_message_preview_successfully(
            BlockHoldingMessage(self.HASH, network_num=123),
            BloxrouteMessageType.BLOCK_HOLDING,
            BlockHoldingMessage.PAYLOAD_LENGTH,
        )
        self.get_message_preview_successfully(
            ConfirmedTxMessage(self.HASH, self.TX_VAL),
            GatewayMessageType.CONFIRMED_TX,
            ConfirmedTxMessage.PAYLOAD_LENGTH + len(self.TX_VAL),
        )
        self.get_message_preview_successfully(
            ConfirmedBlockMessage(self.HASH, self.BLOCK_VAL),
            GatewayMessageType.CONFIRMED_BLOCK,
            ConfirmedBlockMessage.PAYLOAD_LENGTH + len(self.BLOCK_VAL),
        )
        self.get_message_preview_successfully(
            RequestTxStreamMessage(),
            GatewayMessageType.REQUEST_TX_STREAM,
            constants.CONTROL_FLAGS_LEN,
        )

        hash_val = BtcObjectHash(buf=crypto.double_sha256(b"123"), length=crypto.SHA256_HASH_LEN)
        blockchain_message = GetBlocksBtcMessage(12345, 23456, [hash_val], hash_val).rawbytes()
        self.get_message_preview_successfully(
            BlockchainSyncRequestMessage(GetBlocksBtcMessage.MESSAGE_TYPE, blockchain_message),
            BlockchainSyncRequestMessage.MESSAGE_TYPE,
            MSG_TYPE_LEN + len(blockchain_message),
        )
        self.get_message_preview_successfully(
            BlockchainSyncResponseMessage(GetBlocksBtcMessage.MESSAGE_TYPE, blockchain_message),
            BlockchainSyncResponseMessage.MESSAGE_TYPE,
            MSG_TYPE_LEN + len(blockchain_message),
        )

    def test_create_message_success_all_gateway_types(self):
        hello_message = self.create_message_successfully(
            GatewayHelloMessage(123, 1, "127.0.0.1", 40001, 1), GatewayHelloMessage
        )
        self.assertEqual(123, hello_message.protocol_version())
        self.assertEqual(1, hello_message.network_num())
        self.assertEqual("127.0.0.1", hello_message.ip())
        self.assertEqual(40001, hello_message.port())
        self.assertEqual(1, hello_message.ordering())

        block_recv_message = self.create_message_successfully(
            BlockReceivedMessage(self.HASH), BlockReceivedMessage
        )
        self.assertEqual(self.HASH, block_recv_message.block_hash())

        block_holding_message = self.create_message_successfully(
            BlockHoldingMessage(self.HASH, network_num=123), BlockHoldingMessage
        )
        self.assertEqual(self.HASH, block_holding_message.block_hash())

        block_propagation_request_message = self.create_message_successfully(
            BlockPropagationRequestMessage(self.BLOCK), BlockPropagationRequestMessage
        )
        self.assertEqual(self.BLOCK, block_propagation_request_message.blob())

        hash_val = BtcObjectHash(buf=crypto.double_sha256(b"123"), length=crypto.SHA256_HASH_LEN)
        blockchain_message_in = GetBlocksBtcMessage(12345, 23456, [hash_val], hash_val).rawbytes()
        sync_request_message = self.create_message_successfully(
            BlockchainSyncRequestMessage(GetBlocksBtcMessage.MESSAGE_TYPE, blockchain_message_in),
            BlockchainSyncRequestMessage,
        )
        blockchain_message_out = GetBlocksBtcMessage(buf=sync_request_message.payload())
        self.assertEqual(12345, blockchain_message_out.magic())
        self.assertEqual(23456, blockchain_message_out.version())
        self.assertEqual(1, blockchain_message_out.hash_count())
        self.assertEqual(hash_val, blockchain_message_out.hash_stop())

        self.create_message_successfully(
            BlockchainSyncResponseMessage(GetBlocksBtcMessage.MESSAGE_TYPE, blockchain_message_in),
            BlockchainSyncResponseMessage,
        )

        blockchain_message_out = GetBlocksBtcMessage(buf=sync_request_message.payload())
        self.assertEqual(12345, blockchain_message_out.magic())
        self.assertEqual(23456, blockchain_message_out.version())
        self.assertEqual(1, blockchain_message_out.hash_count())
        self.assertEqual(hash_val, blockchain_message_out.hash_stop())

        confirmed_tx = self.create_message_successfully(
            ConfirmedTxMessage(self.HASH, self.TX_VAL), ConfirmedTxMessage
        )
        self.assertEqual(self.HASH, confirmed_tx.tx_hash())
        self.assertEqual(self.TX_VAL, confirmed_tx.tx_val())

        confirmed_tx_no_content = self.create_message_successfully(
            ConfirmedTxMessage(self.HASH), ConfirmedTxMessage
        )
        self.assertEqual(self.HASH, confirmed_tx_no_content.tx_hash())
        self.assertEqual(TxMessage.EMPTY_TX_VAL, confirmed_tx_no_content.tx_val())

        confirmed_block: ConfirmedBlockMessage = self.create_message_successfully(
            ConfirmedBlockMessage(self.HASH, self.BLOCK_VAL), ConfirmedBlockMessage
        )
        self.assertEqual(self.HASH, confirmed_block.block_hash())
        self.assertEqual(self.BLOCK_VAL, confirmed_block.block_content())

        self.create_message_successfully(
            RequestTxStreamMessage(), RequestTxStreamMessage
        )
