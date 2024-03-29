# pyre-ignore-all-errors
import datetime
from typing import Tuple, Optional, Union

from bxcommon.messages.bloxroute.block_hash_message import BlockHashMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.test_utils import helpers
from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon import constants as common_constants

from bxgateway.abstract_message_converter import AbstractMessageConverter, BlockDecompressionResult
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.utils.block_info import BlockInfo


class MockBlockMessage(BlockHashMessage):
    MESSAGE_TYPE = b"mockblock"


class MockMessageConverter(AbstractMessageConverter):

    PREV_BLOCK = Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN))

    def tx_to_bx_txs(
        self,
        tx_msg,
        network_num: int,
        transaction_flag: Optional[TransactionFlag] = None,
        min_tx_network_fee: int = 0,
        account_id: str = common_constants.DECODED_EMPTY_ACCOUNT_ID
    ):
        return [(tx_msg, tx_msg.tx_hash(), tx_msg.tx_val(), transaction_flag)]

    def bx_tx_to_tx(self, bx_tx_msg):
        return bx_tx_msg

    def block_to_bx_block(
        self, block_msg, tx_service, enable_block_compression: bool, min_tx_age_seconds: float
    ) -> Tuple[memoryview, BlockInfo]:
        return block_msg.rawbytes(), \
               BlockInfo(convert.bytes_to_hex(self.PREV_BLOCK.binary), [], datetime.datetime.utcnow(),
                         datetime.datetime.utcnow(), 0, 0, None, None, 0, 0, 0, [])

    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        block_message = MockBlockMessage(buf=bx_block_msg)
        return BlockDecompressionResult(block_message, block_message.block_hash(), [], [])

    def bdn_tx_to_bx_tx(
        self,
        raw_tx: Union[bytes, bytearray, memoryview],
        network_num: int,
        transaction_flag: Optional[TransactionFlag] = None,
        account_id: str = common_constants.DECODED_EMPTY_ACCOUNT_ID
    ) -> TxMessage:
        return TxMessage(
            Sha256Hash(crypto.double_sha256(raw_tx)),
            network_num,
            tx_val=raw_tx,
            transaction_flag=transaction_flag,
            account_id=account_id
        )


class MockBlockchainConnection(AbstractGatewayBlockchainConnection):
    def __init__(self, sock, node):
        super(MockBlockchainConnection, self).__init__(sock, node)
