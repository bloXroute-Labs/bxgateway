import datetime
import time
from typing import Tuple, Optional, List, cast

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter, BlockDecompressionResult
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.utils.block_info import BlockInfo


class BtcNoCompressionMessageConverter(AbstractMessageConverter):
    def tx_to_bx_txs(self, tx_msg, network_num, quota_type: Optional[QuotaType] = None):
        raise NotImplementedError()

    def bx_tx_to_tx(self, bx_tx_msg):
        raise NotImplementedError()

    def block_to_bx_block(self, block_msg, tx_service) -> Tuple[memoryview, BlockInfo]:
        start_datetime = datetime.datetime.utcnow()
        start_time = time.time()

        block_msg = cast(BlockBtcMessage, block_msg)

        block_info = BlockInfo(
            block_msg.block_hash(),
            [],
            start_datetime,
            datetime.datetime.utcnow(),
            (time.time() - start_time) * 1000,
            block_msg.txn_count(),
            # pyre-fixme[6]: Expected `Optional[str]` for 7th param but got
            #  `BtcObjectHash`.
            block_msg.block_hash(),
            convert.bytes_to_hex(block_msg.prev_block_hash().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return block_msg.rawbytes(), block_info

    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        start_datetime = datetime.datetime.utcnow()
        start_time = time.time()
        block_msg = BlockBtcMessage(buf=bx_block_msg)

        block_info = BlockInfo(
            block_msg.block_hash(),
            [],
            start_datetime,
            datetime.datetime.utcnow(),
            (time.time() - start_time) * 1000,
            block_msg.txn_count(),
            # pyre-fixme[6]: Expected `Optional[str]` for 7th param but got
            #  `BtcObjectHash`.
            block_msg.block_hash(),
            convert.bytes_to_hex(block_msg.prev_block_hash().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return BlockDecompressionResult(block_msg, block_info, [], [])
