import datetime
import time
from typing import Tuple, Optional, List, cast

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils import convert
from bxcommon.utils.object_hash import AbstractObjectHash, Sha256Hash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.utils.block_info import BlockInfo


class BtcNoCompressionMessageConverter(AbstractMessageConverter):
    def tx_to_bx_txs(self, tx_msg, network_num):
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
            block_msg.block_hash(),
            convert.bytes_to_hex(block_msg.prev_block().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return block_msg.rawbytes(), block_info

    def bx_block_to_block(self, bx_block_msg, tx_service) -> Tuple[Optional[AbstractMessage], BlockInfo, List[int],
                                                                   List[Sha256Hash]]:
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
            block_msg.block_hash(),
            convert.bytes_to_hex(block_msg.prev_block().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return block_msg, block_info, [], []
