from datetime import datetime
import time

from typing import Tuple, Optional, List, cast

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxutils import logging

from bxgateway.messages.ont.abstract_ont_message_converter import AbstractOntMessageConverter
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.utils.block_info import BlockInfo


logger = logging.get_logger(__name__)


class OntNoCompressionMessageConverter(AbstractOntMessageConverter):

    def block_to_bx_block(self, block_msg: BlockOntMessage, tx_service: TransactionService) \
            -> Tuple[memoryview, BlockInfo]:
        start_datetime = datetime.utcnow()
        start_time = time.time()

        block_info = BlockInfo(
            block_msg.block_hash(),
            [],
            start_datetime,
            datetime.utcnow(),
            (time.time() - start_time) * 1000,
            block_msg.txn_count(),
            str(block_msg.block_hash()),
            convert.bytes_to_hex(block_msg.prev_block_hash().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return block_msg.rawbytes(), block_info

    def bx_block_to_block(self, bx_block_msg: memoryview, tx_service: TransactionService) \
            -> Tuple[Optional[AbstractMessage], BlockInfo, List[int], List[Sha256Hash]]:
        start_datetime = datetime.utcnow()
        start_time = time.time()

        block_msg = BlockOntMessage(buf=bytearray(bx_block_msg))

        block_info = BlockInfo(
            block_msg.block_hash(),
            [],
            start_datetime,
            datetime.utcnow(),
            (time.time() - start_time) * 1000,
            block_msg.txn_count(),
            str(block_msg.block_hash()),
            convert.bytes_to_hex(block_msg.prev_block_hash().binary),
            len(block_msg.rawbytes()),
            len(block_msg.rawbytes()),
            0
        )
        return block_msg, block_info, [], []
