import time
from abc import abstractmethod
from datetime import datetime
from typing import Optional, List, Tuple, Union

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway import ont_constants
from bxgateway.abstract_message_converter import AbstractMessageConverter, BlockDecompressionResult
from bxgateway.messages.ont import ont_messages_util
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.utils.block_info import BlockInfo


def get_block_info(
        bx_block: memoryview,
        block_hash: Sha256Hash,
        short_ids: List[int],
        decompress_start_datetime: datetime,
        decompress_start_timestamp: float,
        total_tx_count: Optional[int] = None,
        ont_block_msg: Optional[BlockOntMessage] = None
) -> BlockInfo:
    if ont_block_msg is not None:
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(bx_block))
        compressed_size = len(bx_block)
        prev_block_hash = convert.bytes_to_hex(ont_block_msg.prev_block_hash().binary)
        ont_block_len = len(ont_block_msg.rawbytes())
        compression_rate = 100 - float(compressed_size) / ont_block_len * 100
    else:
        bx_block_hash = None
        compressed_size = None
        prev_block_hash = None
        ont_block_len = None
        compression_rate = None
    return BlockInfo(
        block_hash,
        short_ids,
        decompress_start_datetime,
        datetime.utcnow(),
        (time.time() - decompress_start_timestamp) * 1000,
        total_tx_count,
        bx_block_hash,
        prev_block_hash,
        ont_block_len,
        compressed_size,
        compression_rate
    )


class AbstractOntMessageConverter(AbstractMessageConverter):

    def __init__(self, ont_magic: int):
        self._ont_magic = ont_magic

    @abstractmethod
    def block_to_bx_block(self, block_msg, tx_service) -> Tuple[memoryview, BlockInfo]:
        """
        Compresses a blockchain block's transactions and packs it into a bloXroute block.
        """
        pass

    @abstractmethod
    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw ONT bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        pass

    def bx_tx_to_tx(self, tx_msg: TxMessage):
        buf = bytearray(ont_constants.ONT_HDR_COMMON_OFF) + tx_msg.tx_val()
        raw_ont_tx_msg = OntMessage(self._ont_magic, TxOntMessage.MESSAGE_TYPE, len(tx_msg.tx_val()), buf)
        ont_tx_msg = TxOntMessage(buf=raw_ont_tx_msg.buf)

        return ont_tx_msg

    def tx_to_bx_txs(self, ont_tx_msg: TxOntMessage, network_num: int, quota_type: Optional[QuotaType] = None):
        tx_msg = TxMessage(ont_tx_msg.tx_hash(), network_num, tx_val=ont_tx_msg.tx(), quota_type=quota_type)

        return [(tx_msg, ont_tx_msg.tx_hash(), ont_tx_msg.tx())]

    def bdn_tx_to_bx_tx(
            self,
            raw_tx: Union[bytes, bytearray, memoryview],
            network_num: int,
            quota_type: Optional[QuotaType] = None
    ) -> TxMessage:
        if isinstance(raw_tx, bytes):
            raw_tx = bytearray(raw_tx)
        try:
            tx_hash, _ = ont_messages_util.get_txid(raw_tx)
        except IndexError:
            raise ValueError(f"Invalid raw transaction provided!")
        return TxMessage(
            message_hash=tx_hash, network_num=network_num, tx_val=raw_tx, quota_type=quota_type
        )
