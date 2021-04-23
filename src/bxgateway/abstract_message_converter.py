import struct
from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional, List, Set, Union, NamedTuple, Deque

from bxcommon import constants
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.utils.object_hash import Sha256Hash, convert
from bxcommon.utils.memory_utils import SpecialMemoryProperties, SpecialTuple

from bxgateway.utils.block_info import BlockInfo


class BlockDecompressionResult(NamedTuple):
    block_msg: Optional[AbstractBlockMessage]
    block_info: BlockInfo
    unknown_short_ids: List[int]
    unknown_tx_hashes: List[Sha256Hash]


def finalize_block_bytes(
        buf: Deque[Union[bytes, bytearray, memoryview]], size: int, short_ids: List[int]
) -> memoryview:
    serialized_short_ids = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(short_ids)
    buf.append(serialized_short_ids)
    size += constants.UL_ULL_SIZE_IN_BYTES
    offset_buf = struct.pack("<Q", size)
    buf.appendleft(offset_buf)
    size += len(serialized_short_ids)

    block = bytearray(size)
    off = 0
    for blob in buf:
        next_off = off + len(blob)
        block[off:next_off] = blob
        off = next_off

    return memoryview(block)


class AbstractMessageConverter(SpecialMemoryProperties, metaclass=ABCMeta):
    """
    Message converter abstract class.

    Converts messages of specific blockchain protocol to internal messages
    """

    @abstractmethod
    def tx_to_bx_txs(
        self,
        tx_msg,
        network_num: int,
        transaction_flag: Optional[TransactionFlag] = None,
        min_tx_network_fee: int = 0,
        account_id: str = constants.DECODED_EMPTY_ACCOUNT_ID
    ) -> List[Tuple[TxMessage, Sha256Hash, Union[bytearray, memoryview]]]:
        """
        Converts blockchain transactions message to internal transaction message

        :param tx_msg: blockchain transactions message
        :param network_num: blockchain network number
        :param transaction_flag: the transaction_flag type to assign to the BDN transaction.
        :param min_tx_network_fee: minimum transaction fee. If support by the network, transactions
                                   with fees lower than this will be excluded from the result
        :param account_id: gateway's account id
        :return: array of tuples (transaction message, transaction hash, transaction bytes)
        """

        pass

    @abstractmethod
    def bx_tx_to_tx(self, bx_tx_msg):
        """
        Converts internal transaction message to blockchain transactions message

        :param bx_tx_msg: internal transaction message
        :return: blockchain transactions message
        """

        pass

    @abstractmethod
    def block_to_bx_block(
        self, block_msg, tx_service, enable_block_compression: bool, min_tx_age_seconds: float
    ) -> Tuple[memoryview, BlockInfo]:
        """
        Convert blockchain block message to internal broadcast message with transactions replaced with short ids

        :param block_msg: blockchain new block message
        :param tx_service: Transactions service
        :param enable_block_compression
        :param min_tx_age_seconds
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash, short ids)
        """
        pass

    @abstractmethod
    def bx_block_to_block(self, bx_block_msg, tx_service) -> BlockDecompressionResult:
        """
        Converts internal broadcast message to blockchain new block message

        Returns None for block message if any of the transactions shorts ids or hashes are unknown

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: block decompression result
        """
        pass

    @abstractmethod
    def bdn_tx_to_bx_tx(
        self,
        raw_tx: Union[bytes, bytearray, memoryview],
        network_num: int,
        transaction_flag: Optional[TransactionFlag] = None,
        account_id: str = constants.DECODED_EMPTY_ACCOUNT_ID
    ) -> TxMessage:
        """
        Convert a raw transaction which arrived from an RPC request into bx transaction.
        :param raw_tx: The raw transaction bytes.
        :param network_num: the network number.
        :param transaction_flag: the quota type to assign to the BDN transaction.
        :param account_id: node's account id
        :return: bx transaction.
        """
        pass

    def encode_raw_msg(self, raw_msg: str) -> bytes:
        """
        Encode a raw message string into bytes
        :param raw_msg: the raw message to encode
        :return: binary encoded message
        :raise ValueError: if the encoding fails
        """
        return convert.hex_to_bytes(raw_msg)

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return super(AbstractMessageConverter, self).special_memory_size(ids)
