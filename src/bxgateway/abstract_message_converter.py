from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional, List, Set, Union

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.memory_utils import SpecialMemoryProperties, SpecialTuple

from bxgateway.utils.block_info import BlockInfo


class AbstractMessageConverter(SpecialMemoryProperties, metaclass=ABCMeta):
    """
    Message converter abstract class.

    Converts messages of specific blockchain protocol to internal messages
    """

    @abstractmethod
    def tx_to_bx_txs(self, tx_msg, network_num, quota_type: Optional[QuotaType] = None):
        """
        Converts blockchain transactions message to internal transaction message

        :param tx_msg: blockchain transactions message
        :param network_num: blockchain network number
        :param quota_type: the quota type to assign to the BDN transaction.
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
    def block_to_bx_block(self, block_msg, tx_service) -> Tuple[memoryview, BlockInfo]:
        """
        Convert blockchain block message to internal broadcast message with transactions replaced with short ids

        :param block_msg: blockchain new block message
        :param tx_service: Transactions service
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash, short ids)
        """

        pass

    @abstractmethod
    def bx_block_to_block(self, bx_block_msg, tx_service) -> Tuple[Optional[AbstractMessage], BlockInfo, List[int],
                                                                   List[Sha256Hash]]:
        """
        Converts internal broadcast message to blockchain new block message

        Returns None for block message if any of the transactions shorts ids or hashes are unknown

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: tuple (new block message, block info, unknown transaction short ids, unknown transaction hashes)
        """

        pass

    @abstractmethod
    def bdn_tx_to_bx_tx(
            self,
            raw_tx: Union[bytes, bytearray, memoryview],
            network_num: int,
            quota_type: Optional[QuotaType] = None
    ) -> TxMessage:
        """
        Convert a raw transaction which arrived from an RPC request into bx transaction.
        :param raw_tx: The raw transaction bytes.
        :param network_num: the network number.
        :param quota_type: the quota type to assign to the BDN transaction.
        :return: bx transaction.
        """
        pass

    @abstractmethod
    def encode_raw_msg(self, raw_msg: str) -> bytes:
        """
        Encode a raw message string into bytes
        :param raw_msg: the raw message to encode
        :return: binary encoded message
        :raise ValueError: if the encoding fails
        """
        pass

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        return super(AbstractMessageConverter, self).special_memory_size(ids)
