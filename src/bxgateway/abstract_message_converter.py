from abc import ABCMeta, abstractmethod
from typing import Tuple, Optional, List, Set

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.memory_utils import SpecialMemoryProperties, SpecialTuple
from bxcommon.utils import memory_utils

from bxgateway.utils.block_info import BlockInfo


class AbstractMessageConverter(SpecialMemoryProperties, metaclass=ABCMeta):
    """
    Message converter abstract class.

    Converts messages of specific blockchain protocol to internal messages
    """

    @abstractmethod
    def tx_to_bx_txs(self, tx_msg, network_num):
        """
        Converts blockchain transactions message to internal transaction message

        :param tx_msg: blockchain transactions message
        :param network_num: blockchain network number
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

    def special_memory_size(self, ids: Optional[Set[int]] = None) -> SpecialTuple:
        obj_size = memory_utils.get_object_size(self)
        return SpecialTuple(obj_size.size, ids)
