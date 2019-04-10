from abc import ABCMeta, abstractmethod


class AbstractMessageConverter(object):
    """
    Message converter abstract class.

    Converts messages of specific blockchain protocol to internal messages
    """

    __metaclass__ = ABCMeta

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
    def block_to_bx_block(self, block_msg, tx_service):
        """
        Convert blockchain block message to internal broadcast message with transactions replaced with short ids

        :param block_msg: blockchain new block message
        :param tx_service: Transactions service
        :return: Internal broadcast message bytes (bytearray), tuple (txs count, previous block hash, short ids)
        """

        pass

    @abstractmethod
    def bx_block_to_block(self, bx_block_msg, tx_service):
        """
        Converts internal broadcast message to blockchain new block message

        Returns None for block message if any of the transactions shorts ids or hashes are unknown

        :param bx_block_msg: internal broadcast message bytes
        :param tx_service: Transactions service
        :return: tuple (new block message, block info, unknown transaction short ids, unknown transaction hashes)
        """

        pass
