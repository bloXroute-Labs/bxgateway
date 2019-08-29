from typing import Set, List, Optional, Iterable, TYPE_CHECKING
import typing
from abc import abstractmethod

from bxcommon.utils import logger
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage


class AbstractEthBlockCleanupService(AbstractBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "EthGatewayNode", network_num: int):
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        super(AbstractEthBlockCleanupService, self).__init__(node=node, network_num=network_num)

    def block_cleanup_request(self, block_hash: Sha256Hash) -> None:
        if not self.is_marked_for_cleanup(block_hash):
            self._block_hash_marked_for_cleanup.add(block_hash)
            self.last_confirmed_block = block_hash
            if self.node.node_conn is not None:
                connection_protocol =\
                    typing.cast("bxgateway.connections.eth.eth_node_connection_protocol.EthNodeConnectionProtocol",
                                self.node.node_conn.connection_protocol)
                connection_protocol.request_block_body([block_hash])
                logger.debug("BlockCleanupRequest BlockHash: {}", repr(block_hash))
            else:
                logger.warn("BlockCleanup for BlockHash: {} Failed, no connection to node", repr(block_hash))

    def clean_block_transactions(
            self,
            block_msg: NewBlockEthProtocolMessage,
            transaction_service: TransactionService
    ) -> None:
        block_hash = block_msg.block_hash()
        transactions_list = block_msg.txns()
        self.clean_block_transactions_by_block_components(
            block_hash=block_hash,
            transactions_list=(tx.hash() for tx in transactions_list),
            transaction_service=transaction_service
        )

    @abstractmethod
    def clean_block_transactions_by_block_components(
            self,
            block_hash: Sha256Hash,
            transactions_list: Iterable[Sha256Hash],
            transaction_service: TransactionService
         ) -> None:
        pass
