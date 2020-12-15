import typing
from abc import abstractmethod
from typing import Iterable, TYPE_CHECKING

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.services.eth.eth_block_queuing_service import EthBlockQueuingService

from bxutils import logging
from bxutils.logging.log_record_type import LogRecordType

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(LogRecordType.BlockCleanup, __name__)


class AbstractEthBlockCleanupService(AbstractBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "EthGatewayNode", network_num: int) -> None:
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        super(AbstractEthBlockCleanupService, self).__init__(node=node, network_num=network_num)


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

    def clean_block_transactions_from_block_queue(
        self,
        block_hash: Sha256Hash,
        block_queuing_service: EthBlockQueuingService
    ) -> None:
        try:
            block_body = block_queuing_service.get_block_body_from_message(block_hash)
            assert block_body is not None
            transactions_hashes = block_body.get_block_transaction_hashes(0)
            self.clean_block_transactions_by_block_components(
                transaction_service=self.node.get_tx_service(),
                block_hash=block_hash,
                transactions_list=transactions_hashes
            )
        except Exception as error:
            logger.error("clean_block_transactions_from_block_queue operation for block {} failed with error {}.",
                         block_hash, error)
            block_body_parts = block_queuing_service.get_block_parts(block_hash)
            logger.debug("Failed block body bytes: {}",
                         "Empty" if block_body_parts is None
                         else convert.bytes_to_hex(block_body_parts.block_body_bytes))

    @abstractmethod
    def clean_block_transactions_by_block_components(
        self,
        block_hash: Sha256Hash,
        transactions_list: Iterable[Sha256Hash],
        transaction_service: TransactionService
    ) -> None:
        pass

    def _request_block(self, block_hash: Sha256Hash) -> None:
        node_conn = typing.cast("bxgateway.connections.eth.eth_node_connection.EthNodeConnection",
                                self.node.get_any_active_blockchain_connection())
        if node_conn is None:
            logger.debug("Request for block '{}' failed. No connection to node.", repr(block_hash))
            return
        connection_protocol = node_conn.connection_protocol
        connection_protocol.request_block_body([block_hash])
        logger.trace("Block cleanup request for {}", block_hash)
