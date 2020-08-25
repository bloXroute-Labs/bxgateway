from abc import abstractmethod
from typing import TYPE_CHECKING

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService

if TYPE_CHECKING:
    from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode

from bxutils import logging

logger = logging.get_logger(__name__)


class AbstractOntBlockCleanupService(AbstractBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "OntGatewayNode", network_num: int) -> None:
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        super(AbstractOntBlockCleanupService, self).__init__(node=node, network_num=network_num)

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg: BlockOntMessage,
            transaction_service: TransactionService
    ) -> None:
        pass

    def clean_block_transactions_from_block_queue(
            self,
            block_hash: Sha256Hash
    ) -> None:
        if block_hash in self.node.block_queuing_service._blocks:
            block_msg = self.node.block_queuing_service._blocks[block_hash]
            self.node.block_cleanup_service.clean_block_transactions(
                transaction_service=self.node.get_tx_service(),
                block_msg=block_msg
            )
        else:
            logger.debug("block cleanup from queuing service failed, block is no longer tracked {}", block_hash)

    def _request_block(self, block_hash: Sha256Hash) -> None:
        block_request_message = GetDataOntMessage(
            magic=self.node.opts.blockchain_net_magic,
            inv_type=InventoryOntType.MSG_BLOCK.value,
            # pyre-fixme[6]: Expected `[OntObjectHash]` for 3rd parameter but got `Sha256Hash`
            block=block_hash
        )
        self.node.send_msg_to_node(block_request_message)
        logger.trace("Received block cleanup request: {}", block_hash)
