from abc import abstractmethod

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.inventory_ont_message import InventoryOntType
from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService

from bxutils import logging

logger = logging.get_logger(__name__)


class AbstractOntBlockCleanupService(AbstractBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    def __init__(self, node: "OntGatewayNode", network_num: int):
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        super(AbstractOntBlockCleanupService, self).__init__(node=node, network_num=network_num)

    def block_cleanup_request(self, block_hash: Sha256Hash) -> None:
        if not self.is_marked_for_cleanup(block_hash):
            self._block_hash_marked_for_cleanup.add(block_hash)
            self.last_confirmed_block = block_hash
            block_request_message = GetDataOntMessage(
                magic=self.node.opts.blockchain_net_magic,
                inv_type=InventoryOntType.MSG_BLOCK.value,
                block=block_hash
            )
            self.node.send_msg_to_node(block_request_message)
            logger.trace("Received block cleanup request: {}", block_hash)

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg: BlockOntMessage,
            transaction_service: TransactionService
    ) -> None:
        pass
