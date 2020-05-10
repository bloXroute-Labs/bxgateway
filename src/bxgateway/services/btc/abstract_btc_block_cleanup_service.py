from abc import abstractmethod

from bxutils import logging

from bxcommon.services.transaction_service import TransactionService
from bxcommon.utils.blockchain_utils.btc.btc_object_hash import Sha256Hash

from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType

logger = logging.get_logger(__name__)


class AbstractBtcBlockCleanupService(AbstractBlockCleanupService):
    """
    Service for managing block cleanup.
    """

    # pyre-fixme[11]: Annotation `BtcGatewayNode` is not defined as a type.
    def __init__(self, node: "BtcGatewayNode", network_num: int):
        """
        Constructor
        :param node: reference to node object
        :param network_num: network number
        """

        super(AbstractBtcBlockCleanupService, self).__init__(node=node, network_num=network_num)

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

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg: BlockBtcMessage,
            transaction_service: TransactionService
    ) -> None:
        pass

    def _request_block(self, block_hash: Sha256Hash):
        block_request_message = GetDataBtcMessage(
            magic=self.node.opts.blockchain_net_magic,
            inv_vects=[(InventoryType.MSG_BLOCK, block_hash)],
            request_witness_data=False
        )
        self.node.send_msg_to_node(block_request_message)
        logger.trace("Received block cleanup request: {}", block_hash)
