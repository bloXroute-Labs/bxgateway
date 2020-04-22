from abc import abstractmethod

from bxutils import logging

from bxcommon.services.transaction_service import TransactionService

from bxgateway.services.abstract_block_cleanup_service import AbstractBlockCleanupService
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InventoryType
from bxgateway.utils.btc.btc_object_hash import Sha256Hash

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

    def block_cleanup_request(self, block_hash: Sha256Hash) -> None:
        if not self.is_marked_for_cleanup(block_hash):
            self._block_hash_marked_for_cleanup.add(block_hash)
            self.last_confirmed_block = block_hash
            block_request_message = GetDataBtcMessage(
                magic=self.node.opts.blockchain_net_magic,
                inv_vects=[(InventoryType.MSG_BLOCK, block_hash)],
                request_witness_data=False
            )
            self.node.send_msg_to_node(block_request_message)
            logger.trace("Received block cleanup request: {}", block_hash)

    @abstractmethod
    def clean_block_transactions(
            self,
            block_msg: BlockBtcMessage,
            transaction_service: TransactionService
    ) -> None:
        pass
