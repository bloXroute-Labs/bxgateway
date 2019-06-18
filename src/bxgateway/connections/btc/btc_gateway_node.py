from bxcommon.services.transaction_service import TransactionService
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.inventory_btc_message import InvBtcMessage, InventoryType
from bxgateway.services.btc.btc_block_processing_service import BtcBlockProcessingService
from bxgateway.testing.eth_lossy_relay_connection import EthLossyRelayConnection
from bxgateway.testing.test_modes import TestModes


class BtcGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(BtcGatewayNode, self).__init__(opts)

        if opts.use_extensions or opts.import_extensions:
            from bxcommon.services.extension_transaction_service import ExtensionTransactionService

        if opts.use_extensions:
            self._tx_service = ExtensionTransactionService(self, self.network_num)
        else:
            self._tx_service = TransactionService(self, self.network_num)

        self.block_processing_service = BtcBlockProcessingService(self)

    def get_blockchain_connection_cls(self):
        return BtcNodeConnection

    def get_relay_connection_cls(self):
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            return EthLossyRelayConnection
        else:
            return BtcRelayConnection

    def get_remote_blockchain_connection_cls(self):
        return BtcRemoteConnection

    def send_msg_to_node(self, msg):
        super(BtcGatewayNode, self).send_msg_to_node(msg)

        # After sending block message to Bitcoin node sending INV message for the same block to the node
        # This is needed to update Synced Headers value of the gateway peer on the Bitcoin node
        # If Synced Headers is not up-to-date than Bitcoin node does not push compact blocks to the gateway
        if isinstance(msg, BlockBtcMessage):
            inv_msg = InvBtcMessage(magic=msg.magic(), inv_vects=[(InventoryType.MSG_BLOCK, msg.block_hash())])
            self.send_msg_to_node(inv_msg)
