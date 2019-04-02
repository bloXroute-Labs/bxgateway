from bxcommon.services.transaction_service import TransactionService
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.connections.btc.btc_node_connection import BtcNodeConnection
from bxgateway.connections.btc.btc_relay_connection import BtcRelayConnection
from bxgateway.connections.btc.btc_remote_connection import BtcRemoteConnection
from bxgateway.testing.eth_lossy_relay_connection import EthLossyRelayConnection
from bxgateway.testing.test_modes import TestModes
from bxgateway.services.btc_transaction_service import BtcTransactionService


class BtcGatewayNode(AbstractGatewayNode):
    def __init__(self, opts):
        super(BtcGatewayNode, self).__init__(opts)
        if opts.use_extensions:
            self._tx_service = BtcTransactionService(self, self.network_num)
        else:
            self._tx_service = TransactionService(self, self.network_num)

    def get_blockchain_connection_cls(self):
        return BtcNodeConnection

    def get_relay_connection_cls(self):
        if TestModes.DROPPING_TXS in self.opts.test_mode:
            return EthLossyRelayConnection
        else:
            return BtcRelayConnection

    def get_remote_blockchain_connection_cls(self):
        return BtcRemoteConnection
