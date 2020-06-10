from typing import List

import task_pool_executor as tpe

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode
from bxgateway.services.gateway_transaction_service import GatewayTransactionService, \
    ProcessTransactionMessageFromNodeResult


class ExtensionGatewayTransactionService(ExtensionTransactionService, GatewayTransactionService):

    def process_transactions_message_from_node(self, msg
                                               ) -> List[ProcessTransactionMessageFromNodeResult]:
        if isinstance(self.node, OntGatewayNode):
            ext_processing_results = self.proxy.process_gateway_transaction_from_node(
                BlockchainProtocol.ONTOLOGY.value,
                tpe.InputBytes(msg.rawbytes())
            )
        # elif isinstance(self.node, EthGatewayNode):
        #     ext_processing_results = self.proxy.process_gateway_transaction_from_node(
        #         BlockchainProtocol.ETHEREUM.value,
        #         tpe.InputBytes(msg.rawbytes())
        #     )
        else:
            return GatewayTransactionService.process_transactions_message_from_node(self, msg)

        result = []

        for tx_result in ext_processing_results:
            tx_hash = Sha256Hash(memoryview(tx_result.get_tx_hash().binary()))
            tx_contents = memoryview(tx_result.get_tx_contents())

            result.append(ProcessTransactionMessageFromNodeResult(
                tx_result.get_is_seen(),
                tx_hash,
                tx_contents,
                TxMessage(message_hash=tx_hash, network_num=self.network_num, tx_val=tx_contents)
            ))

        return result


