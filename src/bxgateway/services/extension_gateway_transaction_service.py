import struct
from typing import List, cast, Set

import task_pool_executor as tpe

from bxcommon import constants
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode
from bxgateway.gateway_opts import GatewayOpts
from bxgateway.services.gateway_transaction_service import GatewayTransactionService, \
    ProcessTransactionMessageFromNodeResult, MissingTransactions


class ExtensionGatewayTransactionService(ExtensionTransactionService, GatewayTransactionService):

    def process_transactions_message_from_node(
        self,
        msg
    ) -> List[ProcessTransactionMessageFromNodeResult]:
        opts = self.node.opts
        msg_bytes = msg.rawbytes()

        if isinstance(self.node, OntGatewayNode) and opts.process_node_txs_in_extension:
            ext_processing_results = memoryview(self.proxy.process_gateway_transaction_from_node(
                BlockchainProtocol.ONTOLOGY.value,
                tpe.InputBytes(msg_bytes)
            ))
        elif isinstance(self.node, EthGatewayNode) and opts.process_node_txs_in_extension:
            ext_processing_results = memoryview(self.proxy.process_gateway_transaction_from_node(
                BlockchainProtocol.ETHEREUM.value,
                tpe.InputBytes(msg_bytes)
            ))
        else:
            return GatewayTransactionService.process_transactions_message_from_node(self, msg)

        result = []

        offset = 0

        txs_count, = struct.unpack_from("<I", ext_processing_results, offset)
        offset += constants.UL_INT_SIZE_IN_BYTES

        while offset < len(ext_processing_results):
            seen, = struct.unpack_from("<?", ext_processing_results, offset)
            offset += 1

            tx_hash = Sha256Hash(ext_processing_results[offset:offset + crypto.SHA256_HASH_LEN])
            offset += crypto.SHA256_HASH_LEN
            tx_offset, = struct.unpack_from("<I", ext_processing_results, offset)
            offset += constants.UL_INT_SIZE_IN_BYTES
            tx_len, = struct.unpack_from("<I", ext_processing_results, offset)
            offset += constants.UL_INT_SIZE_IN_BYTES

            tx_contents = msg_bytes[tx_offset:tx_offset + tx_len]

            result.append(ProcessTransactionMessageFromNodeResult(
                seen,
                tx_hash,
                tx_contents,
                TxMessage(message_hash=tx_hash, network_num=self.network_num, tx_val=tx_contents)
            ))

            if not seen:
                transaction_cache_key = self._tx_hash_to_cache_key(tx_hash)
                self.set_transaction_contents_base(
                    tx_hash,
                    transaction_cache_key,
                    False,
                    0,
                    False,
                    None,
                    tx_len
                )

        assert txs_count == len(result)

        return result

    def process_txs_message(
        self,
        msg: TxsMessage
    ) -> Set[MissingTransactions]:
        msg_bytes = msg.rawbytes()
        missing_transactions: Set[MissingTransactions] = set()
        result_buffer = memoryview(self.proxy.process_txs_msg(tpe.InputBytes(msg_bytes)))
        # result_buffer is a bytearray that consists of
        # number of missing transactions - 2 bytes
        # a list of:
        #   short_id - 4 bytes
        #   Sha256Hash - 32 bytes
        #   content length that was added to transaction service - 4 bytes
        offset = 0
        missing_transactions_count, = struct.unpack_from("<H", result_buffer, offset)
        offset += constants.UL_SHORT_SIZE_IN_BYTES
        if missing_transactions_count == 0:
            return missing_transactions

        while offset < len(result_buffer):
            short_id, = struct.unpack_from("<L", result_buffer, offset)
            offset += constants.SID_LEN

            transaction_hash = Sha256Hash(result_buffer[offset:offset + crypto.SHA256_HASH_LEN])
            offset += crypto.SHA256_HASH_LEN

            content_length, = struct.unpack_from("<L", result_buffer, offset)
            offset += constants.UL_INT_SIZE_IN_BYTES
            if content_length > 0:
                self.set_transaction_contents_base(
                    transaction_hash,
                    transaction_hash,
                    True,
                    0,
                    False,
                    None,
                    content_length
                )
            missing_transactions.add(MissingTransactions(short_id, transaction_hash))

        return missing_transactions
