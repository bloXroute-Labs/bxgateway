import struct
from typing import List, Set, Union

import task_pool_executor as tpe

from bxcommon import constants
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.bloxroute.txs_message import TxsMessage
from bxcommon.models.tx_validation_status import TxValidationStatus
from bxcommon.services.extension_transaction_service import ExtensionTransactionService
from bxcommon.services.transaction_service import TransactionFromBdnGatewayProcessingResult
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.ont.ont_gateway_node import OntGatewayNode
from bxgateway.services.gateway_transaction_service import GatewayTransactionService, \
    ProcessTransactionMessageFromNodeResult, MissingTransactions


class ExtensionGatewayTransactionService(ExtensionTransactionService, GatewayTransactionService):

    def process_gateway_transaction_from_bdn(
        self,
        transaction_hash: Sha256Hash,
        short_id: int,
        transaction_contents: Union[bytearray, memoryview],
        is_compact: bool
    ) -> TransactionFromBdnGatewayProcessingResult:

        transaction_key = self.get_transaction_key(transaction_hash)

        ext_result = self.proxy.process_gateway_transaction_from_bdn(
            # pyre-fixme[6]: Expected `bxcommon.models.transaction_key.TransactionKey`
            transaction_key.transaction_cache_key,
            tpe.InputBytes(transaction_contents),
            short_id,
            is_compact
        )

        result = TransactionFromBdnGatewayProcessingResult(
            ext_result.get_ignore_seen(),
            ext_result.get_existing_short_id(),
            ext_result.get_assigned_short_id(),
            ext_result.get_existing_contents(),
            ext_result.get_set_contents()
        )

        if result.set_content:
            has_short_id, previous_size = ext_result.get_set_contents_result()
            self.set_transaction_contents_base_by_key(
                transaction_key,
                has_short_id,
                previous_size,
                False,
                transaction_contents,
                len(transaction_contents)
            )

        if result.assigned_short_id:
            has_contents = result.existing_contents or result.set_content
            self.assign_short_id_base_by_key(
                transaction_key,
                short_id,
                has_contents,
                False
            )

        return result

    def process_transactions_message_from_node(
        self,
        msg,
        min_tx_network_fee: int,
        enable_transaction_validation: bool
    ) -> List[ProcessTransactionMessageFromNodeResult]:
        opts = self.node.opts
        msg_bytes = msg.rawbytes()

        if (isinstance(self.node, OntGatewayNode) or isinstance(self.node, EthGatewayNode)) and \
                opts.process_node_txs_in_extension:
            ext_processing_results = memoryview(self.proxy.process_gateway_transaction_from_node(
                tpe.InputBytes(msg_bytes),
                min_tx_network_fee,
                enable_transaction_validation
            ))
        else:
            return GatewayTransactionService.process_transactions_message_from_node(
                self, msg, min_tx_network_fee, enable_transaction_validation
            )

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
            tx_validation_status, = struct.unpack_from("<I", ext_processing_results, offset)
            offset += constants.UL_INT_SIZE_IN_BYTES

            tx_contents = msg_bytes[tx_offset:tx_offset + tx_len]

            result.append(
                ProcessTransactionMessageFromNodeResult(
                    seen,
                    tx_hash,
                    tx_contents,
                    TxMessage(
                        message_hash=tx_hash,
                        network_num=self.network_num,
                        tx_val=tx_contents,
                        account_id=self.node.account_id
                    ),
                    TxValidationStatus(tx_validation_status)
                )
            )

            if not seen:
                transaction_key = self.get_transaction_key(tx_hash)
                self.set_transaction_contents_base_by_key(transaction_key, False, 0, False, tx_contents, tx_len)

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
            transaction_key = self.get_transaction_key(transaction_hash)
            offset += crypto.SHA256_HASH_LEN

            content_length, = struct.unpack_from("<L", result_buffer, offset)
            offset += constants.UL_INT_SIZE_IN_BYTES
            if content_length > 0:
                self.set_transaction_contents_base_by_key(
                    transaction_key,
                    True,
                    0,
                    False,
                    None,
                    content_length
                )
            missing_transactions.add(MissingTransactions(short_id, transaction_hash))

        return missing_transactions
