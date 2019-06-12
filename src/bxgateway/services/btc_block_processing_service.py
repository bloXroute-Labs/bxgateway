from bxcommon.utils import crypto, logger
from bxgateway.btc_constants import BTC_SHA_HASH_LEN
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.services.block_processing_service import BlockProcessingService
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcBlockProcessingService(BlockProcessingService):

    def on_block_processed(self, block_message: BlockBtcMessage):
        """
        Additional processing of the block that can be customized per blockchain
        :param block_message: block message specific to a blockchain
        """
        transaction_service = self._node.get_tx_service()

        # remove transactions that were seen in the previous block
        transaction_service.clean_transaction_hashes_not_seen_in_block()

        for tx in block_message.txns():
            tx_hash = BtcObjectHash(crypto.bitcoin_hash(tx), length=BTC_SHA_HASH_LEN)
            transaction_service.track_seen_transaction_hash(tx_hash)


