from typing import Dict, Any, Union

import rlp

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import log_messages
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxutils import logging

logger = logging.get_logger(__name__)


class EthTransactionFeedEntry:
    tx_hash: str
    tx_contents: Dict[str, Any]

    def __init__(self, tx_hash: Sha256Hash, tx_contents: Union[memoryview, Dict[str, Any]]) -> None:
        self.tx_hash = f"0x{str(tx_hash)}"

        try:
            if isinstance(tx_contents, memoryview):
                # parse transaction from memoryview
                transaction = rlp.decode(tx_contents.tobytes(), Transaction)
                self.tx_contents = transaction.to_json()
            else:
                # normalize json from source
                self.tx_contents = Transaction.from_json(tx_contents).to_json()
        except Exception as e:
            tx_contents_str = tx_contents
            if isinstance(tx_contents, memoryview):
                tx_contents_str = tx_contents.tobytes()
                
            logger.error(
                log_messages.COULD_NOT_DESERIALIZE_TRANSACTION,
                tx_hash,
                tx_contents_str,
                e
            )
            raise e

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, EthTransactionFeedEntry)
            and other.tx_hash == self.tx_hash
            and other.tx_contents == self.tx_contents
        )
