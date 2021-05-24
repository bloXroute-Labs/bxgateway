from typing import List

import blxr_rlp as rlp

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class NewPooledTransactionHashesEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.NEW_POOLED_TRANSACTION_HASHES

    fields = [
        ("_transaction_hashes_raw", rlp.sedes.CountableList(rlp.sedes.binary))
    ]

    _transaction_hashes_raw: List[bytearray]
    _transaction_hashes: List[Sha256Hash]

    def __init__(self, msg_bytes, *args, **kwargs) -> None:
        self._transaction_hashes_raw = []
        self._transaction_hashes = []
        super().__init__(msg_bytes, *args, **kwargs)

    def transaction_hashes(self) -> List[Sha256Hash]:
        if not self._transaction_hashes:
            self._transaction_hashes_raw = self.get_field_value("_transaction_hashes_raw")
            self._transaction_hashes = [
                Sha256Hash(transaction_hash) for transaction_hash in self._transaction_hashes_raw
            ]
        return self._transaction_hashes
