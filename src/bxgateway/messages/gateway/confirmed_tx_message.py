from typing import Optional

from bxcommon import constants
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class ConfirmedTxMessage(AbstractBloxrouteMessage):
    """
    Confirmation message that a transaction has been accepted into the
    blockchain node's mempool. Only sent from streaming gateways.
    """

    MESSAGE_TYPE = GatewayMessageType.CONFIRMED_TX
    MSG_SIZE = (
        AbstractBloxrouteMessage.HEADER_LENGTH
        + crypto.SHA256_HASH_LEN
        + constants.CONTROL_FLAGS_LEN
    )
    PAYLOAD_LENGTH = crypto.SHA256_HASH_LEN + constants.CONTROL_FLAGS_LEN
    EMPTY_TX_VAL = TxMessage.EMPTY_TX_VAL

    buf: bytearray
    _tx_hash: Optional[Sha256Hash] = None
    _tx_val: Optional[memoryview] = None

    def __init__(
        self,
        tx_hash: Optional[Sha256Hash] = None,
        tx_val: memoryview = TxMessage.EMPTY_TX_VAL,
        buf: Optional[bytearray] = None,
    ) -> None:
        if buf is None:
            assert tx_hash is not None
            assert tx_val is not None
            buf = bytearray(self.MSG_SIZE + len(tx_val))

            off = AbstractBloxrouteMessage.HEADER_LENGTH
            buf[off:off + crypto.SHA256_HASH_LEN] = tx_hash.binary

            off += crypto.SHA256_HASH_LEN
            buf[off:off + len(tx_val)] = tx_val

        self.buf = buf
        super().__init__(self.MESSAGE_TYPE, self.PAYLOAD_LENGTH + len(tx_val), buf)

    def tx_hash(self) -> Sha256Hash:
        if self._tx_hash is None:
            off = AbstractBloxrouteMessage.HEADER_LENGTH
            self._tx_hash = Sha256Hash(self._memoryview[off:off + crypto.SHA256_HASH_LEN])

        tx_hash = self._tx_hash
        assert tx_hash is not None
        return tx_hash

    def tx_val(self) -> memoryview:
        if self._tx_val is None:
            off = AbstractBloxrouteMessage.HEADER_LENGTH + crypto.SHA256_HASH_LEN
            if len(self.buf) == self.PAYLOAD_LENGTH:
                self._tx_val = ConfirmedTxMessage.EMPTY_TX_VAL
            else:
                self._tx_val = self._memoryview[off:-constants.CONTROL_FLAGS_LEN]

        tx_val = self._tx_val
        assert tx_val is not None
        return tx_val
