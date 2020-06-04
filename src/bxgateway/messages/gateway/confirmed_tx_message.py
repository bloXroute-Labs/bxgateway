from typing import Optional

from bxcommon import constants
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxutils.logging import LogLevel


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

    _tx_hash: Optional[Sha256Hash] = None

    def __init__(
        self, tx_hash: Optional[Sha256Hash] = None, buf: Optional[bytearray] = None
    ) -> None:
        if buf is None:
            assert tx_hash is not None
            buf = bytearray(self.MSG_SIZE)

            off = AbstractBloxrouteMessage.HEADER_LENGTH
            buf[off:off + crypto.SHA256_HASH_LEN] = tx_hash.binary

        self.buf = buf
        super().__init__(self.MESSAGE_TYPE, self.PAYLOAD_LENGTH, buf)

    def tx_hash(self) -> Sha256Hash:
        if self._tx_hash is None:
            off = AbstractBloxrouteMessage.HEADER_LENGTH
            self._tx_hash = Sha256Hash(self._memoryview[off:off + crypto.SHA256_HASH_LEN])

        tx_hash = self._tx_hash
        assert tx_hash is not None
        return tx_hash


    def log_level(self):
        return LogLevel.INFO
