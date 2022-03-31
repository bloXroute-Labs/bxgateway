from typing import Optional

from bxcommon import constants
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxutils.logging import LogLevel


class ConfirmedBlockMessage(AbstractBloxrouteMessage):
    """
    Confirmation message that a block has been accepted into the
    blockchain node's blockchain. Only sent to/from streaming gateways.
    """

    MESSAGE_TYPE = GatewayMessageType.CONFIRMED_BLOCK
    MSG_SIZE = (
        AbstractBloxrouteMessage.HEADER_LENGTH
        + crypto.SHA256_HASH_LEN
        + constants.CONTROL_FLAGS_LEN
    )
    PAYLOAD_LENGTH = crypto.SHA256_HASH_LEN + constants.CONTROL_FLAGS_LEN
    EMPTY_BLOCK_CONTENT = memoryview(bytes())

    buf: bytearray
    _block_hash: Optional[Sha256Hash] = None
    _block_content: Optional[memoryview] = None

    def __init__(
        self,
        block_hash: Optional[Sha256Hash] = None,
        block_content: memoryview = EMPTY_BLOCK_CONTENT,
        buf: Optional[bytearray] = None,
    ) -> None:
        if buf is None:
            assert block_hash is not None

            buf = bytearray(self.MSG_SIZE + len(block_content))

            off = AbstractBloxrouteMessage.HEADER_LENGTH
            buf[off:off + crypto.SHA256_HASH_LEN] = block_hash.binary
            off += crypto.SHA256_HASH_LEN
            buf[off:off + len(block_content)] = block_content

        self.buf = buf
        super().__init__(self.MESSAGE_TYPE, self.PAYLOAD_LENGTH + len(block_content), buf)

    def block_hash(self) -> Sha256Hash:
        if self._block_hash is None:
            off = AbstractBloxrouteMessage.HEADER_LENGTH
            self._block_hash = Sha256Hash(self._memoryview[off:off + crypto.SHA256_HASH_LEN])

        block_hash = self._block_hash
        assert block_hash is not None
        return block_hash

    def block_content(self) -> memoryview:
        if self._block_content is None:
            off = AbstractBloxrouteMessage.HEADER_LENGTH + crypto.SHA256_HASH_LEN
            self._block_content = self._memoryview[off: len(self._memoryview) - constants.CONTROL_FLAGS_LEN]

        block_content = self._block_content
        assert block_content is not None
        return block_content

    def log_level(self) -> LogLevel:
        return LogLevel.DEBUG

    def __repr__(self) -> str:
        return "ConfirmedBlockMessage<block_hash: {0}, block_content_size: {1}>".format(
            self.block_hash(), len(self.block_content())
        )
