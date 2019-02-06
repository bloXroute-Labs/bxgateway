from bxcommon import constants
from bxcommon.messages.bloxroute.message import Message
from bxcommon.utils.log_level import LogLevel
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class BlockPropagationRequestMessage(Message):
    """
    Request for other gateways to encrypt and propagate block message.
    """
    MESSAGE_TYPE = GatewayMessageType.BLOCK_PROPAGATION_REQUEST

    def __init__(self, blob=None, buf=None):
        if buf is None:
            payload_len = len(blob)
            buf = bytearray(constants.HDR_COMMON_OFF + len(blob))

            off = constants.HDR_COMMON_OFF
            buf[off:off + len(blob)] = blob
        else:
            payload_len = len(buf) - constants.HDR_COMMON_OFF

        self.buf = buf

        super(BlockPropagationRequestMessage, self).__init__(self.MESSAGE_TYPE, payload_len, self.buf)
        self._blob = None

    def log_level(self):
        return LogLevel.INFO

    def blob(self):
        if self._blob is None:
            off = constants.HDR_COMMON_OFF
            self._blob = self.buf[off: off + self.payload_len()]
        return self._blob
