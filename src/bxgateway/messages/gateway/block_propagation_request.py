from bxutils.logging.log_level import LogLevel

from bxcommon import constants
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage

from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class BlockPropagationRequestMessage(AbstractBloxrouteMessage):
    """
    Request for other gateways to encrypt and propagate block message.
    """
    MESSAGE_TYPE = GatewayMessageType.BLOCK_PROPAGATION_REQUEST

    def __init__(self, blob=None, buf=None):
        if buf is None:
            payload_len = len(blob) + constants.CONTROL_FLAGS_LEN
            buf = bytearray(self.HEADER_LENGTH + payload_len)

            off = self.HEADER_LENGTH
            buf[off:off + len(blob)] = blob
        else:
            payload_len = len(buf) - constants.BX_HDR_COMMON_OFF

        self.buf = buf

        super(BlockPropagationRequestMessage, self).__init__(self.MESSAGE_TYPE, payload_len, self.buf)
        self._blob = None

    def log_level(self):
        return LogLevel.DEBUG

    def blob(self):
        if self._blob is None:
            off = self.HEADER_LENGTH
            self._blob = self.buf[off: off + self.payload_len() - constants.CONTROL_FLAGS_LEN]
        return self._blob
