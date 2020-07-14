from typing import Optional

from bxcommon import constants
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType
from bxutils.logging import LogLevel


class RequestTxStreamMessage(AbstractBloxrouteMessage):

    MESSAGE_TYPE = GatewayMessageType.REQUEST_TX_STREAM

    def __init__(self, buf: Optional[bytearray] = None) -> None:
        if buf is None:
            buf = bytearray(
                AbstractBloxrouteMessage.HEADER_LENGTH
                + constants.CONTROL_FLAGS_LEN
            )

        super().__init__(self.MESSAGE_TYPE, constants.CONTROL_FLAGS_LEN, buf)

    def log_level(self):
        return LogLevel.DEBUG
