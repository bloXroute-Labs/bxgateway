from enum import Enum
from typing import Union, Optional

from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.btc.btc_message_type import BtcMessageType


class MessageConversionType(Enum):
    BLOCK_COMPRESSION = "BlockCompression"
    BLOCK_DECOMPRESSION = "BlockDecompression"
    COMPACT_BLOCK_COMPRESSION = "CompactBlockCompression"


class MessageConversionError(Exception):

    def __init__(
            self,
            msg_hash: Sha256Hash,
            src_msg_type: Optional[bytes],
            target_msg_type: bytes,
            error: str,
            conversion_type: "MessageConversionType"
    ):
        if src_msg_type:
            self.error_msg = f"failed to convert {src_msg_type.decode()} to {target_msg_type.decode()} - {error}"
        else:
            self.error_msg = f"failed to convert message - {error}"

        self.msg_hash = msg_hash
        self.conversion_type = conversion_type
        super(MessageConversionError, self).__init__(self.error_msg)


def btc_block_decompression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash, BloxrouteMessageType.BROADCAST, BtcMessageType.BLOCK, str(error), MessageConversionType.BLOCK_DECOMPRESSION
    )


def btc_block_compression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash, BtcMessageType.BLOCK, BloxrouteMessageType.BROADCAST, str(error), MessageConversionType.BLOCK_COMPRESSION
    )


def eth_block_compression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash, None, BloxrouteMessageType.BROADCAST, str(error), MessageConversionType.BLOCK_COMPRESSION
    )


def btc_compact_block_compression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash,
        BtcMessageType.COMPACT_BLOCK,
        BloxrouteMessageType.BROADCAST,
        str(error),
        MessageConversionType.COMPACT_BLOCK_COMPRESSION
    )
