from enum import Enum
from typing import Union

from bxcommon.messages.bloxroute.bloxroute_message_type import BloxrouteMessageType
from bxcommon.utils.object_hash import Sha256Hash

from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class MessageConversionType(Enum):
    BLOCK_COMPRESSION = "BlockCompression"
    BLOCK_DECOMPRESSION = "BlockDecompression"
    COMPACT_BLOCK_COMPRESSION = "CompactBlockCompression"


class MessageConversionError(Exception):

    def __init__(
            self,
            msg_hash: Sha256Hash,
            src_msg_type: bytes,
            target_msg_type: bytes,
            error: str,
            conversion_type: "MessageConversionType"
    ):
        self.error_msg = f"failed to convert {src_msg_type.decode()} to {target_msg_type.decode()} - {error}"


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
        msg_hash, EthProtocolMessageType.NEW_BLOCK_BYTES, BloxrouteMessageType.BROADCAST, str(error), MessageConversionType.BLOCK_COMPRESSION
    )


def eth_block_decompression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash, BloxrouteMessageType.BROADCAST, EthProtocolMessageType.NEW_BLOCK_BYTES, str(error), MessageConversionType.BLOCK_DECOMPRESSION
    )


def btc_compact_block_compression_error(msg_hash: Sha256Hash, error: Union[Exception, str]) -> "MessageConversionError":
    return MessageConversionError(
        msg_hash,
        BtcMessageType.COMPACT_BLOCK,
        BloxrouteMessageType.BROADCAST,
        str(error),
        MessageConversionType.COMPACT_BLOCK_COMPRESSION
    )
