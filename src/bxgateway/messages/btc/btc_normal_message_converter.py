import datetime
import struct
import time
from collections import deque
from typing import Tuple

from bxcommon import constants
from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.utils import crypto, convert
from bxgateway import btc_constants
from bxgateway.messages.btc.abstract_btc_message_converter import AbstractBtcMessageConverter
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcNormalMessageConverter(AbstractBtcMessageConverter):

    def block_to_bx_block(self, block_msg, tx_service) -> Tuple[memoryview, BlockInfo]:
        """
        Compresses a Bitcoin block's transactions and packs it into a bloXroute block.
        """
        compress_start_datetime = datetime.datetime.utcnow()
        compress_start_timestamp = time.time()
        size = 0
        buf = deque()
        short_ids = []
        header = block_msg.header()
        size += len(header)
        buf.append(header)

        for tx in block_msg.txns():
            tx_hash = BtcObjectHash(buf=crypto.double_sha256(tx), length=btc_constants.BTC_SHA_HASH_LEN)
            short_id = tx_service.get_short_id(tx_hash)
            if short_id == constants.NULL_TX_SID:
                buf.append(tx)
                size += len(tx)
            else:
                short_ids.append(short_id)
                buf.append(btc_constants.BTC_SHORT_ID_INDICATOR_AS_BYTEARRAY)
                size += 1

        serialized_short_ids = compact_block_short_ids_serializer.serialize_short_ids_into_bytes(short_ids)
        buf.append(serialized_short_ids)
        size += constants.C_SIZE_T_SIZE_IN_BYTES
        offset_buf = struct.pack("@N", size)
        buf.appendleft(offset_buf)
        size += len(serialized_short_ids)

        block = bytearray(size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off

        prev_block_hash = convert.bytes_to_hex(block_msg.prev_block().binary)
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(block))
        original_size = len(block_msg.rawbytes())

        block_info = BlockInfo(
            block_msg.block_hash(),
            short_ids,
            compress_start_datetime,
            datetime.datetime.utcnow(),
            (time.time() - compress_start_timestamp) * 1000,
            block_msg.txn_count(),
            bx_block_hash,
            prev_block_hash,
            original_size,
            size,
            100 - float(size) / original_size * 100
        )
        return memoryview(block), block_info
