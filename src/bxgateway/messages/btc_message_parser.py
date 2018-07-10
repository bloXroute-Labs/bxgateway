import hashlib
import struct
from collections import deque

from bxcommon.btc_messages import HASH_LEN, BTC_HDR_COMMON_OFF, btcvarint_to_int, \
    get_next_tx_size
from bxcommon.messages_new.broadcast_message import BroadcastMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import BTCObjectHash

sha256 = hashlib.sha256


# Convert a block message to a broadcast message
def block_to_broadcastmsg(msg, tx_manager):
    # Do the block compression
    size = 0
    buf = deque()
    header = msg.header()
    size += len(header)
    buf.append(header)

    for tx in msg.txns():
        tx_hash = BTCObjectHash(buf=sha256(sha256(tx).digest()).digest(), length=HASH_LEN)
        shortid = tx_manager.get_txid(tx_hash)
        if shortid == -1:
            buf.append(tx)
            size += len(tx)
        else:
            next_tx = bytearray(5)
            logger.debug("XXX: Packing transaction with shortid {0} into block".format(shortid))
            struct.pack_into('<I', next_tx, 1, shortid)
            buf.append(next_tx)
            size += 5

    # Parse it into the bloXroute message format and send it along
    block = bytearray(size)
    off = 0
    for blob in buf:
        next_off = off + len(blob)
        block[off:next_off] = blob
        off = next_off

    return BroadcastMessage(msg.block_hash(), block)


# Convert a block message to a broadcast message
def broadcastmsg_to_block(msg, tx_manager):
    # XXX: make this not a copy
    blob = bytearray(msg.blob())

    size = 0
    pieces = deque()

    # get header size
    headersize = 80 + BTC_HDR_COMMON_OFF
    _, txn_count_size = btcvarint_to_int(blob, headersize)
    headersize += txn_count_size

    header = blob[:headersize]
    pieces.append(header)
    size += headersize

    off = size
    while off < len(blob):
        if blob[off] == 0x00:
            sid, = struct.unpack_from('<I', blob, off + 1)
            tx = tx_manager.get_tx_from_sid(sid)
            if tx is None:
                logger.error(
                    "XXX: Failed to decode transaction with short id {0} received from bloXroute".format(sid))
                return None
            off += 5
        else:
            txsize = get_next_tx_size(blob, off)
            tx = blob[off:off + txsize]
            off += txsize

        pieces.append(tx)
        size += len(tx)

    blx_block = bytearray(size)
    off = 0
    for piece in pieces:
        next_off = off + len(piece)
        blx_block[off:next_off] = piece
        off = next_off

    return blx_block
