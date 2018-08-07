import hashlib
import struct
from collections import deque

from bxcommon.constants import BTC_HDR_COMMON_OFF, BTC_SHA_HASH_LEN, SHA256_HASH_LEN
from bxcommon.messages.broadcast_message import BroadcastMessage
from bxcommon.messages.btc.btc_message import BTCMessage
from bxcommon.messages.btc.btc_messages_util import btcvarint_to_int, get_next_tx_size
from bxcommon.messages.btc.tx_btc_message import TxBTCMessage
from bxcommon.messages.tx_message import TxMessage
from bxcommon.utils import logger
from bxcommon.utils.object_hash import BTCObjectHash, ObjectHash

sha256 = hashlib.sha256


# Convert a block message to a broadcast message
def block_to_broadcastmsg(msg, tx_service):
    # Do the block compression
    size = 0
    buf = deque()
    header = msg.header()
    size += len(header)
    buf.append(header)

    for tx in msg.txns():
        tx_hash = BTCObjectHash(buf=sha256(sha256(tx).digest()).digest(), length=BTC_SHA_HASH_LEN)
        shortid = tx_service.get_txid(tx_hash)
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


def broadcastmsg_to_block(msg, tx_service):
    # XXX: make this not a copy
    blob = bytearray(msg.blob())

    size = 0
    pieces = deque()

    # parse statistics variables
    total_tx_count = 0
    unknown_tx_sids = []
    unknown_tx_hashes = []

    # get header size
    headersize = 80 + BTC_HDR_COMMON_OFF
    _, txn_count_size = btcvarint_to_int(blob, headersize)
    headersize += txn_count_size

    block_hash = ObjectHash(blob[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + SHA256_HASH_LEN])
    header = blob[:headersize]
    pieces.append(header)
    size += headersize

    off = size
    while off < len(blob):
        if blob[off] == 0x00:
            sid, = struct.unpack_from('<I', blob, off + 1)
            tx_hash, tx = tx_service.get_tx_from_sid(sid)

            if tx_hash is None:
                unknown_tx_sids.append(sid)
            elif tx is None:
                unknown_tx_hashes.append(tx_hash)

            off += 5
        else:
            txsize = get_next_tx_size(blob, off)
            tx = blob[off:off + txsize]
            off += txsize

        if not unknown_tx_sids and not unknown_tx_hashes:  # stop appending pieces when at least on tx sid or content is unknown
            pieces.append(tx)
            size += len(tx)

        total_tx_count += 1

    if not unknown_tx_sids and not unknown_tx_hashes:
        blx_block = bytearray(size)
        off = 0
        for piece in pieces:
            next_off = off + len(piece)
            blx_block[off:next_off] = piece
            off = next_off

        logger.debug("Successfully parsed block broadcast message. {0} transactions in block".format(total_tx_count))
        return blx_block, block_hash, unknown_tx_sids, unknown_tx_hashes
    else:
        logger.warn("Block recovery: Unable to parse block message. {0} sids, {1} tx hashes missing. total txs: {2}"
                    .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
        return None, block_hash, unknown_tx_sids, unknown_tx_hashes


def tx_msg_to_btc_tx_msg(tx_msg, btc_magic):
    if not isinstance(tx_msg, TxMessage):
        raise TypeError("tx_msg is expected to be of type TxMessage")

    buf = bytearray(BTC_HDR_COMMON_OFF) + tx_msg.blob()

    btc_tx_msg = BTCMessage(btc_magic, 'tx', len(tx_msg.blob()), buf)

    return btc_tx_msg


def btc_tx_msg_to_tx_msg(btc_tx_msg):
    if not isinstance(btc_tx_msg, TxBTCMessage):
        raise TypeError("tx_msg is expected to be of type TxBTCMessage")

    tx_msg = TxMessage(btc_tx_msg.tx_hash(), btc_tx_msg.tx())

    return tx_msg
