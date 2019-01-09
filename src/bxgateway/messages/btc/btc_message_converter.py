import struct
from collections import deque

from bxcommon.constants import NULL_TX_SID
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, logger
from bxgateway import btc_constants
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.btc_constants import BTC_SHA_HASH_LEN, BTC_HDR_COMMON_OFF, BTC_BLOCK_HDR_SIZE, BTC_SHORT_ID_LENGTH
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_messages_util import btcvarint_to_int, get_next_tx_size
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class BtcMessageConverter(AbstractMessageConverter):

    def __init__(self, btc_magic):
        if not btc_magic:
            raise ValueError("btc_magic is required")

        self._btc_magic = btc_magic

    def block_to_bx_block(self, btc_block_msg, tx_service):
        """
        Compresses a Bitcoin block's transactions and packs it into a bloXroute block.
        """
        size = 0
        buf = deque()
        header = btc_block_msg.header()
        size += len(header)
        buf.append(header)

        for tx in btc_block_msg.txns():
            tx_hash = BtcObjectHash(buf=crypto.double_sha256(tx), length=BTC_SHA_HASH_LEN)
            shortid = tx_service.get_txid(tx_hash)
            if shortid == NULL_TX_SID:
                buf.append(tx)
                size += len(tx)
            else:
                next_tx = bytearray(5)
                next_tx[0] = btc_constants.BTC_SHORT_ID_INDICATOR
                logger.debug("XXX: Packing transaction with shortid {0} into block".format(shortid))
                struct.pack_into('<I', next_tx, 1, shortid)
                buf.append(next_tx)
                size += 5

        block = bytearray(size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off
        return block

    def bx_block_to_block(self, bx_block, tx_service):
        """
        Uncompresses a bx bx_block from a broadcast bx_block message and converts to a raw BTC bx_block.
        """

        # Initialize tracking of transaction and SID mapping
        block_pieces = deque()
        offset = 0
        total_tx_count = 0
        unknown_tx_sids = []
        unknown_tx_hashes = []

        # Compute block header hash
        block_header_size = BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE
        block_hash = BtcObjectHash(buf=crypto.bitcoin_hash(bx_block[BTC_HDR_COMMON_OFF:block_header_size]),
                                   length=BTC_SHA_HASH_LEN)
        offset += block_header_size

        # Add header piece
        _, txn_count_size = btcvarint_to_int(bx_block, block_header_size)
        offset += txn_count_size
        block_pieces.append(bx_block[:offset])

        # Add transaction pieces
        off = offset
        while off < len(bx_block):
            if bx_block[off] == btc_constants.BTC_SHORT_ID_INDICATOR:
                sid, = struct.unpack_from("<I", bx_block, off + 1)
                tx_hash, tx = tx_service.get_tx_from_sid(sid)

                if tx_hash is None:
                    unknown_tx_sids.append(sid)
                elif tx is None:
                    unknown_tx_hashes.append(tx_hash)
                off += BTC_SHORT_ID_LENGTH
            else:
                tx_size = get_next_tx_size(bx_block, off)
                tx = bx_block[off:off + tx_size]
                off += tx_size

            # Don't append when txsid or content unknown
            if not unknown_tx_sids and not unknown_tx_hashes:
                block_pieces.append(tx)
                offset += len(tx)

            total_tx_count += 1

        # Turn pieces into bytearray
        if not unknown_tx_sids and not unknown_tx_hashes:
            btc_block = bytearray(offset)
            off = 0
            for piece in block_pieces:
                next_off = off + len(piece)
                btc_block[off:next_off] = piece
                off = next_off

            logger.debug(
                "Successfully parsed bx_block broadcast message. {0} transactions in bx_block".format(total_tx_count))
            return BlockBtcMessage(buf=btc_block), block_hash, unknown_tx_sids, unknown_tx_hashes
        else:
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in bx_block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
            return None, block_hash, unknown_tx_sids, unknown_tx_hashes

    def bx_tx_to_tx(self, tx_msg):
        if not isinstance(tx_msg, TxMessage):
            raise TypeError("tx_msg is expected to be of type TxMessage")

        buf = bytearray(BTC_HDR_COMMON_OFF) + tx_msg.tx_val()

        btc_tx_msg = BtcMessage(self._btc_magic, "tx", len(tx_msg.tx_val()), buf)

        return btc_tx_msg

    def tx_to_bx_txs(self, btc_tx_msg, network_num):
        if not isinstance(btc_tx_msg, TxBtcMessage):
            raise TypeError("tx_msg is expected to be of type TxBTCMessage")

        tx_msg = TxMessage(btc_tx_msg.tx_hash(), network_num, btc_tx_msg.tx())

        return [(tx_msg, btc_tx_msg.tx_hash(), btc_tx_msg.tx())]
