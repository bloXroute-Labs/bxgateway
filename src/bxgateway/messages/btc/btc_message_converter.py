import struct
from collections import deque

from bxcommon.constants import NULL_TX_SID
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, logger, convert
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.btc_constants import BTC_SHA_HASH_LEN, BTC_HDR_COMMON_OFF
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

    def bx_block_to_block(self, block, tx_service):
        """
        Uncompresses a blob from a broadcast block message and converts to a raw BTC block.
        """
        size = 0
        pieces = deque()

        # parse statistics variables
        total_tx_count = 0
        unknown_tx_sids = []
        unknown_tx_hashes = []

        # get header size
        headersize = 80 + BTC_HDR_COMMON_OFF
        _, txn_count_size = btcvarint_to_int(block, headersize)
        headersize += txn_count_size

        block_hash = ObjectHash(block[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + SHA256_HASH_LEN])
        header = block[:headersize]
        pieces.append(header)
        size += headersize

        off = size
        while off < len(block):
            if block[off] == 0x00:
                sid, = struct.unpack_from('<I', block, off + 1)
                tx_hash, tx = tx_service.get_tx_from_sid(sid)

                if tx_hash is None:
                    unknown_tx_sids.append(sid)
                elif tx is None:
                    unknown_tx_hashes.append(tx_hash)

                off += 5
            else:
                txsize = get_next_tx_size(block, off)
                tx = block[off:off + txsize]
                off += txsize

            # dont append when txsid or content unknown
            if not unknown_tx_sids and not unknown_tx_hashes:
                pieces.append(tx)
                size += len(tx)

            total_tx_count += 1

        if not unknown_tx_sids and not unknown_tx_hashes:
            btc_block = bytearray(size)
            off = 0
            for piece in pieces:
                next_off = off + len(piece)
                btc_block[off:next_off] = piece
                off = next_off

            logger.debug(
                "Successfully parsed block broadcast message. {0} transactions in block".format(total_tx_count))
            return BlockBtcMessage(buf=btc_block), block_hash, unknown_tx_sids, unknown_tx_hashes
        else:
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in block: {2}"
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
