from abc import abstractmethod
from collections import deque

from bxcommon.messages.bloxroute import compact_block_short_ids_serializer
from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, logger
from bxgateway import btc_constants
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.btc import btc_messages_util
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class AbstractBtcMessageConverter(AbstractMessageConverter):

    def __init__(self, btc_magic):
        if not btc_magic:
            raise ValueError("btc_magic is required")

        self._btc_magic = btc_magic

    @abstractmethod
    def block_to_bx_block(self, btc_block_msg, tx_service):
        pass

    def bx_block_to_block(self, bx_block, tx_service):
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw BTC bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        if not isinstance(bx_block, memoryview):
            bx_block = memoryview(bx_block)

        # Initialize tracking of transaction and SID mapping
        block_pieces = deque()
        offset = 0
        total_tx_count = 0
        unknown_tx_sids = []
        unknown_tx_hashes = []

        block_offsets = compact_block_short_ids_serializer.get_bx_block_offsets(bx_block)
        short_ids, short_ids_len = compact_block_short_ids_serializer.deserialize_short_ids_from_buffer(
            bx_block,
            block_offsets.short_id_offset
        )

        # Compute block header hash
        block_header_size = \
            block_offsets.block_begin_offset + \
            btc_constants.BTC_HDR_COMMON_OFF + \
            btc_constants.BTC_BLOCK_HDR_SIZE
        block_hash = BtcObjectHash(
            buf=crypto.bitcoin_hash(
                bx_block[
                    block_offsets.block_begin_offset + btc_constants.BTC_HDR_COMMON_OFF:
                    block_header_size
                ]),
            length=btc_constants.BTC_SHA_HASH_LEN)
        offset += block_header_size

        # Add header piece
        _, txn_count_size = btc_messages_util.btc_varint_to_int(bx_block, block_header_size)
        offset += txn_count_size
        block_pieces.append(bx_block[block_offsets.block_begin_offset:offset])

        # Add transaction pieces
        off = offset
        short_tx_index = 0
        while off < block_offsets.short_id_offset:
            if bx_block[off] == btc_constants.BTC_SHORT_ID_INDICATOR:
                sid = short_ids[short_tx_index]
                tx_hash, tx, _ = tx_service.get_transaction(sid)

                if tx_hash is None:
                    unknown_tx_sids.append(sid)
                elif tx is None:
                    unknown_tx_hashes.append(tx_hash)
                off += btc_constants.BTC_SHORT_ID_INDICATOR_LENGTH
                short_tx_index += 1
            else:
                tx_size = btc_messages_util.get_next_tx_size(bx_block, off)
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
            return BlockBtcMessage(buf=btc_block), block_hash, short_ids, unknown_tx_sids, unknown_tx_hashes
        else:
            logger.warn("Block recovery needed. Missing {0} sids, {1} tx hashes. Total txs in bx_block: {2}"
                        .format(len(unknown_tx_sids), len(unknown_tx_hashes), total_tx_count))
            return None, block_hash, short_ids, unknown_tx_sids, unknown_tx_hashes

    def bx_tx_to_tx(self, tx_msg):
        if not isinstance(tx_msg, TxMessage):
            raise TypeError("tx_msg is expected to be of type TxMessage")

        buf = bytearray(btc_constants.BTC_HDR_COMMON_OFF) + tx_msg.tx_val()
        raw_btc_tx_msg = BtcMessage(self._btc_magic, TxBtcMessage.MESSAGE_TYPE, len(tx_msg.tx_val()), buf)
        btc_tx_msg = TxBtcMessage(buf=raw_btc_tx_msg.buf)

        return btc_tx_msg

    def tx_to_bx_txs(self, btc_tx_msg, network_num):
        if not isinstance(btc_tx_msg, TxBtcMessage):
            raise TypeError("tx_msg is expected to be of type TxBTCMessage")

        tx_msg = TxMessage(tx_hash=btc_tx_msg.tx_hash(), network_num=network_num, tx_val=btc_tx_msg.tx())

        return [(tx_msg, btc_tx_msg.tx_hash(), btc_tx_msg.tx())]
