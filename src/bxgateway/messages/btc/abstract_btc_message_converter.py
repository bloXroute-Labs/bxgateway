import typing
from abc import abstractmethod

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.utils import crypto, convert

from bxgateway import btc_constants
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.utils.block_info import BlockInfo
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


def get_block_info(
        bx_block: memoryview,
        block_hash: BtcObjectHash,
        short_ids: typing.List[int],
        total_tx_count: typing.Optional[int] = None,
        btc_block_msg: typing.Optional[BlockBtcMessage] = None
) -> BlockInfo:
    if btc_block_msg is not None:
        bx_block_hash = convert.bytes_to_hex(crypto.double_sha256(bx_block))
        prev_block_hash = convert.bytes_to_hex(btc_block_msg.prev_block().binary)
    else:
        bx_block_hash = None
        prev_block_hash = None
    return BlockInfo(
        total_tx_count,
        block_hash,
        bx_block_hash,
        prev_block_hash,
        short_ids
    )


class AbstractBtcMessageConverter(AbstractMessageConverter):

    def __init__(self, btc_magic):
        if not btc_magic:
            raise ValueError("btc_magic is required")

        self._btc_magic = btc_magic

    @abstractmethod
    def block_to_bx_block(self, btc_block_msg, tx_service):
        """
        Compresses a blockchain block's transactions and packs it into a bloXroute block.
        """
        pass

    def bx_block_to_block(self, bx_block, tx_service):
        """
        Uncompresses a bx_block from a broadcast bx_block message and converts to a raw BTC bx_block.

        bx_block must be a memoryview, since memoryview[offset] returns a bytearray, while bytearray[offset] returns
        a byte.
        """
        pass

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
