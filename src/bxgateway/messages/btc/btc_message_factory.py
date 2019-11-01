from typing import Type

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxgateway.messages.btc.addr_btc_message import AddrBtcMessage
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.block_transactions_btc_message import BlockTransactionsBtcMessage
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.compact_block_btc_message import CompactBlockBtcMessage
from bxgateway.messages.btc.data_btc_message import GetBlocksBtcMessage, GetHeadersBtcMessage
from bxgateway.messages.btc.fee_filter_btc_message import FeeFilterBtcMessage
from bxgateway.messages.btc.get_addr_btc_message import GetAddrBtcMessage
from bxgateway.messages.btc.get_block_transactions_btc_message import GetBlockTransactionsBtcMessage
from bxgateway.messages.btc.headers_btc_message import HeadersBtcMessage
from bxgateway.messages.btc.inventory_btc_message import GetDataBtcMessage, InvBtcMessage, NotFoundBtcMessage
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage
from bxgateway.messages.btc.pong_btc_message import PongBtcMessage
from bxgateway.messages.btc.reject_btc_message import RejectBtcMessage
from bxgateway.messages.btc.send_compact_btc_message import SendCompactBtcMessage
from bxgateway.messages.btc.send_headers_btc_message import SendHeadersBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.messages.btc.ver_ack_btc_message import VerAckBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxgateway.messages.btc.xversion_btc_message import XversionBtcMessage


class _BtcMessageFactory(AbstractMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        BtcMessageType.VERSION: VersionBtcMessage,
        BtcMessageType.VERACK: VerAckBtcMessage,
        BtcMessageType.PING: PingBtcMessage,
        BtcMessageType.PONG: PongBtcMessage,
        BtcMessageType.GET_ADDRESS: GetAddrBtcMessage,
        BtcMessageType.ADDRESS: AddrBtcMessage,
        BtcMessageType.INVENTORY: InvBtcMessage,
        BtcMessageType.GET_DATA: GetDataBtcMessage,
        BtcMessageType.NOT_FOUND: NotFoundBtcMessage,
        BtcMessageType.GET_HEADERS: GetHeadersBtcMessage,
        BtcMessageType.GET_BLOCKS: GetBlocksBtcMessage,
        BtcMessageType.TRANSACTIONS: TxBtcMessage,
        BtcMessageType.BLOCK: BlockBtcMessage,
        BtcMessageType.HEADERS: HeadersBtcMessage,
        BtcMessageType.REJECT: RejectBtcMessage,
        BtcMessageType.SEND_HEADERS: SendHeadersBtcMessage,
        BtcMessageType.COMPACT_BLOCK: CompactBlockBtcMessage,
        BtcMessageType.GET_BLOCK_TRANSACTIONS: GetBlockTransactionsBtcMessage,
        BtcMessageType.BLOCK_TRANSACTIONS: BlockTransactionsBtcMessage,
        BtcMessageType.FEE_FILTER: FeeFilterBtcMessage,
        BtcMessageType.SEND_COMPACT: SendCompactBtcMessage,
        BtcMessageType.XVERSION: XversionBtcMessage
    }

    def __init__(self):
        super(_BtcMessageFactory, self).__init__()
        self.message_type_mapping = self._MESSAGE_TYPE_MAPPING

    def get_base_message_type(self) -> Type[AbstractMessage]:
        return BtcMessage


btc_message_factory = _BtcMessageFactory()
