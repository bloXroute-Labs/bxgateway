from typing import Type

from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.messages.abstract_message_factory import AbstractMessageFactory
from bxgateway.messages.ont.addr_ont_message import AddrOntMessage
from bxgateway.messages.ont.block_ont_message import BlockOntMessage
from bxgateway.messages.ont.consensus_ont_message import OntConsensusMessage
from bxgateway.messages.ont.get_addr_ont_message import GetAddrOntMessage
from bxgateway.messages.ont.get_blocks_ont_message import GetBlocksOntMessage
from bxgateway.messages.ont.get_data_ont_message import GetDataOntMessage
from bxgateway.messages.ont.get_headers_ont_message import GetHeadersOntMessage
from bxgateway.messages.ont.headers_ont_message import HeadersOntMessage
from bxgateway.messages.ont.inventory_ont_message import InvOntMessage
from bxgateway.messages.ont.notfound_ont_message import NotFoundOntMessage
from bxgateway.messages.ont.ont_message import OntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.messages.ont.ping_ont_message import PingOntMessage
from bxgateway.messages.ont.pong_ont_message import PongOntMessage
from bxgateway.messages.ont.tx_ont_message import TxOntMessage
from bxgateway.messages.ont.ver_ack_ont_message import VerAckOntMessage
from bxgateway.messages.ont.version_ont_message import VersionOntMessage


class _OntMessageFactory(AbstractMessageFactory):
    _MESSAGE_TYPE_MAPPING = {
        OntMessageType.VERSION: VersionOntMessage,
        OntMessageType.VERACK: VerAckOntMessage,
        OntMessageType.GET_ADDRESS: GetAddrOntMessage,
        OntMessageType.ADDRESS: AddrOntMessage,
        OntMessageType.PING: PingOntMessage,
        OntMessageType.PONG: PongOntMessage,
        OntMessageType.CONSENSUS: OntConsensusMessage,
        OntMessageType.INVENTORY: InvOntMessage,
        OntMessageType.GET_DATA: GetDataOntMessage,
        OntMessageType.GET_HEADERS: GetHeadersOntMessage,
        OntMessageType.GET_BLOCKS: GetBlocksOntMessage,
        OntMessageType.BLOCK: BlockOntMessage,
        OntMessageType.HEADERS: HeadersOntMessage,
        OntMessageType.TRANSACTIONS: TxOntMessage,
        OntMessageType.NOT_FOUND: NotFoundOntMessage
    }

    def __init__(self):
        super(_OntMessageFactory, self).__init__()
        self.message_type_mapping = self._MESSAGE_TYPE_MAPPING

    def get_base_message_type(self) -> Type[AbstractMessage]:
        return OntMessage


ont_message_factory = _OntMessageFactory()
