from typing import Optional

from bxgateway.messages.ont.data_ont_message import DataOntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType
from bxgateway.utils.ont.ont_object_hash import OntObjectHash


class GetBlocksOntMessage(DataOntMessage):
    MESSAGE_TYPE = OntMessageType.GET_BLOCKS

    def __init__(self, magic: Optional[int] = None, length: Optional[int] = None,
                 hash_start: Optional[OntObjectHash] = None, hash_stop: Optional[OntObjectHash] = None,
                 buf: Optional[bytearray] = None):
        super(GetBlocksOntMessage, self).__init__(magic, length, hash_start, hash_stop, self.MESSAGE_TYPE, buf)
