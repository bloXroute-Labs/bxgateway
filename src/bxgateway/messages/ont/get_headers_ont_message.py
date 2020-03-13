from typing import Optional

from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.ont.data_ont_message import DataOntMessage
from bxgateway.messages.ont.ont_message_type import OntMessageType


class GetHeadersOntMessage(DataOntMessage):
    MESSAGE_TYPE = OntMessageType.GET_HEADERS

    def __init__(self, magic: Optional[int] = None, length: Optional[int] = None,
                 hash_start: Optional[Sha256Hash] = None, hash_stop: Optional[Sha256Hash] = None,
                 buf: Optional[bytearray] = None):
        super(GetHeadersOntMessage, self).__init__(magic, length, hash_start, hash_stop, self.MESSAGE_TYPE, buf)
