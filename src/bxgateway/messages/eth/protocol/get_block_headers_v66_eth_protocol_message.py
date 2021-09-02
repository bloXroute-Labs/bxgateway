import blxr_rlp as rlp

from bxutils import logging
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import \
    GetBlockHeadersEthProtocolMessage

logger = logging.get_logger(__name__)


class GetBlockHeadersV66EthProtocolMessage(GetBlockHeadersEthProtocolMessage):
    fields = [
        ("request_id", rlp.sedes.big_endian_int),
        ("block_headers", GetBlockHeadersEthProtocolMessage)
    ]

    def __repr__(self):
        return f"{repr(self.get_message())[:-1]}, request_id: {self.get_request_id()}>"

    def get_request_id(self) -> int:
        return self.get_field_value("request_id")

    def get_message(self) -> GetBlockHeadersEthProtocolMessage:
        block_headers = self.get_field_value("block_headers")
        if not block_headers:
            logger.error("Received invalid block headers")
        return GetBlockHeadersEthProtocolMessage(None, *block_headers)
