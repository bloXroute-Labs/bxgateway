from dataclasses import dataclass
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage


@dataclass
class RemoteHeaderRequest:
    """
    Class for tracking the GetBlockHeadersEthProtocolMessage requests we forward to the remote blockchain node and the
    attempts to receive non-empty HeadersEthProtocolMessages in response
    """
    get_msg: GetBlockHeadersEthProtocolMessage
    attempts: int
