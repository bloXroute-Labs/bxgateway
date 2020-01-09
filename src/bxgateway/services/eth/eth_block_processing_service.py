from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.messages.eth.protocol.block_bodies_eth_protocol_message import BlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_bodies_eth_protocol_message import GetBlockBodiesEthProtocolMessage
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.services.block_processing_service import BlockProcessingService


class EthBlockProcessingService(BlockProcessingService):
    _node: "bxgateway.connections.eth.eth_gateway_node.EthGatewayNode"

    def try_process_get_block_headers_request(self, msg: GetBlockHeadersEthProtocolMessage) -> bool:
        if msg.get_amount() == 1 or msg.get_skip() == 0:
            block_hash_bytes = msg.get_block_hash()

            if len(block_hash_bytes) != eth_constants.BLOCK_HASH_LEN:
                return False

            block_hash = Sha256Hash(block_hash_bytes)

            block_header_bytes = self._node.block_queuing_service.get_sent_new_block_header(block_hash)

            if block_header_bytes is not None:
                block_headers_msg = BlockHeadersEthProtocolMessage.from_header_bytes(block_header_bytes)
                raw_headers = block_headers_msg.get_block_headers()
                headers_list = list(raw_headers)

                if headers_list:
                    self._node.send_msg_to_node(block_headers_msg)
                    return True

        return False

    def try_process_get_block_bodies_request(self, msg: GetBlockBodiesEthProtocolMessage) -> bool:
        block_hashes = msg.get_block_hashes()

        if len(block_hashes) == 1:
            block_hash = Sha256Hash(block_hashes[0])

            block_body_bytes = self._node.block_queuing_service.get_sent_new_block_body(block_hash)

            if block_body_bytes is not None:
                block_body_msg = BlockBodiesEthProtocolMessage.from_body_bytes(block_body_bytes)
                self._node.send_msg_to_node(block_body_msg)
                return True

        return False
