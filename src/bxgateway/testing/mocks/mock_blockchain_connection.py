from bxcommon.test_utils import helpers
from bxcommon.utils import crypto, convert
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.abstract_message_converter import AbstractMessageConverter
from bxgateway.block_hash_message import BlockHashMessage
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection


class MockBlockMessage(BlockHashMessage):
    MESSAGE_TYPE = "mockblock"


class MockMessageConverter(AbstractMessageConverter):
    PREV_BLOCK = ObjectHash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN))

    def tx_to_bx_txs(self, tx_msg, network_num):
        return [(tx_msg, tx_msg.tx_hash(), tx_msg.tx_val())]

    def bx_tx_to_tx(self, bx_tx_msg):
        return bx_tx_msg

    def block_to_bx_block(self, block_msg, tx_service):
        return block_msg.rawbytes(), (0, convert.bytes_to_hex(self.PREV_BLOCK.binary), [])

    def bx_block_to_block(self, bx_block_msg, tx_service):
        block_message = MockBlockMessage(buf=bx_block_msg)
        return block_message, block_message.block_hash(), [], [], []


class MockBlockchainConnection(AbstractGatewayBlockchainConnection):
    def __init__(self, sock, address, node):
        super(MockBlockchainConnection, self).__init__(sock, address, node)
        self.message_converter = MockMessageConverter()
