from bxgateway.testing.abstract_rlpx_cipher_test import AbstractRLPxCipherTest

from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.utils import convert

from bxcommon.utils.blockchain_utils.eth import crypto_utils, eth_common_constants
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.connections.eth.eth_remote_connection import EthRemoteConnection
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_factory import EthProtocolMessageFactory
from bxgateway.messages.eth.protocol.get_block_headers_eth_protocol_message import GetBlockHeadersEthProtocolMessage
from bxgateway.testing import spies, gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages
from bxgateway.utils.eth import frame_utils


class BlockchainSyncEthTest(AbstractRLPxCipherTest):
    BLOCK_HASH = helpers.generate_bytes(eth_common_constants.BLOCK_HASH_LEN)

    def setUp(self):
        self.local_node_fileno = 1
        self.local_node_fileno_2 = 2
        self.remote_node_fileno = 3
        self.local_blockchain_ip = "127.0.0.1"
        self.local_blockchain_port = 30303
        self.local_blockchain_port_2 = 30302

        eth_node_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        self.gateway_node = spies.make_spy_node(
            EthGatewayNode, 8000, include_default_eth_args=True, blockchain_protocol="Ethereum",
            blockchain_address=(self.local_blockchain_ip, self.local_blockchain_port),
            blockchain_peers="enode://d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396@127.0.0.1:30302",
            pub_key="d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396")
        gateway_node_private_key = convert.hex_to_bytes(self.gateway_node.opts.private_key)
        eth_node_public_key = crypto_utils.private_to_public_key(eth_node_private_key)

        self.eth_node_cipher, gateway_cipher = self.setup_ciphers(eth_node_private_key, gateway_node_private_key)

        self.gateway_node._node_public_key = eth_node_public_key
        self.gateway_node._remote_public_key = eth_node_public_key

        self.eth_node_connection = spies.make_spy_connection(EthNodeConnection, self.local_node_fileno,
                                                             self.local_blockchain_port, self.gateway_node)

        self.eth_node_connection_2 = spies.make_spy_connection(EthNodeConnection, self.local_node_fileno_2,
                                                               self.local_blockchain_port_2, self.gateway_node)
        self.eth_remote_node_connection = spies.make_spy_connection(EthRemoteConnection, self.remote_node_fileno, 8003,
                                                                    self.gateway_node)

        self.eth_node_connection._rlpx_cipher = gateway_cipher
        self.eth_node_connection_2._rlpx_cipher = gateway_cipher
        self.eth_node_connection.message_factory = EthProtocolMessageFactory(gateway_cipher)
        self.eth_node_connection_2.message_factory = EthProtocolMessageFactory(gateway_cipher)
        self.eth_remote_node_connection._rlpx_cipher = gateway_cipher
        self.eth_remote_node_connection.message_factory = EthProtocolMessageFactory(gateway_cipher)
        self.gateway_node.remote_node_conn = self.eth_remote_node_connection

        self.gateway_node.connection_pool.add(
            self.local_node_fileno, LOCALHOST, self.local_blockchain_port, self.eth_node_connection
        )
        self.gateway_node.connection_pool.add(
            self.local_node_fileno_2, LOCALHOST, self.local_blockchain_port_2, self.eth_node_connection_2
        )
        self.gateway_node.connection_pool.add(self.remote_node_fileno, LOCALHOST, 8003, self.eth_remote_node_connection)

    def test_block_headers_request(self):
        get_headers = GetBlockHeadersEthProtocolMessage(None, self.BLOCK_HASH, 111, 222, 0)

        # Reply with empty headers to the first get headers request for fast sync mode support
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_not_called()
        self.eth_node_connection.enqueue_msg.assert_called_once_with(BlockHeadersEthProtocolMessage(None, []))

        # The second get headers message should be proxied to remote blockchain node
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_called_once_with(get_headers)

        headers = BlockHeadersEthProtocolMessage(None, [
            mock_eth_messages.get_dummy_block_header(1), mock_eth_messages.get_dummy_block_header(2)
        ])
        headers_frames = map(self.eth_node_cipher.encrypt_frame,
                             frame_utils.get_frames(headers.msg_type, headers.rawbytes()))
        for headers_frame in headers_frames:
            helpers.receive_node_message(self.gateway_node, self.remote_node_fileno, headers_frame)
        self.eth_node_connection.enqueue_msg.assert_called_with(headers)

    def test_queued_block_headers_requests_from_different_nodes(self):
        get_headers = GetBlockHeadersEthProtocolMessage(None, self.BLOCK_HASH, 111, 222, 0)

        # Reply with empty headers to the first get headers request for fast sync mode support
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_not_called()
        self.eth_node_connection.enqueue_msg.assert_called_once_with(BlockHeadersEthProtocolMessage(None, []))

        # Reply with empty headers to second blockchain node
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno_2, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_not_called()
        self.eth_node_connection_2.enqueue_msg.assert_called_once_with(BlockHeadersEthProtocolMessage(None, []))

        # The second get headers message should be proxied to remote blockchain node
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_called_once_with(get_headers)

        # request from second blockchain node
        self.eth_remote_node_connection.enqueue_msg.reset_mock()
        get_headers_frames = map(self.eth_node_cipher.encrypt_frame,
                                 frame_utils.get_frames(get_headers.msg_type, get_headers.rawbytes()))
        for get_headers_frame in get_headers_frames:
            helpers.receive_node_message(self.gateway_node, self.local_node_fileno_2, get_headers_frame)
        self.eth_remote_node_connection.enqueue_msg.assert_called_once_with(get_headers)

        # response for first blockchain node
        headers = BlockHeadersEthProtocolMessage(None, [
            mock_eth_messages.get_dummy_block_header(1), mock_eth_messages.get_dummy_block_header(2)
        ])
        headers_frames = map(self.eth_node_cipher.encrypt_frame,
                             frame_utils.get_frames(headers.msg_type, headers.rawbytes()))
        for headers_frame in headers_frames:
            helpers.receive_node_message(self.gateway_node, self.remote_node_fileno, headers_frame)
        self.eth_node_connection.enqueue_msg.assert_called_with(headers)

        # receive response for second blockchain node
        headers = BlockHeadersEthProtocolMessage(None, [
            mock_eth_messages.get_dummy_block_header(1), mock_eth_messages.get_dummy_block_header(2)
        ])
        headers_frames = map(self.eth_node_cipher.encrypt_frame,
                             frame_utils.get_frames(headers.msg_type, headers.rawbytes()))
        for headers_frame in headers_frames:
            helpers.receive_node_message(self.gateway_node, self.remote_node_fileno, headers_frame)
        self.eth_node_connection_2.enqueue_msg.assert_called_with(headers)
