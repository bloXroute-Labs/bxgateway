import time

import typing

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger, convert
from bxgateway import eth_constants, gateway_constants
from bxgateway.connections.abstract_blockchain_connection_protocol import AbstractBlockchainConnectionProtocol
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.disconnect_eth_protocol_message import DisconnectEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_factory import EthProtocolMessageFactory
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.hello_eth_protocol_message import HelloEthProtocolMessage
from bxgateway.messages.eth.protocol.ping_eth_protocol_message import PingEthProtocolMessage
from bxgateway.messages.eth.protocol.pong_eth_protocol_message import PongEthProtocolMessage
from bxgateway.messages.eth.protocol.raw_eth_protocol_message import RawEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.utils.eth import frame_utils
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.eth.eth_gateway_stats_service import eth_gateway_stats_service


class EthBaseConnectionProtocol(AbstractBlockchainConnectionProtocol):
    def __init__(self, connection, is_handshake_initiator, private_key, public_key):
        super(EthBaseConnectionProtocol, self).__init__(connection)

        self.node = typing.cast("bxgateway.connections.eth.eth_gateway_node.EthGatewayNode", connection.node)
        self.rlpx_cipher = RLPxCipher(is_handshake_initiator, private_key, public_key)

        self._last_ping_pong_time = None
        self._handshake_complete = False

        connection.hello_messages = [EthProtocolMessageType.AUTH,
                                     EthProtocolMessageType.AUTH_ACK,
                                     EthProtocolMessageType.HELLO,
                                     EthProtocolMessageType.STATUS]

        connection.message_factory = EthProtocolMessageFactory(self.rlpx_cipher)
        expected_first_msg_type = EthProtocolMessageType.AUTH_ACK if is_handshake_initiator \
            else EthProtocolMessageType.AUTH
        connection.message_factory.set_expected_msg_type(expected_first_msg_type)
        connection.message_converter = EthMessageConverter()
        connection.message_handlers = {
            EthProtocolMessageType.AUTH: self.msg_auth,
            EthProtocolMessageType.AUTH_ACK: self.msg_auth_ack,
            EthProtocolMessageType.HELLO: self.msg_hello,
            EthProtocolMessageType.DISCONNECT: self.msg_disconnect,
            EthProtocolMessageType.PING: self.msg_ping,
            EthProtocolMessageType.PONG: self.msg_pong,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers
        }
        connection.ping_message = PingEthProtocolMessage(None)
        connection.pong_message = PongEthProtocolMessage(None)

        self._waiting_checkpoint_headers_request = True

        if is_handshake_initiator:
            logger.debug("Public key of remote node is known. Starting handshake.")
            self._enqueue_auth_message()
        else:
            logger.debug("Public key of remote node is not know. Waiting for handshake request.")
            connection.node.alarm_queue.register_alarm(eth_constants.HANDSHAKE_TIMEOUT_SEC, self._handshake_timeout)

    def msg_auth(self, msg):
        logger.debug("Begin process auth message")
        msg_bytes = msg.rawbytes()
        decrypted_auth_msg, size = self.rlpx_cipher.decrypt_auth_message(bytes(msg_bytes))
        if decrypted_auth_msg is None:
            logger.debug("Auth message is not complete. Continue waiting for more bytes.")
            return

        self.rlpx_cipher.parse_auth_message(decrypted_auth_msg)

        self._enqueue_auth_ack_message()
        self._finalize_handshake()
        self._enqueue_hello_message()
        self.connection.message_factory.reset_expected_msg_type()

    def msg_auth_ack(self, msg):
        logger.debug("Begin process auth ack message")
        auth_ack_msg_bytes = msg.rawbytes()
        self.rlpx_cipher.decrypt_auth_ack_message(bytes(auth_ack_msg_bytes))
        self._finalize_handshake()
        self._enqueue_hello_message()
        self.connection.message_factory.reset_expected_msg_type()

    def msg_ping(self, msg):
        self.connection.msg_ping(msg)
        self._last_ping_pong_time = time.time()

    def msg_pong(self, msg):
        self.connection.msg_pong(msg)
        self._last_ping_pong_time = time.time()

    def msg_hello(self, msg):
        logger.debug("Hello message received.")
        self._enqueue_status_message()

    def msg_disconnect(self, msg):
        logger.debug("Disconnect message received. Disconnect reason {0}.".format(msg.get_reason()))
        self.connection.mark_for_close()

    def msg_get_block_headers(self, msg):
        logger.info("Replying with empty headers message to the get headers request")
        block_headers_msg = BlockHeadersEthProtocolMessage(None, [])
        self.connection.enqueue_msg(block_headers_msg)
        self._waiting_checkpoint_headers_request = False

    def get_message_bytes(self, msg):
        if isinstance(msg, RawEthProtocolMessage):
            yield msg.rawbytes()
        else:
            serialization_start_time = time.time()
            frames = frame_utils.get_frames(msg.msg_type,
                                            msg.rawbytes(),
                                            eth_constants.DEFAULT_FRAME_PROTOCOL_ID,
                                            eth_constants.DEFAULT_FRAME_SIZE)
            eth_gateway_stats_service.log_serialized_message(time.time() - serialization_start_time)

            assert frames
            logger.debug("Broke message into {0} frames.".format(len(frames)))

            encryption_start_time = time.time()
            for frame in frames:
                yield self.rlpx_cipher.encrypt_frame(frame)
            eth_gateway_stats_service.log_encrypted_message(time.time() - encryption_start_time)

    def _enqueue_auth_message(self):
        auth_msg_bytes = self._get_auth_msg_bytes()
        logger.info("Enqueued auth bytes on: {}".format(self.connection))
        self.connection.enqueue_msg_bytes(auth_msg_bytes)

    def _enqueue_auth_ack_message(self):
        auth_ack_msg_bytes = self._get_auth_ack_msg_bytes()
        logger.info("Enqueued auth ack bytes on: {}".format(self.connection))
        self.connection.enqueue_msg_bytes(auth_ack_msg_bytes)

    def _enqueue_hello_message(self):
        public_key = self.node.get_public_key()

        hello_msg = HelloEthProtocolMessage(None,
                                            eth_constants.P2P_PROTOCOL_VERSION,
                                            f"{gateway_constants.GATEWAY_PEER_NAME} {self.node.opts.source_version}",
                                            eth_constants.CAPABILITIES,
                                            self.connection.external_port,
                                            public_key)

        logger.info("Enqueued hello bytes on: {}".format(self.connection))
        self.connection.enqueue_msg(hello_msg)

    def _enqueue_status_message(self):
        logger.debug("Sending status message")

        network_id = self.node.opts.network_id
        chain_difficulty = int(self.node.opts.chain_difficulty, 16)
        chain_head_hash = convert.hex_to_bytes(self.node.opts.genesis_hash)
        genesis_hash = convert.hex_to_bytes(self.node.opts.genesis_hash)

        status_msg = StatusEthProtocolMessage(None,
                                              eth_constants.ETH_PROTOCOL_VERSION,
                                              network_id,
                                              chain_difficulty,
                                              chain_head_hash,
                                              genesis_hash)

        self.connection.enqueue_msg(status_msg)

    def _enqueue_disconnect_message(self, disconnect_reason):
        logger.debug("Sending disconnect message")

        disconnect_msg = DisconnectEthProtocolMessage(None, [disconnect_reason])
        self.connection.enqueue_msg(disconnect_msg)

    def _get_auth_msg_bytes(self):
        auth_msg_bytes = self.rlpx_cipher.create_auth_message()
        auth_msg_bytes_encrypted = self.rlpx_cipher.encrypt_auth_message(auth_msg_bytes)

        return bytearray(auth_msg_bytes_encrypted)

    def _get_auth_ack_msg_bytes(self):
        auth_ack_msg_bytes = self.rlpx_cipher.create_auth_ack_message()
        auth_ack_msg_bytes_encrypted = self.rlpx_cipher.encrypt_auth_ack_message(auth_ack_msg_bytes)

        return bytearray(auth_ack_msg_bytes_encrypted)

    def _finalize_handshake(self):
        self._handshake_complete = True

        logger.debug("Setting up cipher.")
        self.rlpx_cipher.setup_cipher()

        self._last_ping_pong_time = time.time()
        self.node.alarm_queue.register_alarm(eth_constants.PING_PONG_INTERVAL_SEC, self._ping_timeout)

    def _handshake_timeout(self):
        if not self._handshake_complete:
            logger.debug("Handshake was not completed within defined timeout. Closing connection.")
            self.connection.mark_for_close(force_destroy_now=True)
        else:
            logger.debug("Handshake completed within defined timeout.")
        return 0

    def _ping_timeout(self):
        if self.connection.state & ConnectionState.MARK_FOR_CLOSE:
            return 0

        time_since_last_ping_pong = time.time() - self._last_ping_pong_time
        logger.debug("Ping time out: {} seconds since last ping / pong received from node"
                     .format(time_since_last_ping_pong))

        if time_since_last_ping_pong > eth_constants.PING_PONG_TIMEOUT_SEC:
            logger.debug("Node has not replied with ping / pong for {0} seconds, more than {1}. Disconnecting."
                         .format(time_since_last_ping_pong, eth_constants.PING_PONG_TIMEOUT_SEC))
            self._enqueue_disconnect_message(eth_constants.DISCONNECT_REASON_TIMEOUT)
            self.node.alarm_queue.register_alarm(eth_constants.DISCONNECT_DELAY_SEC,
                                                            self.connection.mark_for_close)
            return 0

        if time_since_last_ping_pong > eth_constants.PING_PONG_INTERVAL_SEC:
            self.connection.send_ping()
        else:
            logger.debug("Last ping / pong was received {0} seconds ago. No actions needed."
                         .format(time_since_last_ping_pong))
