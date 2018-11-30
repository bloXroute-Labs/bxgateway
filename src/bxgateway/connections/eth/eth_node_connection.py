import time

from bxcommon.connections.connection_state import ConnectionState
from bxcommon.utils import logger
from bxgateway import eth_constants
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.messages.eth.eth_message_converter import EthMessageConverter
from bxgateway.messages.eth.protocol.disconnect_eth_protocol_message import DisconnectEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_factory import EthProtocolMessageFactory
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.hello_eth_protocol_message import HelloEthProtocolMessage
from bxgateway.messages.eth.protocol.ping_eth_protocol_message import PingEthProtocolMessage
from bxgateway.messages.eth.protocol.pong_eth_protocol_message import PongEthProtocolMessage
from bxgateway.messages.eth.protocol.raw_eth_protocol_message import RawEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.utils.eth import rlp_utils, frame_utils
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher


class EthNodeConnection(AbstractGatewayBlockchainConnection):

    def __init__(self, sock, address, node, from_me):
        super(EthNodeConnection, self).__init__(sock, address, node, from_me)

        self.message_handlers = {
            EthProtocolMessageType.AUTH: self.msg_auth,
            EthProtocolMessageType.AUTH_ACK: self.msg_auth_ack,
            EthProtocolMessageType.HELLO: self.msg_hello,
            EthProtocolMessageType.DISCONNECT: self.msg_disconnect,
            EthProtocolMessageType.PING: self.msg_ping,
            EthProtocolMessageType.PONG: self.msg_pong,
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.TRANSACTIONS: self.msg_tx,
            EthProtocolMessageType.NEW_BLOCK_HASHES: self.msg_new_block_hashes,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers,
            EthProtocolMessageType.BLOCK_HEADERS: self.msg_block_headers,
            EthProtocolMessageType.GET_BLOCK_BODIES: self.msg_get_block_bodies,
            EthProtocolMessageType.BLOCK_BODIES: self.msg_get_block_bodies,
            EthProtocolMessageType.NEW_BLOCK: self.msg_block,
        }

        self.message_converter = EthMessageConverter()

        self.can_send_pings = True
        self.ping_message = PingEthProtocolMessage(None)
        self.pong_message = PongEthProtocolMessage(None)

        self._last_ping_pong_time = None

        remote_public_key = self.node.get_remote_public_key()

        self._is_handshake_initiator = remote_public_key is not None

        self._rlpx_cipher = RLPxCipher(self._is_handshake_initiator,
                                       self.node.get_private_key(),
                                       remote_public_key=remote_public_key)

        self.message_factory = EthProtocolMessageFactory(self._rlpx_cipher)
        expected_first_msg_type = EthProtocolMessageType.AUTH_ACK if self._is_handshake_initiator \
            else EthProtocolMessageType.AUTH
        self.message_factory.set_expected_msg_type(expected_first_msg_type)

        self._handshake_complete = False

        if self._is_handshake_initiator:
            logger.debug("Public key of remote node is known. Starting handshake.")
            self._enqueue_auth_message()
        else:
            logger.debug("Public key of remote node is not know. Waiting for handshake request.")
            self.node.alarm_queue.register_alarm(eth_constants.HANDSHAKE_TIMEOUT_SEC, self._handshake_timeout)

    def enqueue_msg(self, msg):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        if isinstance(msg, RawEthProtocolMessage):
            super(EthNodeConnection, self).enqueue_msg(msg)
            return

        # Break message into frames
        frames = frame_utils.get_frames(msg.msg_type,
                                        msg.rawbytes(),
                                        eth_constants.DEFAULT_FRAME_PROTOCOL_ID,
                                        eth_constants.DEFAULT_FRAME_SIZE)
        assert frames
        logger.debug("Broke message into {0} frames.".format(len(frames)))

        for frame in frames:
            frame_bytes = self._rlpx_cipher.encrypt_frame(frame)
            self.enqueue_msg_bytes(frame_bytes)

    def msg_auth(self, msg):
        logger.debug("Begin process auth message")
        msg_bytes = msg.rawbytes()
        decrypted_auth_msg, size = self._rlpx_cipher.decrypt_auth_message(msg_bytes)
        if decrypted_auth_msg is None:
            logger.debug("Auth message is not complete. Continue waiting for more bytes.")
            return

        self.inputbuf.remove_bytes(size)
        self._rlpx_cipher.parse_auth_message(decrypted_auth_msg)

        self._enqueue_auth_ack_message()
        self._finalize_handshake()
        self._enqueue_hello_message()
        self.message_factory.reset_expected_msg_type()

    def msg_auth_ack(self, msg):
        logger.debug("Begin process auth ack message")
        auth_ack_msg_bytes = msg.rawbytes()
        self._rlpx_cipher.decrypt_auth_ack_message(bytes(auth_ack_msg_bytes))
        self._finalize_handshake()
        self._enqueue_hello_message()
        self.message_factory.reset_expected_msg_type()

    def msg_ping(self, msg):
        super(EthNodeConnection, self).msg_ping(msg)
        self._last_ping_pong_time = time.time()

    def msg_pong(self, msg):
        super(EthNodeConnection, self).msg_pong(msg)
        self._last_ping_pong_time = time.time()

    def msg_hello(self, msg):
        logger.debug("Hello message received.")
        self._enqueue_status_message()

    def msg_status(self, msg):
        logger.debug("Status message received.")

        self.state |= ConnectionState.ESTABLISHED
        self.node.node_conn = self

        self.send_ping()

    def msg_disconnect(self, msg):
        logger.debug("Disconnect message received. Disconnect reason {0}.".format(msg.get_reason()))
        self.mark_for_close()

    def msg_new_block_hashes(self, msg):
        logger.debug("New block hashes message received. Ignoring.")

    def msg_get_block_headers(self, msg):
        logger.debug("Get Block Headers message received. Ignoring.")

    def msg_block_headers(self, msg):
        logger.debug("Block Headers message received. Ignoring.")

    def msg_get_block_bodies(self, msg):
        logger.debug("Get Block Bodies message received. Ignoring.")

    def msg_block_bodies(self, msg):
        logger.debug("Block bodies message received. Ignoring.")

    def _enqueue_auth_message(self):
        auth_msg_bytes = self._get_auth_msg_bytes()
        self.enqueue_msg_bytes(auth_msg_bytes)

    def _enqueue_auth_ack_message(self):
        auth_ack_msg_bytes = self._get_auth_ack_msg_bytes()
        self.enqueue_msg_bytes(auth_ack_msg_bytes)

    def _enqueue_hello_message(self):
        logger.debug("Sending hello message")

        public_key = self.node.get_public_key()

        hello_msg = HelloEthProtocolMessage(None,
                                            eth_constants.P2P_PROTOCOL_VERSION,
                                            self.node.opts.bloxroute_version,
                                            eth_constants.CAPABILITIES,
                                            self.external_port,
                                            public_key)

        self.enqueue_msg(hello_msg)

    def _enqueue_status_message(self):
        logger.debug("Sending status message")

        network_id = self.node.opts.network_id
        chain_difficulty = int(self.node.opts.chain_difficulty, 16)
        chain_head_hash = rlp_utils.decode_hex(self.node.opts.genesis_hash)
        genesis_hash = rlp_utils.decode_hex(self.node.opts.genesis_hash)

        status_msg = StatusEthProtocolMessage(None,
                                              eth_constants.ETH_PROTOCOL_VERSION,
                                              network_id,
                                              chain_difficulty,
                                              chain_head_hash,
                                              genesis_hash)

        self.enqueue_msg(status_msg)

    def _enqueue_disconnect_message(self, disconnect_reason):
        logger.debug("Sending disconnect message")

        disconnect_msg = DisconnectEthProtocolMessage(None, [disconnect_reason])
        self.enqueue_msg(disconnect_msg)

    def _get_auth_msg_bytes(self):
        auth_msg_bytes = self._rlpx_cipher.create_auth_message()
        auth_msg_bytes_encrypted = self._rlpx_cipher.encrypt_auth_message(auth_msg_bytes)

        return bytearray(auth_msg_bytes_encrypted)

    def _get_auth_ack_msg_bytes(self):
        auth_ack_msg_bytes = self._rlpx_cipher.create_auth_ack_message()
        auth_ack_msg_bytes_encrypted = self._rlpx_cipher.encrypt_auth_ack_message(auth_ack_msg_bytes)

        return bytearray(auth_ack_msg_bytes_encrypted)

    def _finalize_handshake(self):
        self._handshake_complete = True

        logger.debug("Setting up cipher.")
        self._rlpx_cipher.setup_cipher()

        self._last_ping_pong_time = time.time()
        self.node.alarm_queue.register_alarm(eth_constants.PING_PONG_INTERVAL_SEC, self._ping_timeout)

    def _handshake_timeout(self):
        if not self._handshake_complete:
            logger.debug("Handshake was not completed within defined timeout. Closing connection.")
            self.mark_for_close()
        else:
            logger.debug("Handshake completed within defined timeout.")
        return 0

    def _ping_timeout(self):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return 0

        time_since_last_ping_pong = time.time() - self._last_ping_pong_time
        logger.debug("Ping time out: {} seconds since last ping / pong received from node"
                     .format(time_since_last_ping_pong))

        if (time_since_last_ping_pong > eth_constants.PING_PONG_TIMEOUT_SEC):
            logger.debug("Node has not replied with ping / pong for {0} seconds, more than {1}. Disconnecting."
                         .format(time_since_last_ping_pong, eth_constants.PING_PONG_TIMEOUT_SEC))
            self._enqueue_disconnect_message(eth_constants.DISCONNECT_REASON_TIMEOUT)
            self.node.alarm_queue.register_alarm(eth_constants.DISCONNECT_DELAY_SEC, self.mark_for_close)
            return 0

        if time_since_last_ping_pong > eth_constants.PING_PONG_INTERVAL_SEC:
            self.send_ping()
        else:
            logger.debug("Last ping / pong was received {0} seconds ago. No actions needed."
                         .format(time_since_last_ping_pong))

        return eth_constants.PING_PONG_INTERVAL_SEC
