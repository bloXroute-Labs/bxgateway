import time
from abc import ABCMeta
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast, Optional, Union

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.utils import convert
from bxgateway import gateway_constants
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxgateway.connections.abstract_blockchain_connection_protocol import AbstractBlockchainConnectionProtocol
from bxgateway.messages.eth.protocol.block_headers_eth_protocol_message import BlockHeadersEthProtocolMessage
from bxgateway.messages.eth.protocol.disconnect_eth_protocol_message import DisconnectEthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxgateway.messages.eth.protocol.hello_eth_protocol_message import HelloEthProtocolMessage
from bxgateway.messages.eth.protocol.pong_eth_protocol_message import PongEthProtocolMessage
from bxgateway.messages.eth.protocol.raw_eth_protocol_message import RawEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message import StatusEthProtocolMessage
from bxgateway.messages.eth.protocol.status_eth_protocol_message_v63 import StatusEthProtocolMessageV63
from bxgateway.utils.eth import frame_utils
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher
from bxgateway.utils.stats.eth.eth_gateway_stats_service import eth_gateway_stats_service
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)


@dataclass
class EthConnectionProtocolStatus:
    auth_message_sent: bool = False
    auth_message_received: bool = False
    auth_ack_message_sent: bool = False
    auth_ack_message_received: bool = False
    hello_message_sent: bool = False
    hello_message_received: bool = False
    status_message_sent: bool = False
    status_message_received: bool = False
    disconnect_message_received: bool = False
    disconnect_reason: Optional[int] = None


class EthBaseConnectionProtocol(AbstractBlockchainConnectionProtocol, metaclass=ABCMeta):
    node: "EthGatewayNode"

    def __init__(self, connection, is_handshake_initiator, rlpx_cipher: RLPxCipher):
        super(EthBaseConnectionProtocol, self).__init__(
            connection,
            block_cleanup_poll_interval_s=eth_common_constants.BLOCK_CLEANUP_NODE_BLOCK_LIST_POLL_INTERVAL_S
        )

        self.node = cast("EthGatewayNode", connection.node)

        self.rlpx_cipher = rlpx_cipher
        self.connection_status = EthConnectionProtocolStatus()

        self._last_ping_pong_time: Optional[float] = None
        self._handshake_complete = False

        connection.hello_messages = [
            EthProtocolMessageType.AUTH,
            EthProtocolMessageType.AUTH_ACK,
            EthProtocolMessageType.HELLO,
            EthProtocolMessageType.STATUS,
            # Ethereum Parity sends PING message before handshake is completed
            EthProtocolMessageType.PING,
            EthProtocolMessageType.DISCONNECT
        ]

        connection.message_handlers = {
            EthProtocolMessageType.AUTH: self.msg_auth,
            EthProtocolMessageType.AUTH_ACK: self.msg_auth_ack,
            EthProtocolMessageType.HELLO: self.msg_hello,
            EthProtocolMessageType.STATUS: self.msg_status,
            EthProtocolMessageType.DISCONNECT: self.msg_disconnect,
            EthProtocolMessageType.PING: self.msg_ping,
            EthProtocolMessageType.PONG: self.msg_pong,
            EthProtocolMessageType.GET_BLOCK_HEADERS: self.msg_get_block_headers
        }
        connection.pong_message = PongEthProtocolMessage(None)

        self._waiting_checkpoint_headers_request = True

        if is_handshake_initiator:
            self.connection.log_trace("Public key is known. Starting handshake.")
            self._enqueue_auth_message()
        else:
            self.connection.log_trace("Public key is unknown. Waiting for handshake request.")
            self.node.alarm_queue.register_alarm(eth_common_constants.HANDSHAKE_TIMEOUT_SEC, self._handshake_timeout)

    def msg_auth(self, msg):
        self.connection.log_trace("Beginning processing of auth message.")
        self.connection_status.auth_message_received = True
        msg_bytes = msg.rawbytes()
        decrypted_auth_msg, size = self.rlpx_cipher.decrypt_auth_message(bytes(msg_bytes))
        if decrypted_auth_msg is None:
            self.connection.log_trace("Auth message is incomplete. Waiting for more bytes.")
            return

        self.rlpx_cipher.parse_auth_message(decrypted_auth_msg)

        self._enqueue_auth_ack_message()
        self._finalize_handshake()
        self._enqueue_hello_message()
        self.connection.message_factory.reset_expected_msg_type()

    def msg_auth_ack(self, msg):
        self.connection.log_trace("Beginning processing of auth ack message.")
        self.connection_status.auth_ack_message_received = True
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

    def msg_hello(self, msg: HelloEthProtocolMessage):
        client_version_string = msg.get_client_version_string()
        version = msg.get_version()
        self.connection.log_info("Exchanging handshake messages with blockchain node {}, with version field {}.",
                                 client_version_string, version)
        self.connection_status.hello_message_received = True

    def msg_status(self, msg: Union[StatusEthProtocolMessage, StatusEthProtocolMessageV63]):
        self.connection.log_trace("Status message received.")
        try:
            protocol_version = msg.get_eth_version()
        except Exception:
            status_msg = StatusEthProtocolMessageV63(msg.rawbytes())
            protocol_version = status_msg.get_eth_version()
        else:
            status_msg = msg

        self.connection_status.status_message_received = True

        for peer in self.node.blockchain_peers:
            if self.node.is_blockchain_peer(self.connection.peer_ip, self.connection.peer_port):
                peer.connection_established = True

        chain_difficulty_from_status_msg = status_msg.get_chain_difficulty()
        chain_difficulty = int(self.node.opts.chain_difficulty, 16)
        fork_id = status_msg.get_fork_id()
        if isinstance(chain_difficulty_from_status_msg, int) and chain_difficulty_from_status_msg > chain_difficulty:
            chain_difficulty = chain_difficulty_from_status_msg
        self._enqueue_status_message(chain_difficulty, fork_id, protocol_version)

    def msg_disconnect(self, msg):
        self.connection_status.disconnect_message_received = True
        self.connection_status.disconnect_reason = msg.get_reason()

        self.connection.log_debug("Disconnect message was received from the blockchain node. Disconnect reason '{0}'.",
                                  self.connection_status.disconnect_reason)
        self.connection.mark_for_close()

    def msg_get_block_headers(self, msg):
        self.connection.log_trace("Replying with empty headers message to the get headers request")
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
                                            eth_common_constants.DEFAULT_FRAME_PROTOCOL_ID,
                                            eth_common_constants.DEFAULT_FRAME_SIZE)
            eth_gateway_stats_service.log_serialized_message(time.time() - serialization_start_time)

            assert frames
            self.connection.log_trace("Broke message into {} frames", len(frames))

            encryption_start_time = time.time()
            for frame in frames:
                yield self.rlpx_cipher.encrypt_frame(frame)
            eth_gateway_stats_service.log_encrypted_message(time.time() - encryption_start_time)

    def _enqueue_auth_message(self):
        auth_msg_bytes = self._get_auth_msg_bytes()
        self.connection.log_debug("Enqueued auth bytes.")
        self.connection.enqueue_msg_bytes(auth_msg_bytes)
        self.connection_status.auth_message_sent = True

    def _enqueue_auth_ack_message(self):
        auth_ack_msg_bytes = self._get_auth_ack_msg_bytes()
        self.connection.log_debug("Enqueued auth ack bytes.")
        self.connection.enqueue_msg_bytes(auth_ack_msg_bytes)
        self.connection_status.auth_ack_message_sent = True

    def _get_eth_protocol_version(self) -> int:
        for peer in self.node.blockchain_peers:
            if self.node.is_blockchain_peer(self.connection.peer_ip, self.connection.peer_port):
                return peer.blockchain_protocol_version
        return eth_common_constants.ETH_PROTOCOL_VERSION

    def _enqueue_hello_message(self):
        public_key = self.node.get_public_key()
        eth_protocol_version = eth_common_constants.ETH_PROTOCOL_VERSION
        if self.connection.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            eth_protocol_version = self._get_eth_protocol_version()
        elif self.connection.CONNECTION_TYPE == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            eth_protocol_version = self.node.remote_blockchain_protocol_version

        hello_msg = HelloEthProtocolMessage(
            None,
            eth_common_constants.P2P_PROTOCOL_VERSION,
            f"{gateway_constants.GATEWAY_PEER_NAME} {self.node.opts.source_version}".encode("utf-8"),
            ((b"eth", eth_protocol_version),),
            self.connection.external_port,
            public_key
        )
        self.connection.enqueue_msg(hello_msg)
        self.connection_status.hello_message_sent = True

    def _enqueue_status_message(self, chain_difficulty: int, fork_id, protocol_version: int):
        network_id = self.node.opts.network_id
        chain_head_hash = convert.hex_to_bytes(self.node.opts.genesis_hash)
        genesis_hash = chain_head_hash

        if protocol_version == gateway_constants.ETH_PROTOCOL_VERSION_63:
            status_msg = StatusEthProtocolMessageV63(
                None,
                protocol_version,
                network_id,
                chain_difficulty,
                chain_head_hash,
                genesis_hash,
            )
        else:
            status_msg = StatusEthProtocolMessage(
                None,
                protocol_version,
                network_id,
                chain_difficulty,
                chain_head_hash,
                genesis_hash,
                fork_id
            )

        self.connection.enqueue_msg(status_msg)
        self.connection_status.status_message_sent = True

    def _enqueue_disconnect_message(self, disconnect_reason):
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

        self.connection.log_trace("Setting up cipher.")
        self.rlpx_cipher.setup_cipher()

        self._last_ping_pong_time = time.time()
        self.node.alarm_queue.register_alarm(eth_common_constants.PING_PONG_INTERVAL_SEC, self._ping_timeout)

    def _handshake_timeout(self):
        if not self._handshake_complete:
            self.connection.log_debug("Handshake was not completed within defined timeout. Closing connection.")
            self.connection.mark_for_close()
        else:
            self.connection.log_trace("Handshake completed within defined timeout.")
        return 0

    def _ping_timeout(self) -> float:
        if not self.connection.is_alive():
            return 0

        last_ping_pong_time = self._last_ping_pong_time
        assert last_ping_pong_time is not None
        time_since_last_ping_pong = time.time() - last_ping_pong_time
        self.connection.log_trace("Ping timeout: {} seconds since last ping / pong received from node.",
                                  time_since_last_ping_pong)

        if time_since_last_ping_pong > eth_common_constants.PING_PONG_TIMEOUT_SEC:
            self.connection.log_debug("Node has not replied with ping / pong for {} seconds, more than {} limit."
                                      "Disconnecting", time_since_last_ping_pong, eth_common_constants.PING_PONG_TIMEOUT_SEC)
            self._enqueue_disconnect_message(eth_common_constants.DISCONNECT_REASON_TIMEOUT)
            self.node.alarm_queue.register_alarm(eth_common_constants.DISCONNECT_DELAY_SEC, self.connection.mark_for_close)

        return 0
