import time
import typing
from typing import TYPE_CHECKING, TypeVar

from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.messages.abstract_block_message import AbstractBlockMessage
from bxcommon.network.abstract_socket_connection_protocol import AbstractSocketConnectionProtocol
from bxcommon.utils import memory_utils
from bxcommon.utils.stats import stats_format, hooks
from bxcommon.utils.stats.block_stat_event_type import BlockStatEventType
from bxcommon.utils.stats.block_statistics_service import block_stats
from bxgateway import gateway_constants
from bxutils import logging
from bxgateway import log_messages

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

GatewayNode = TypeVar("GatewayNode", bound="AbstractGatewayNode")

logger = logging.get_logger(__name__)


class AbstractGatewayBlockchainConnection(AbstractConnection[GatewayNode]):
    CONNECTION_TYPE = ConnectionType.BLOCKCHAIN_NODE

    def __init__(self, sock: AbstractSocketConnectionProtocol, node: GatewayNode):
        super(AbstractGatewayBlockchainConnection, self).__init__(sock, node)

        if node.opts.tune_send_buffer_size:
            transport = sock.transport
            assert transport is not None

            # Requires OS tuning (for Linux here; MacOS and windows require different commands):
            # echo 'net.core.wmem_max=16777216' >> /etc/sysctl.conf && sysctl -p
            # cat /proc/sys/net/core/wmem_max # to check

            # pyre-fixme[16]: `Transport` has no attribute `get_write_buffer_limits`.
            _, previous_buffer_size = transport.get_write_buffer_limits()
            try:
                transport.set_write_buffer_limits(high=gateway_constants.BLOCKCHAIN_SOCKET_SEND_BUFFER_SIZE)
                self.log_debug(
                    "Setting socket send buffer size for blockchain connection to {}",
                    transport.get_write_buffer_limits()
                )
            except Exception as e:
                self.log_warning(log_messages.SOCKET_BUFFER_SEND_FAIL, e)

            # Linux systems generally set the value to 2x what was passed in, so just make sure
            # the socket buffer is at least the size set
            _, set_buffer_size = transport.get_write_buffer_limits()
            if set_buffer_size < gateway_constants.BLOCKCHAIN_SOCKET_SEND_BUFFER_SIZE:
                self.log_warning(log_messages.SET_SOCKET_BUFFER_SIZE, set_buffer_size, previous_buffer_size)
                # transport.set_write_buffer_limits(high=previous_buffer_size)

        self.connection_protocol = None
        self.is_server = False

        self.can_send_pings = True
        self.pong_timeout_enabled = True

    def __hash__(self):
        return hash((self.endpoint.ip_address, self.endpoint.port))

    def __eq__(self, other) -> bool:
        if not isinstance(other, AbstractGatewayBlockchainConnection):
            return False
        return self.endpoint.ip_address == other.endpoint.ip_address and self.endpoint.port == other.endpoint.port

    def advance_bytes_written_to_socket(self, bytes_sent):
        if self.message_tracker and self.message_tracker.is_sending_block_message():
            assert self.node.opts.track_detailed_sent_messages

            entry = self.message_tracker.messages[0]
            super(AbstractGatewayBlockchainConnection, self).advance_bytes_written_to_socket(bytes_sent)

            if not self.message_tracker.is_sending_block_message():
                block_message = typing.cast(AbstractBlockMessage, entry.message)
                block_message_queue_time = entry.queued_time
                block_message_length = entry.length
                block_hash = block_message.block_hash()
                handling_time, relay_desc = self.node.track_block_from_bdn_handling_ended(block_hash)
                block_stats.add_block_event_by_block_hash(block_hash,
                                                          BlockStatEventType.BLOCK_SENT_TO_BLOCKCHAIN_NODE,
                                                          network_num=self.network_num,
                                                          more_info="{} in {}; Handled in {}; R - {}; {}".format(
                                                              stats_format.byte_count(
                                                                  block_message_length
                                                              ),
                                                              stats_format.timespan(
                                                                  block_message_queue_time,
                                                                  time.time()
                                                              ),
                                                              stats_format.duration(handling_time),
                                                              relay_desc,
                                                              block_message.extra_stats_data()
                                                          ))
        else:
            super(AbstractGatewayBlockchainConnection, self).advance_bytes_written_to_socket(bytes_sent)

    def log_connection_mem_stats(self) -> None:
        """
        logs the connection's memory stats
        """
        super(AbstractGatewayBlockchainConnection, self).log_connection_mem_stats()
        t0 = time.time()
        class_name = self.__class__.__name__
        if self.node.message_converter is not None:
            hooks.add_obj_mem_stats(
                class_name,
                self.network_num,
                self.node.message_converter,
                "message_converter",
                memory_utils.ObjectSize(
                    "message_converter", memory_utils.get_special_size(self.node.message_converter).size,
                    is_actual_size=True
                ),
                object_item_count=1,
                object_type=memory_utils.ObjectType.META,
                size_type=memory_utils.SizeType.SPECIAL
            )
        t_m_c_diff = round(time.time() - t0, 3)
        logger.debug("Recording {} message_converter MemoryStats took: {} seconds", class_name, t_m_c_diff)

    def get_connection_state_details(self):
        """
        Returns details of the current connection state. Used to submit details to SDN on disconnect.
        :return:
        """
        return f"Connection state: {self.state}"

    def dispose(self):
        super().dispose()

        if self.CONNECTION_TYPE == ConnectionType.BLOCKCHAIN_NODE:
            self.node.on_blockchain_connection_destroyed(self)
        elif self.CONNECTION_TYPE == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            if self.node.remote_node_conn is None or self.node.remote_node_conn == self:
                self.node.on_remote_blockchain_connection_destroyed(self)
            else:
                logger.warning(log_messages.CLOSE_CONNECTION_ATTEMPT,
                               self.peer_desc, self.node.remote_node_conn.peer_desc)
