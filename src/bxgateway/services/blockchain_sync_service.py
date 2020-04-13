from collections import defaultdict

from bxutils import logging
from bxgateway import log_messages

from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType

from bxgateway import gateway_constants
from bxgateway.messages.gateway.blockchain_sync_request_message import BlockchainSyncRequestMessage
from bxgateway.messages.gateway.blockchain_sync_response_message import BlockchainSyncResponseMessage

logger = logging.get_logger(__name__)


class BlockchainSyncService(object):
    """
    Service for passing blockchain sync messages requests (e.g. getheaders, getblocks) and responses back and forth
    between nodes via a bloXroute controlled connection.

    NOTE:
    This service is currently unused, as we are directly connecting the gateway node to a remote bloXroute owned
    blockchain node for synchronizing chainstate. This class is left here as scaffolding for future work for a
    more on-network chainstate synchronization.
    """

    def __init__(self, node, callback_mapping):
        self.node = node

        # request => response type mapping
        self.callback_mapping = callback_mapping

        # expected response command => alarm for retrying with broadcast
        self.awaiting_remote_callback_alarms = {}

        # map of command => connections awaiting local callback
        self.awaiting_local_callback_connections = defaultdict(set)

    def is_valid_blockchain_sync_message(self, command):
        return command in self.callback_mapping

    def am_waiting_for_remote_callback(self, command):
        """
        If node is waiting for a remote callback for command.
        :param command: awaiting response message type
        """
        return command in self.awaiting_remote_callback_alarms

    def make_sync_request(self, command, message):
        """
        Request blockchain sync with remote node.

        :param command blockchain message type
        :param message: blockchain request message to send out
        """
        if not self.is_valid_blockchain_sync_message(command):
            raise ValueError("Unknown blockchain request/response mapping for message of type: {}."
                             .format(command))

        request_message = BlockchainSyncRequestMessage(command, message.rawbytes())
        response_command = self.callback_mapping[command]
        retry_alarm_id = self.node.alarm_queue.register_alarm(gateway_constants.BLOCKCHAIN_SYNC_BROADCAST_DELAY_S,
                                                              lambda: self._retry_request_with_broadcast(
                                                                  command, message
                                                              ))
        self.awaiting_remote_callback_alarms[response_command] = retry_alarm_id

        preferred_connection = self.node.get_preferred_gateway_connection()
        if preferred_connection is not None:
            preferred_connection.enqueue_msg(request_message, prepend=True)

    def _retry_request_with_broadcast(self, command, message):
        if self.am_waiting_for_remote_callback(self.callback_mapping[command]):
            self.node.broadcast(BlockchainSyncRequestMessage(command, message.rawbytes()),
                                connection_types=[ConnectionType.GATEWAY])

        return constants.CANCEL_ALARMS

    def process_remote_sync_request(self, command, message, connection):
        """
        Pass remote blockchain sync query to blockchain node.
        :param command request blockchain message type
        :param message: blockchain message request from remote
        :param connection: Connection that requested sync
        """
        if self.is_valid_blockchain_sync_message(command):
            response_command = self.callback_mapping[command]
            if len(self.awaiting_local_callback_connections[response_command]) == 0:
                self.node.send_msg_to_node(message)
            self.awaiting_local_callback_connections[response_command].add(connection)
        else:
            logger.warning(log_messages.UNKNOWN_CALLBACK_MAPPING_WITH_SYNC_REQ.format(command))

    def process_remote_sync_response(self, command, message, connection):
        """
        Pass remote blockchain sync message response to blockchain node.
        Sets the preferred connection from broadcast result.

        :param command response blockchain message type
        :param message: blockchain message response from remote
        :param connection: Connection that returned response
        """
        if self.am_waiting_for_remote_callback(command):
            self.node.alarm_queue.unregister_alarm(self.awaiting_remote_callback_alarms[command])
            del self.awaiting_remote_callback_alarms[command]

            self.node.set_preferred_gateway_connection(connection)
            self.node.send_msg_to_node(message)
        else:
            logger.warning(log_messages.UNSOLICITED_SYNC_RESPONSE.format(command))

    def process_local_sync_response(self, command, message):
        """
        Pass local blockchain sync message back to remote asker.
        :param command blockchain message type
        :param message: blockchain message response from local blockchain node
        """
        for conn in self.awaiting_local_callback_connections[command]:
            conn.enqueue_msg(BlockchainSyncResponseMessage(command, message.rawbytes()))

        self.awaiting_local_callback_connections[command].clear()
