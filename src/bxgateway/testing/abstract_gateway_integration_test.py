from abc import abstractmethod
from typing import Type, List

from bxgateway.gateway_opts import GatewayOpts
from bxgateway.testing import gateway_helpers
from bxcommon import constants
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway.testing.mocks import full_gateway_node


class AbstractGatewayIntegrationTest(AbstractTestCase):
    """
    Test scaffolding for a two gateway network, with connections to a simulated relay and blockchain connection.
    """

    def setUp(self) -> None:
        if constants.USE_EXTENSION_MODULES:
            helpers.set_extensions_parallelism()

        self.gateway_info_1 = full_gateway_node.of_type(self.gateway_class(), self.gateway_opts_1())
        self.gateway_info_2 = full_gateway_node.of_type(self.gateway_class(), self.gateway_opts_2())

        (
            self.gateway_1,
            self.blockchain_fileno_1,
            self.relay_fileno_1,
            self.gateway_fileno_1,
            _,
            _,
            _,
            self.blockchain_connection_1,
            self.gateway_connection_1,
            self.relay_connection_1,
        ) = self.gateway_info_1

        (
            self.gateway_2,
            self.blockchain_fileno_2,
            self.relay_fileno_2,
            self.gateway_fileno_2,
            _,
            _,
            _,
            self.blockchain_connection_2,
            self.gateway_connection_2,
            self.relay_connection_2,
        ) = self.gateway_info_2

    @abstractmethod
    def gateway_class(self) -> Type[AbstractGatewayNode]:
        pass

    @abstractmethod
    def gateway_opts_1(self) -> GatewayOpts:
        return gateway_helpers.get_gateway_opts(9000)

    @abstractmethod
    def gateway_opts_2(self) -> GatewayOpts:
        return gateway_helpers.get_gateway_opts(9001)

    def gateway_1_receive_message_from_blockchain(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_1, self.blockchain_fileno_1, message.rawbytes())

    def gateway_1_receive_message_from_relay(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_1, self.relay_fileno_1, message.rawbytes())

    def gateway_1_receive_message_from_gateway(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_1, self.gateway_fileno_1, message.rawbytes())

    def gateway_2_receive_message_from_blockchain(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_2, self.blockchain_fileno_2, message.rawbytes())

    def gateway_2_receive_message_from_relay(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_2, self.relay_fileno_2, message.rawbytes())

    def gateway_2_receive_message_from_gateway(self, message: AbstractMessage):
        helpers.receive_node_message(self.gateway_2, self.gateway_fileno_2, message.rawbytes())

    def gateway_1_get_queued_messages_for_blockchain(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_1, self.blockchain_fileno_1)

    def gateway_1_get_queued_messages_for_relay(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_1, self.relay_fileno_1)

    def gateway_1_get_queued_messages_for_gateway(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_1, self.gateway_fileno_1)

    def gateway_2_get_queued_messages_for_blockchain(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_2, self.blockchain_fileno_2)

    def gateway_2_get_queued_messages_for_relay(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_2, self.relay_fileno_2)

    def gateway_2_get_queued_messages_for_gateway(self) -> List[AbstractMessage]:
        return helpers.get_queued_node_messages(self.gateway_2, self.gateway_fileno_2)
