from mock import MagicMock

from bxgateway.testing import gateway_helpers
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.constants import LOCALHOST
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection


def make_spy_node(gateway_cls, port, **kwargs):
    opts = gateway_helpers.get_gateway_opts(port, **kwargs)
    if opts.use_extensions:
        helpers.set_extensions_parallelism()
    gateway_node = gateway_cls(opts, MockNodeSSLService(gateway_cls.NODE_TYPE, MagicMock()))
    gateway_node.broadcast = MagicMock(wraps=gateway_node.broadcast)
    gateway_node.requester = MagicMock()
    return gateway_node


def make_spy_connection(connection_cls, fileno, port, node, state=ConnectionState.ESTABLISHED):
    gateway_connection = connection_cls(
        MockSocketConnection(fileno, node, ip_address=LOCALHOST, port=port), node
    )
    if state == ConnectionState.ESTABLISHED:
        gateway_connection.on_connection_established()
    else:
        gateway_connection.state = state
    gateway_connection.enqueue_msg = MagicMock()
    return gateway_connection
