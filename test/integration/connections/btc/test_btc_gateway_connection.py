import random
import time
from unittest import skip

from mock import MagicMock

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import LOCALHOST
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services import sdn_http_service
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.network_thread import NetworkThread
from bxgateway.connections.btc.btc_gateway_node import BtcGatewayNode


@skip("These dont work yet either")
class BtcGatewayConnectionTest(AbstractTestCase):
    """
    These are somewhat slower test cases that rely on spinning up the entire network event loop and processing events.
    If they, rerun them once or twice (possibly with increasing the sleep timeout) before debugging what actually
    failed.
    """

    def test_gateway_to_gateway_connection_initialization(self):
        cli_peer_gateway_address = OutboundPeerModel(LOCALHOST, 8001)
        sdn_peer_gateway_address = OutboundPeerModel(LOCALHOST, 8002)

        sdn_http_service.fetch_gateway_peers = MagicMock(return_value=[sdn_peer_gateway_address])
        sdn_http_service.fetch_relay_peers = MagicMock(return_value=[])

        opts = helpers.get_gateway_opts(8000, node_id="sut", peer_gateways=[cli_peer_gateway_address],
                                        include_default_btc_args=True)

        cli_peer_gateway = BtcGatewayNode(helpers.get_gateway_opts(8001, include_default_btc_args=True))
        sdn_peer_gateway = BtcGatewayNode(helpers.get_gateway_opts(8002, include_default_btc_args=True))

        threads = [NetworkThread(cli_peer_gateway), NetworkThread(sdn_peer_gateway)]
        map(NetworkThread.start, threads)

        sut_thread = NetworkThread(BtcGatewayNode(opts))
        sut_thread.start()

        time.sleep(4)

        self.assertTrue(sut_thread.node.connection_pool.has_connection(LOCALHOST, 8001))
        self.assertTrue(sut_thread.node.connection_pool.has_connection(LOCALHOST, 8002))

        cli_peer_connection = sut_thread.node.connection_pool.get_byipport(LOCALHOST, 8001)
        sdn_peer_connection = sut_thread.node.connection_pool.get_byipport(LOCALHOST, 8002)

        self.assertEquals(cli_peer_connection.connection_type, ConnectionType.GATEWAY)
        self.assertEquals(sdn_peer_connection.connection_type, ConnectionType.GATEWAY)

        map(NetworkThread.close, threads)
        sut_thread.close()

    def test_gateway_to_gateway_connection_duplicate_connection_resolution(self):
        # seed for consistent ordering generation results
        random.seed(0)

        main_gateway_address = OutboundPeerModel(LOCALHOST, 8000)
        peer_gateway_address = OutboundPeerModel(LOCALHOST, 8001)

        main_opts = helpers.get_gateway_opts(8000, node_id="main", include_default_btc_args=True)
        peer_opts = helpers.get_gateway_opts(8001, node_id="peer", include_default_btc_args=True)

        def fetch_gateway_peers(node_id):
            if node_id == "main":
                return [peer_gateway_address]
            else:
                return [main_gateway_address]

        sdn_http_service.fetch_gateway_peers = fetch_gateway_peers
        sdn_http_service.fetch_relay_peers = MagicMock(return_value=[])

        main_thread = NetworkThread(BtcGatewayNode(main_opts))
        peer_thread = NetworkThread(BtcGatewayNode(peer_opts))

        main_thread.start()
        peer_thread.start()

        # longer, for hello messages to be passed to each other
        time.sleep(6)

        self.assertEquals(1, len(filter(lambda conn: conn.connection_type == ConnectionType.GATEWAY,
                                        main_thread.node.connection_pool)))
        self.assertEquals(1, len(filter(lambda conn: conn.connection_type == ConnectionType.GATEWAY,
                                        peer_thread.node.connection_pool)))

        main_thread.close()
        peer_thread.close()
