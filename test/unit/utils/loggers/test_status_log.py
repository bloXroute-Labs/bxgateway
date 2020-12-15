import os
import uuid
from typing import Tuple, Set

from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import config
from bxcommon.utils import model_loader
from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_type import ConnectionType
from bxgateway.utils.logging.status.blockchain_connection import BlockchainConnection
from bxgateway.utils.logging.status.connection_state import ConnectionState
from bxgateway.utils.logging.status.status_log import initialize, update, Diagnostics, Summary, Analysis, Environment, \
    Network, STATUS_FILE_NAME, ExtensionModulesState, GatewayStatus, InstallationType
from bxgateway.utils.logging.status import summary


class StatusLogTest(AbstractTestCase):

    def setUp(self):
        self.conn_pool = ConnectionPool()
        self.source_version = "v1.0.0"
        self.ip_address = "0.0.0.0"
        self.continent = "NA"
        self.country = "United States"
        self.account_id = None
        self.blockchain_peers: Set[BlockchainPeerInfo] = set()

        self.fileno1 = 1
        self.ip1 = "123.123.123.123"
        self.port1 = 1000
        self.node1 = MockNode(helpers.get_common_opts(1001, external_ip="128.128.128.128"))
        self.node_id1 = str(uuid.uuid1())
        self.conn1 = MockConnection(
            MockSocketConnection(self.fileno1, self.node1, ip_address=self.ip1, port=self.port1), self.node1
        )
        self.conn1.CONNECTION_TYPE = ConnectionType.RELAY_BLOCK

        self.fileno2 = 5
        self.ip2 = "234.234.234.234"
        self.port2 = 2000
        self.node2 = MockNode(helpers.get_common_opts(1003, external_ip="321.321.321.321"))
        self.node_id2 = str(uuid.uuid1())
        self.conn2 = MockConnection(
            MockSocketConnection(self.fileno2, self.node2, ip_address=self.ip2, port=self.port2), self.node2
        )
        self.conn2.CONNECTION_TYPE = ConnectionType.RELAY_TRANSACTION

        self.fileno3 = 6
        self.ip3 = "234.234.234.234"
        self.port3 = 3000
        self.node3 = MockNode(helpers.get_common_opts(1003, external_ip="213.213.213.213"))
        self.node_id3 = str(uuid.uuid1())
        self.conn3 = MockConnection(
            MockSocketConnection(self.fileno3, self.node3, ip_address=self.ip3, port=self.port3), self.node3
        )
        self.conn3.CONNECTION_TYPE = ConnectionType.BLOCKCHAIN_NODE
        self.blockchain_peers.add(BlockchainPeerInfo(self.ip3, self.port3))
        self.blockchain_conn3 = BlockchainConnection(self.ip3, str(self.port3), str(self.fileno3), None)

        self.fileno4 = 8
        self.ip4 = "111.222.111.222"
        self.port4 = 3000
        self.node4 = MockNode(helpers.get_common_opts(1003, external_ip="101.101.101.101"))
        self.node_id4 = str(uuid.uuid1())
        self.conn4 = MockConnection(
            MockSocketConnection(self.fileno4, self.node4, ip_address=self.ip4, port=self.port4), self.node4
        )
        self.conn4.CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

        self.fileno5 = 9
        self.ip5 = "123.456.456.456"
        self.port5 = 3000
        self.node5 = MockNode(helpers.get_common_opts(1003, external_ip="456.456.456.456"))
        self.node_id5 = str(uuid.uuid1())
        self.conn5 = MockConnection(
            MockSocketConnection(self.fileno5, self.node5, ip_address=self.ip5, port=self.port5), self.node5
        )
        self.conn5.CONNECTION_TYPE = ConnectionType.BLOCKCHAIN_NODE
        self.blockchain_conn5 = BlockchainConnection(self.ip5, str(self.port5), str(self.fileno5), None)

        self.quota_level = 0
        initialize(False, self.source_version, self.ip_address, self.continent, self.country, False, self.account_id, self.quota_level)

        path = config.get_data_file(STATUS_FILE_NAME)
        self.addCleanup(os.remove, path)

    def test_on_initialize_logger(self):
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        self.assertEqual(0, len(network_loaded.block_relays))
        self.assertEqual(0, len(network_loaded.transaction_relays))
        self.assertEqual(0, len(network_loaded.blockchain_nodes))
        self.assertEqual(0, len(network_loaded.remote_blockchain_nodes))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.OFFLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, None)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, None)
        self.assertEqual(summary_loaded.blockchain_node_connection_states, None)
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, None)
        self.assertEqual(summary_loaded.update_required, False)
        self.assertEqual(analysis_loaded.gateway_version, self.source_version)
        self.assertEqual(type(analysis_loaded.extensions_check), ExtensionModulesState)
        self.assertEqual(type(environment_loaded.installation_type), InstallationType)

    def test_on_get_connection_state_intialization(self):
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        self.assertEqual(0, len(network_loaded.block_relays))
        self.assertEqual(0, len(network_loaded.transaction_relays))
        self.assertEqual(0, len(network_loaded.blockchain_nodes))
        self.assertEqual(0, len(network_loaded.remote_blockchain_nodes))

    def test_on_update_one_connection(self):
        self.conn_pool.add(self.fileno1, self.ip1, self.port1, self.conn1)
        update(self.conn_pool, False, self.source_version, self.ip_address, self.continent, self.country, True,
               self.blockchain_peers, self.account_id, self.quota_level)
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relays[0]
        self.assertEqual(0, len(network_loaded.transaction_relays))
        self.assertEqual(1, len(network_loaded.blockchain_nodes))
        self.assertEqual(0, len(network_loaded.remote_blockchain_nodes))
        self.assertEqual(summary_loaded,
                         network_loaded.get_summary(self.ip_address, self.continent, self.country, True,
                                                    self.account_id, self.quota_level))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.WITH_ERRORS)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, ConnectionState.DISCONNECTED)
        self.assertEqual(
            summary_loaded.blockchain_node_connection_states,
            {f"{self.ip3} {self.port3}": ConnectionState.DISCONNECTED}
        )
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, ConnectionState.DISCONNECTED)
        self.assertEqual(summary_loaded.quota_level, summary.gateway_status_get_quota_level(self.quota_level))
        self.assertEqual(summary_loaded.update_required, True)
        self.assertEqual(block_relay_loaded.ip_address, self.ip1)
        self.assertEqual(block_relay_loaded.port, str(self.port1))
        self.assertEqual(block_relay_loaded.fileno, str(self.fileno1))

    def test_on_update_all_connections(self):
        self._add_connections()
        update(self.conn_pool, False, self.source_version, self.ip_address, self.continent, self.country, False,
               self.blockchain_peers, self.account_id, self.quota_level)
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relays[0]
        transaction_relay_loaded = network_loaded.transaction_relays[0]
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_nodes[0]
        for blockchain_node_loaded in network_loaded.blockchain_nodes:
            blockchain_node_loaded.connection_time = None

        self.assertEqual(summary_loaded, network_loaded.get_summary(self.ip_address, self.continent, self.country,
                                                                    False, self.account_id, self.quota_level))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.ONLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(
            summary_loaded.blockchain_node_connection_states,
            {
                f"{self.ip3} {self.port3}": ConnectionState.ESTABLISHED,
                f"{self.ip5} {self.port5}": ConnectionState.ESTABLISHED,
            }
        )
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.quota_level, summary.gateway_status_get_quota_level(self.quota_level))
        self.assertEqual(block_relay_loaded.ip_address, self.ip1)
        self.assertEqual(block_relay_loaded.port, str(self.port1))
        self.assertEqual(block_relay_loaded.fileno, str(self.fileno1))
        self.assertEqual(transaction_relay_loaded.ip_address, self.ip2)
        self.assertEqual(transaction_relay_loaded.port, str(self.port2))
        self.assertEqual(transaction_relay_loaded.fileno, str(self.fileno2))
        self.assertIn(self.blockchain_conn3, network_loaded.blockchain_nodes)
        self.assertIn(self.blockchain_conn5, network_loaded.blockchain_nodes)
        self.assertEqual(remote_blockchain_node_loaded.ip_address, self.ip4)
        self.assertEqual(remote_blockchain_node_loaded.port, str(self.port4))
        self.assertEqual(remote_blockchain_node_loaded.fileno, str(self.fileno4))

    def test_on_check_extensions(self):
        initialize(True, self.source_version, self.ip_address, self.continent, self.country,
                   False, self.account_id, self.quota_level)
        _, analysis_loaded, _, _ = self._load_status_file()
        self.assertNotEqual(analysis_loaded.extensions_check, ExtensionModulesState.UNAVAILABLE)
        self.assertEqual(type(analysis_loaded.extensions_check), ExtensionModulesState)

    def test_on_network_update_connection(self):
        new_desc4 = "1.1.1.1 80"
        new_ip4 = new_desc4.split()[0]
        new_port4 = new_desc4.split()[1]
        new_fileno4 = "10"
        self._add_connections()
        update(self.conn_pool, True, self.source_version, self.ip_address, self.continent, self.country, False,
               self.blockchain_peers, self.account_id, self.quota_level)
        summary_loaded, _, _, network_loaded = self._load_status_file()
        network_loaded.remote_blockchain_nodes.clear()
        network_loaded.add_connection(ConnectionType.REMOTE_BLOCKCHAIN_NODE, new_desc4, new_fileno4)
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_nodes[0]
        self.assertEqual(summary_loaded,
                         network_loaded.get_summary(self.ip_address, self.continent, self.country,
                                                    False, self.account_id, self.quota_level))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.ONLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.quota_level, summary.gateway_status_get_quota_level(self.quota_level))
        self.assertEqual(
            summary_loaded.blockchain_node_connection_states,
            {
                f"{self.ip3} {self.port3}": ConnectionState.ESTABLISHED,
                f"{self.ip5} {self.port5}": ConnectionState.ESTABLISHED,
            }
        )
        self.assertEqual(remote_blockchain_node_loaded.ip_address, new_ip4)
        self.assertEqual(remote_blockchain_node_loaded.port, new_port4)
        self.assertEqual(remote_blockchain_node_loaded.fileno, new_fileno4)

    def test_remove_blockchain_connection_and_add_new(self):
        self._add_connections()

        new_desc6 = "1.1.1.1 80"
        new_ip6 = new_desc6.split()[0]
        new_port6 = new_desc6.split()[1]
        new_fileno6 = "10"
        new_node6 = MockNode(helpers.get_common_opts(1004, external_ip="214.215.216.217"))
        new_conn6 = MockConnection(
            MockSocketConnection(new_fileno6, new_node6, ip_address=new_ip6, port=int(new_port6)), new_node6
        )
        self.conn_pool.delete(self.conn3)
        self.blockchain_peers.add(BlockchainPeerInfo(new_ip6, int(new_port6)))
        self.conn_pool.add(int(new_fileno6), new_ip6, int(new_port6), new_conn6)

        update(self.conn_pool, True, self.source_version, self.ip_address, self.continent, self.country, False,
               self.blockchain_peers, self.account_id, self.quota_level)
        summary_loaded, _, _, network_loaded = self._load_status_file()
        self.assertEqual(summary_loaded,
                         network_loaded.get_summary(self.ip_address, self.continent, self.country,
                                                    False, self.account_id, self.quota_level))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.WITH_ERRORS)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.quota_level, summary.gateway_status_get_quota_level(self.quota_level))
        self.assertEqual(
            summary_loaded.blockchain_node_connection_states,
            {
                f"{new_ip6} {new_port6}": ConnectionState.ESTABLISHED,
                f"{self.ip3} {self.port3}": ConnectionState.DISCONNECTED,
                f"{self.ip5} {self.port5}": ConnectionState.ESTABLISHED,
            }
        )

    def _add_connections(self):
        self.conn_pool.add(self.fileno1, self.ip1, self.port1, self.conn1)
        self.conn_pool.add(self.fileno2, self.ip2, self.port2, self.conn2)
        self.conn_pool.add(self.fileno3, self.ip3, self.port3, self.conn3)
        self.conn_pool.add(self.fileno4, self.ip4, self.port4, self.conn4)
        self.conn_pool.add(self.fileno5, self.ip5, self.port5, self.conn5)
        self.blockchain_peers.add(BlockchainPeerInfo(self.ip5, self.port5))

    def _load_status_file(self) -> Tuple[Summary, Analysis, Environment, Network]:
        path = config.get_data_file(STATUS_FILE_NAME)
        self.assertTrue(os.path.exists(path))
        with open(path, "r", encoding="utf-8") as json_file:
            status_file = json_file.read()
        diagnostics_loaded = model_loader.load_model_from_json(Diagnostics, status_file)
        summary_loaded = diagnostics_loaded.summary
        analysis_loaded = diagnostics_loaded.analysis
        network_loaded = analysis_loaded.network
        environment_loaded = analysis_loaded.environment
        return summary_loaded, analysis_loaded, environment_loaded, network_loaded
