import os
import uuid
from typing import Tuple

from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxcommon.utils import config
from bxcommon.utils import model_loader
from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_type import ConnectionType
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

        self.fileno4 = 8
        self.ip4 = "111.222.111.222"
        self.port4 = 3000
        self.node4 = MockNode(helpers.get_common_opts(1003, external_ip="101.101.101.101"))
        self.node_id4 = str(uuid.uuid1())
        self.conn4 = MockConnection(
            MockSocketConnection(self.fileno4, self.node4, ip_address=self.ip4, port=self.port4), self.node4
        )
        self.conn4.CONNECTION_TYPE = ConnectionType.REMOTE_BLOCKCHAIN_NODE

        initialize(False, self.source_version, self.ip_address, self.continent, self.country, False, self.account_id)

        path = config.get_data_file(STATUS_FILE_NAME)
        self.addCleanup(os.remove, path)

    def test_on_initialize_logger(self):
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relay
        transaction_relay_loaded = network_loaded.transaction_relay
        blockchain_node_loaded = network_loaded.blockchain_node
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_node
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.OFFLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, None)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, None)
        self.assertEqual(summary_loaded.blockchain_node_connection_state, None)
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, None)
        self.assertEqual(summary_loaded.update_required, False)
        self.assertEqual(analysis_loaded.gateway_version, self.source_version)
        self.assertEqual(block_relay_loaded.ip_address, None)
        self.assertEqual(block_relay_loaded.port, None)
        self.assertEqual(block_relay_loaded.fileno, None)
        self.assertEqual(transaction_relay_loaded.ip_address, None)
        self.assertEqual(transaction_relay_loaded.port, None)
        self.assertEqual(transaction_relay_loaded.fileno, None)
        self.assertEqual(blockchain_node_loaded.ip_address, None)
        self.assertEqual(blockchain_node_loaded.port, None)
        self.assertEqual(blockchain_node_loaded.fileno, None)
        self.assertEqual(remote_blockchain_node_loaded.ip_address, None)
        self.assertEqual(remote_blockchain_node_loaded.port, None)
        self.assertEqual(remote_blockchain_node_loaded.fileno, None)
        self.assertEqual(type(analysis_loaded.extensions_check), ExtensionModulesState)
        self.assertEqual(type(environment_loaded.installation_type), InstallationType)

    def test_on_get_connection_state_intialization(self):
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relay
        transaction_relay_loaded = network_loaded.transaction_relay
        blockchain_node_loaded = network_loaded.blockchain_node
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_node
        self.assertEqual(block_relay_loaded.get_connection_state(), ConnectionState.DISCONNECTED)
        self.assertEqual(transaction_relay_loaded.get_connection_state(), ConnectionState.DISCONNECTED)
        self.assertEqual(blockchain_node_loaded.get_connection_state(), ConnectionState.DISCONNECTED)
        self.assertEqual(remote_blockchain_node_loaded.get_connection_state(), ConnectionState.DISCONNECTED)

    def test_on_update_one_connection(self):
        self.conn_pool.add(self.fileno1, self.ip1, self.port1, self.conn1)
        update(self.conn_pool, False, self.source_version, self.ip_address, self.continent, self.country, True,
               self.account_id)
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relay
        transaction_relay_loaded = network_loaded.transaction_relay
        blockchain_node_loaded = network_loaded.blockchain_node
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_node
        self.assertEqual(summary_loaded,
                         network_loaded.get_summary(self.ip_address, self.continent, self.country, True,
                                                    self.account_id))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.WITH_ERRORS)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, ConnectionState.DISCONNECTED)
        self.assertEqual(summary_loaded.blockchain_node_connection_state, ConnectionState.DISCONNECTED)
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, ConnectionState.DISCONNECTED)
        self.assertEqual(summary_loaded.update_required, True)
        self.assertEqual(block_relay_loaded.ip_address, self.ip1)
        self.assertEqual(block_relay_loaded.port, str(self.port1))
        self.assertEqual(block_relay_loaded.fileno, str(self.fileno1))
        self.assertEqual(transaction_relay_loaded.ip_address, None)
        self.assertEqual(transaction_relay_loaded.port, None)
        self.assertEqual(transaction_relay_loaded.fileno, None)
        self.assertEqual(blockchain_node_loaded.ip_address, None)
        self.assertEqual(blockchain_node_loaded.port, None)
        self.assertEqual(blockchain_node_loaded.fileno, None)
        self.assertEqual(remote_blockchain_node_loaded.ip_address, None)
        self.assertEqual(remote_blockchain_node_loaded.port, None)
        self.assertEqual(remote_blockchain_node_loaded.fileno, None)

    def test_on_update_all_connections(self):
        self._add_connections()
        update(self.conn_pool, False, self.source_version, self.ip_address, self.continent, self.country, False,
               self.account_id)
        summary_loaded, analysis_loaded, environment_loaded, network_loaded = self._load_status_file()
        block_relay_loaded = network_loaded.block_relay
        transaction_relay_loaded = network_loaded.transaction_relay
        blockchain_node_loaded = network_loaded.blockchain_node
        remote_blockchain_node_loaded = network_loaded.remote_blockchain_node
        self.assertEqual(summary_loaded, network_loaded.get_summary(self.ip_address, self.continent, self.country,
                                                                    False, self.account_id))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.ONLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.block_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.transaction_relay_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.blockchain_node_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(summary_loaded.remote_blockchain_node_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(block_relay_loaded.ip_address, self.ip1)
        self.assertEqual(block_relay_loaded.port, str(self.port1))
        self.assertEqual(block_relay_loaded.fileno, str(self.fileno1))
        self.assertEqual(transaction_relay_loaded.ip_address, self.ip2)
        self.assertEqual(transaction_relay_loaded.port, str(self.port2))
        self.assertEqual(transaction_relay_loaded.fileno, str(self.fileno2))
        self.assertEqual(blockchain_node_loaded.ip_address, self.ip3)
        self.assertEqual(blockchain_node_loaded.port, str(self.port3))
        self.assertEqual(blockchain_node_loaded.fileno, str(self.fileno3))
        self.assertEqual(remote_blockchain_node_loaded.ip_address, self.ip4)
        self.assertEqual(remote_blockchain_node_loaded.port, str(self.port4))
        self.assertEqual(remote_blockchain_node_loaded.fileno, str(self.fileno4))

    def test_on_check_extensions(self):
        initialize(True, self.source_version, self.ip_address, self.continent, self.country,
                   False, self.account_id)
        _, analysis_loaded, _, _ = self._load_status_file()
        self.assertNotEqual(analysis_loaded.extensions_check, ExtensionModulesState.UNAVAILABLE)
        self.assertEqual(type(analysis_loaded.extensions_check), ExtensionModulesState)

    def test_on_network_update_connection(self):
        new_desc3 = "1.1.1.1 80"
        new_ip3 = new_desc3.split()[0]
        new_port3 = new_desc3.split()[1]
        new_fileno3 = "10"
        self._add_connections()
        update(self.conn_pool, True, self.source_version, self.ip_address, self.continent, self.country, False,
               self.account_id)
        summary_loaded, _, _, network_loaded = self._load_status_file()
        network_loaded.update_connection(ConnectionType.BLOCKCHAIN_NODE, new_desc3, new_fileno3)
        blockchain_node_loaded = network_loaded.blockchain_node
        self.assertEqual(summary_loaded,
                         network_loaded.get_summary(self.ip_address, self.continent, self.country,
                                                    False, self.account_id))
        self.assertEqual(summary_loaded.gateway_status, GatewayStatus.ONLINE)
        self.assertEqual(summary_loaded.account_info, summary.gateway_status_get_account_info(None))
        self.assertEqual(summary_loaded.blockchain_node_connection_state, ConnectionState.ESTABLISHED)
        self.assertEqual(blockchain_node_loaded.ip_address, new_ip3)
        self.assertEqual(blockchain_node_loaded.port, new_port3)
        self.assertEqual(blockchain_node_loaded.fileno, new_fileno3)

    def _add_connections(self):
        self.conn_pool.add(self.fileno1, self.ip1, self.port1, self.conn1)
        self.conn_pool.add(self.fileno2, self.ip2, self.port2, self.conn2)
        self.conn_pool.add(self.fileno3, self.ip3, self.port3, self.conn3)
        self.conn_pool.add(self.fileno4, self.ip4, self.port4, self.conn4)

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
