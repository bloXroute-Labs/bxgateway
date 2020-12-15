from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union, Dict, List, cast, Iterator, Tuple

from bxcommon.connections.connection_type import ConnectionType
from bxcommon.network.ip_endpoint import IpEndpoint
from bxgateway.utils.logging.status import summary
from bxgateway.utils.logging.status.blockchain_connection import BlockchainConnection
from bxgateway.utils.logging.status.connection_state import ConnectionState
from bxgateway.utils.logging.status.gateway_status import GatewayStatus
from bxgateway.utils.logging.status.relay_connection import RelayConnection
from bxgateway.utils.logging.status.summary import Summary


@dataclass
class Network:
    block_relays: List[RelayConnection]
    transaction_relays: List[RelayConnection]
    blockchain_nodes: List[BlockchainConnection]
    remote_blockchain_nodes: List[BlockchainConnection]

    def iter_network_type_pairs(
        self
    ) -> Iterator[Tuple[ConnectionType, Union[List[RelayConnection], List[BlockchainConnection]]]]:
        networks = {
            ConnectionType.RELAY_BLOCK: self.block_relays,
            ConnectionType.RELAY_TRANSACTION: self.transaction_relays,
            ConnectionType.BLOCKCHAIN_NODE: self.blockchain_nodes,
            ConnectionType.REMOTE_BLOCKCHAIN_NODE: self.remote_blockchain_nodes
        }
        for network_type, network in networks.items():
            yield network_type, network

    def get_summary(self, ip_address: str, continent: str, country: str, update_required: bool,
                    account_id: Optional[str], quota_level: Optional[int]) -> Summary:
        block_relay_connections_state = _connections_states_info(self.block_relays)
        transaction_relay_connections_state = _connections_states_info(self.transaction_relays)
        blockchain_node_connections_state = _blockchain_connections_state_info(self.blockchain_nodes)
        remote_blockchain_node_connections_state = _connections_states_info(self.remote_blockchain_nodes)
        gateway_status = self._get_gateway_status()

        return Summary(gateway_status, summary.gateway_status_get_account_info(account_id),
                       block_relay_connections_state, transaction_relay_connections_state,
                       blockchain_node_connections_state, remote_blockchain_node_connections_state,
                       ip_address, continent, country, update_required,
                       summary.gateway_status_get_quota_level(quota_level))

    def remove_connection(self, conn: Union[RelayConnection, BlockchainConnection], conn_type: ConnectionType) -> None:
        if conn_type == ConnectionType.RELAY_BLOCK:
            self.block_relays.remove(cast(RelayConnection, conn))
        elif conn_type == ConnectionType.RELAY_TRANSACTION:
            self.transaction_relays.remove(cast(RelayConnection, conn))
        elif conn_type == ConnectionType.BLOCKCHAIN_NODE:
            self.blockchain_nodes.remove(cast(BlockchainConnection, conn))
        elif conn_type == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            self.remote_blockchain_nodes.remove(cast(BlockchainConnection, conn))

    def add_connection(self, conn: ConnectionType, desc: str, file_no: Optional[str] = None,
                       peer_id: Optional[str] = None) -> None:
        ip_addr = desc.split()[0]
        port = desc.split()[1]
        current_time = _get_current_time()

        if conn == ConnectionType.RELAY_BLOCK:
            relay_connection = RelayConnection(ip_addr, port, file_no, peer_id, current_time)
            if relay_connection not in self.block_relays:
                self.block_relays.append(relay_connection)
        elif conn == ConnectionType.RELAY_TRANSACTION:
            relay_connection = RelayConnection(ip_addr, port, file_no, peer_id, current_time)
            if relay_connection not in self.transaction_relays:
                self.transaction_relays.append(relay_connection)
        elif conn == ConnectionType.BLOCKCHAIN_NODE:
            assert ip_addr is not None
            assert port is not None
            blockchain_connection = BlockchainConnection(ip_addr, port, file_no, current_time)
            if blockchain_connection not in self.blockchain_nodes:
                self.blockchain_nodes.append(blockchain_connection)
        elif conn == ConnectionType.REMOTE_BLOCKCHAIN_NODE:
            assert ip_addr is not None
            assert port is not None
            blockchain_connection = BlockchainConnection(ip_addr, port, file_no, current_time)
            if blockchain_connection not in self.remote_blockchain_nodes:
                self.remote_blockchain_nodes.append(BlockchainConnection(ip_addr, port, file_no, current_time))

    def _get_gateway_status(self) -> GatewayStatus:
        return GatewayStatus.ONLINE if _check_connections_established(
            self.block_relays) and _check_connections_established(
            self.transaction_relays) and _check_connections_established(
            self.blockchain_nodes) and _check_connections_established(
            self.remote_blockchain_nodes) else GatewayStatus.WITH_ERRORS


def _get_current_time() -> str:
    return "UTC " + str(datetime.utcnow())


def _check_connections_established(connections: Union[List[RelayConnection], List[BlockchainConnection]]) -> bool:
    return len(connections) > 0 and all([conn.get_connection_state() == ConnectionState.ESTABLISHED for conn in connections])


def _connections_states_info(connections: Union[List[RelayConnection], List[BlockchainConnection]]) -> ConnectionState:
    return ConnectionState.ESTABLISHED if _check_connections_established(connections) else ConnectionState.DISCONNECTED


def _blockchain_connections_state_info(connections: List[BlockchainConnection]) -> Dict[str, ConnectionState]:
    connection_states = {}
    for conn in connections:
        connection_states[str(IpEndpoint(conn.ip_address, int(conn.port)))] = \
            ConnectionState.ESTABLISHED if _check_connections_established([conn]) else ConnectionState.DISCONNECTED
    return connection_states
