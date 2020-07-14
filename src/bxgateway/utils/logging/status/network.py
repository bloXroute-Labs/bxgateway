from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Union

from bxcommon.connections.connection_type import ConnectionType
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

    def get_summary(self, ip_address: str, continent: str, country: str, update_required: bool,
                    account_id: Optional[str], quota_level: Optional[int]) -> Summary:
        block_relay_connections_state = _connections_states_info(self.block_relays)
        transaction_relay_connections_state = _connections_states_info(self.transaction_relays)
        blockchain_node_connections_state = _connections_states_info(self.blockchain_nodes)
        remote_blockchain_node_connections_state = _connections_states_info(self.remote_blockchain_nodes)
        gateway_status = self._get_gateway_status()

        return Summary(gateway_status, summary.gateway_status_get_account_info(account_id),
                       block_relay_connections_state, transaction_relay_connections_state,
                       blockchain_node_connections_state, remote_blockchain_node_connections_state,
                       ip_address, continent, country, update_required,
                       summary.gateway_status_get_quota_level(quota_level))

    def clear(self):
        self.block_relays = []
        self.transaction_relays = []
        self.blockchain_nodes = []
        self.remote_blockchain_nodes = []

    def add_connection(self, conn: ConnectionType, desc: Optional[str] = None, file_no: Optional[str] = None,
                       peer_id: Optional[str] = None) -> None:
        ip_addr = None if desc is None else desc.split()[0]
        port = None if desc is None else desc.split()[1]
        current_time = _get_current_time()

        if conn == ConnectionType.RELAY_BLOCK:
            self.block_relays.append(RelayConnection(ip_addr, port, file_no, peer_id, current_time))
        elif conn == ConnectionType.RELAY_TRANSACTION:
            self.transaction_relays.append(RelayConnection(ip_addr, port, file_no, peer_id, current_time))
        elif conn == ConnectionType.BLOCKCHAIN_NODE:
            self.blockchain_nodes.append(BlockchainConnection(ip_addr, port, file_no, current_time))
        else:
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
