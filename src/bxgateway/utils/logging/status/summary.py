from dataclasses import dataclass
from typing import Optional

from bxgateway.utils.logging.status.connection_state import ConnectionState
from bxgateway.utils.logging.status.gateway_status import GatewayStatus


@dataclass
class Summary:
    gateway_status: Optional[GatewayStatus] = None
    block_relay_connection_state: Optional[ConnectionState] = None
    transaction_relay_connection_state: Optional[ConnectionState] = None
    blockchain_node_connection_state: Optional[ConnectionState] = None
    remote_blockchain_node_connection_state: Optional[ConnectionState] = None
    ip_address: Optional[str] = None
    continent: Optional[str] = None
    country: Optional[str] = None
    update_required: bool = False
