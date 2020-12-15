from dataclasses import dataclass
from typing import Optional, Dict

from bxgateway.utils.logging.status.connection_state import ConnectionState
from bxgateway.utils.logging.status.gateway_status import GatewayStatus


@dataclass
class Summary:
    gateway_status: Optional[GatewayStatus] = None
    account_info: Optional[str] = None
    block_relay_connection_state: Optional[ConnectionState] = None
    transaction_relay_connection_state: Optional[ConnectionState] = None
    blockchain_node_connection_states: Optional[Dict[str, ConnectionState]] = None
    remote_blockchain_node_connection_state: Optional[ConnectionState] = None
    ip_address: Optional[str] = None
    continent: Optional[str] = None
    country: Optional[str] = None
    update_required: bool = False
    quota_level: Optional[str] = None


def gateway_status_get_account_info(account_id: Optional[str]) -> str:
    return f"This gateway is associated to account: {account_id}" if account_id \
        else "This gateway is not registered to any account and is limited to the daily free quota"


def gateway_status_get_quota_level(quota_level: Optional[int]) -> str:
    return f"{quota_level}%"