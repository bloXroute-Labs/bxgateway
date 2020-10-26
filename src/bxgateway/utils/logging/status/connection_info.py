from dataclasses import dataclass
from typing import Optional

from bxgateway.utils.logging.status.connection_state import ConnectionState


@dataclass
class ConnectionInfo:
    ip_address: Optional[str]
    port: Optional[str]
    fileno: Optional[str]
    connection_time: Optional[str]

    def get_connection_state(self) -> ConnectionState:
        return ConnectionState.DISCONNECTED if self.fileno is None else ConnectionState.ESTABLISHED
