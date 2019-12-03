from dataclasses import dataclass
from typing import Optional

from bxgateway.utils.logging.status.connection_info import ConnectionInfo


@dataclass
class RelayConnection(ConnectionInfo):
    peer_id: Optional[str]

    def __init__(self, ip_address: Optional[str] = None, port: Optional[str] = None, fileno: Optional[str] = None,
                 peer_id: Optional[str] = None, connection_time: Optional[str] = None):
        self.ip_address = ip_address
        self.port = port
        self.fileno = fileno
        self.peer_id = peer_id
        self.connection_time = connection_time
