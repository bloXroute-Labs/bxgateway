from dataclasses import dataclass
from typing import Optional


@dataclass
class BlockchainPeerInfo:
    ip: str
    port: int
    node_public_key: Optional[str] = None

    def __repr__(self):
        return f"BlockchainPeerInfo({self.ip}:{self.port})"

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, BlockchainPeerInfo)
            and other.port == self.port
            and other.ip == self.ip
        )

    def __hash__(self):
        return hash(f"{self.ip}:{self.port}")
