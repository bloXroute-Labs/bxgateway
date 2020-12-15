import blxr_rlp as rlp
from bxgateway.messages.eth.discovery.eth_discovery_message import EthDiscoveryMessage
from bxgateway.messages.eth.discovery.eth_discovery_message_type import EthDiscoveryMessageType
from bxgateway.messages.eth.serializers.endpoint import Endpoint


class PongEthDiscoveryMessage(EthDiscoveryMessage):
    msg_type = EthDiscoveryMessageType.PONG

    fields = [("remote_address", Endpoint()),
              ("ping_hash", rlp.sedes.raw),
              ("expiration", rlp.sedes.big_endian_int)]

    def get_remote_address(self):
        return self.get_field_value("remote_address")

    def get_ping_hash(self):
        return self.get_field_value("ping_hash")

    def get_expiration(self):
        return self.get_field_value("expiration")
