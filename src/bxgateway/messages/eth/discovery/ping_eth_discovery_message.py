import blxr_rlp as rlp
from bxgateway.messages.eth.discovery.eth_discovery_message import EthDiscoveryMessage
from bxgateway.messages.eth.discovery.eth_discovery_message_type import EthDiscoveryMessageType
from bxgateway.messages.eth.serializers.endpoint import Endpoint


class PingEthDiscoveryMessage(EthDiscoveryMessage):
    msg_type = EthDiscoveryMessageType.PING

    fields = [("protocol_version", rlp.sedes.big_endian_int),
              ("listen_address", Endpoint()),
              ("remote_address", Endpoint()),
              ("expiration", rlp.sedes.big_endian_int)]

    def get_protocol_version(self):
        return self.get_field_value("protocol_version")

    def get_listen_address(self):
        return self.get_field_value("listen_address")

    def get_remote_address(self):
        return self.get_field_value("remote_address")

    def get_expiration(self):
        return self.get_field_value("expiration")
