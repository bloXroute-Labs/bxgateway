import blxr_rlp as rlp
from bxcommon import constants
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType


class HelloEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.HELLO

    fields = [("version", rlp.sedes.big_endian_int),
              ("client_version_string", rlp.sedes.binary),
              ("capabilities", rlp.sedes.CountableList(rlp.sedes.List([rlp.sedes.binary, rlp.sedes.big_endian_int]))),
              ("listen_port", rlp.sedes.big_endian_int),
              ("remote_pubkey", rlp.sedes.binary)]

    def get_version(self):
        return self.get_field_value("version")

    def get_client_version_string(self):
        return self.get_field_value("client_version_string").decode(constants.DEFAULT_TEXT_ENCODING)

    def get_capabilities(self):
        return self.get_field_value("capabilities")

    def get_listen_port(self):
        return self.get_field_value("listen_port")

    def get_remote_pubkey(self):
        return self.get_field_value("remote_pubkey")
