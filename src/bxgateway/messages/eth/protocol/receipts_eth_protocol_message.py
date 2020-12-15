import blxr_rlp as rlp
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxcommon.utils.blockchain_utils.eth import rlp_utils


class ReceiptsEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.RECEIPTS

    fields = [("raw_data", rlp.sedes.raw)]

    def get_raw_data(self):
        return self.get_field_value("raw_data")

    def get_receipts_bytes(self):
        return rlp_utils.get_first_list_field_items_bytes(self._memory_view)
