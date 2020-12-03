import blxr_rlp as rlp
from bxgateway.messages.eth.protocol.eth_protocol_message import EthProtocolMessage
from bxgateway.messages.eth.protocol.eth_protocol_message_type import EthProtocolMessageType
from bxcommon.utils.blockchain_utils.eth import rlp_utils


class GetNodeDataEthProtocolMessage(EthProtocolMessage):
    msg_type = EthProtocolMessageType.GET_NODE_DATA

    fields = [("hashes", rlp.sedes.CountableList(rlp.sedes.binary))]

    def get_hashes(self):
        return rlp_utils.get_first_list_field_items_bytes(self._memory_view)