from typing import TYPE_CHECKING, Optional

from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.requests.abstract_rpc_request import AbstractRpcRequest
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcInternalError
from bxgateway import argument_parsers

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode


class AbstractBlockchainPeerRpcRequest(AbstractRpcRequest["AbstractGatewayNode"]):
    help = {
        "params": f"{rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY}: "
                "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
                "For other blockchain protocols, the format is <ip>:<port>",
    }

    def __init__(self, request: BxJsonRpcRequest, node: "AbstractGatewayNode") -> None:
        self._blockchain_peer_info: Optional[BlockchainPeerInfo] = None
        super().__init__(request, node)

    def validate_params(self) -> None:
        super().validate_params()
        params = self.params
        if params is None or not isinstance(params, dict):
            raise RpcInvalidParams(
                self.request_id,
                "Params request field is either missing or not a dictionary type."
            )
        if rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY in params:
            peer = params[rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY]
            blockchain_protocol = self.node.opts.blockchain_protocol
            if blockchain_protocol is not None:
                self._blockchain_peer_info = self.parse_peer(blockchain_protocol, peer)
            else:
                raise RpcInternalError(
                    self.request_id,
                    "Could not process request to add/remove blockchain peer. Please contact bloXroute support."
                )
        else:
            raise RpcInvalidParams(
                self.request_id,
                f"Missing param: {rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY}."
            )

    def parse_enode(self, enode: str) -> BlockchainPeerInfo:
        # Make sure enode is at least as long as the public key
        if not argument_parsers.enode_is_valid_length(enode):
            raise RpcInvalidParams(
                self.request_id,
                f"Invalid enode: {enode}, with length: {len(enode)}. "
                f"Expected format: enode://<eth node public key>@<eth node ip>:<port>"
            )
        try:
            pub_key, ip, port = argument_parsers.get_enode_parts(enode)
            if not port.isnumeric():
                raise RpcInvalidParams(
                    self.request_id,
                    f"Invalid port: {port}"
                )
        except ValueError:
            raise RpcInvalidParams(
                self.request_id,
                f"Invalid enode: {enode}. "
                f"Expected format: enode://<eth node public key>@<eth node ip>:<port>"
            )
        else:
            return BlockchainPeerInfo(ip, int(port), pub_key)

    def parse_ip_port(self, ip_port_string: str) -> BlockchainPeerInfo:
        ip, port = argument_parsers.get_ip_port_string_parts(ip_port_string)
        if not port.isnumeric():
            raise RpcInvalidParams(
                self.request_id,
                f"Invalid port: {port}"
            )
        return BlockchainPeerInfo(ip, int(port))

    def parse_peer(self, blockchain_protocol: str, peer: str) -> BlockchainPeerInfo:
        if blockchain_protocol.lower() == BlockchainProtocol.ETHEREUM.value:
            return self.parse_enode(peer)
        else:
            return self.parse_ip_port(peer)
