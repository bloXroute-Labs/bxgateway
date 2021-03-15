from bxcommon import constants
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.models.node_event_model import NodeEventType
from bxcommon.rpc.rpc_errors import RpcInvalidParams
from bxcommon.services import sdn_http_service
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc import rpc_constants
from bxgateway.rpc.requests.abstract_blockchain_peer_rpc_request import AbstractBlockchainPeerRpcRequest


class AddBlockchainPeerRpcRequest(AbstractBlockchainPeerRpcRequest):
    help = {
        "params":
            f"{rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY}: "
            "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
            "For other blockchain protocols, the format is <ip>:<port>, "
            f"{rpc_constants.ACCOUNT_ID_PARAMS_KEY}: Account Id, "
            f"{rpc_constants.PRIVATE_KEY_PARAMS_KEY}: Private key",
        "description": "Add blockchain peer"
    }

    def validate_params(self) -> None:
        super().validate_params()
        self.authenticate_request()

    async def process_request(self) -> JsonRpcResponse:
        blockchain_peer_info = self._blockchain_peer_info
        assert blockchain_peer_info is not None

        if self.node.opts.auth_with_cert and self.node.blockchain_peers:
            raise RpcInvalidParams(
                self.request_id,
                f"Cannot add blockchain peer to the gateway."
            )
        if blockchain_peer_info not in self.node.blockchain_peers:
            self.node.blockchain_peers.add(blockchain_peer_info)
            self.node.enqueue_connection(
                blockchain_peer_info.ip,
                blockchain_peer_info.port,
                ConnectionType.BLOCKCHAIN_NODE,
                account_id=blockchain_peer_info.account_id
            )
            self.node.requester.send_threaded_request(
                sdn_http_service.submit_peer_connection_event,
                NodeEventType.BLOCKCHAIN_NODE_CONN_ADDED,
                self.node.opts.node_id,
                blockchain_peer_info.ip,
                blockchain_peer_info.port,
                None,
                blockchain_peer_info.account_id
            )

        return self.ok({
            "new_peer": f"{blockchain_peer_info.ip}:{blockchain_peer_info.port}"
        })
