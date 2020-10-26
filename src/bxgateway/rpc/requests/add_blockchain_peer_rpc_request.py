from bxcommon.connections.connection_type import ConnectionType
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc import rpc_constants
from bxgateway.rpc.requests.abstract_blockchain_peer_rpc_request import AbstractBlockchainPeerRpcRequest


class AddBlockchainPeerRpcRequest(AbstractBlockchainPeerRpcRequest):
    help = {
        "params": f"{rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY}: "
                "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
                "For other blockchain protocols, the format is <ip>:<port>",
        "description": "Add blockchain peer"
    }

    async def process_request(self) -> JsonRpcResponse:
        blockchain_peer_info = self._blockchain_peer_info
        assert blockchain_peer_info is not None
        self.node.blockchain_peers.add(blockchain_peer_info)
        self.node.enqueue_connection(
            blockchain_peer_info.ip, blockchain_peer_info.port, ConnectionType.BLOCKCHAIN_NODE
        )

        return self.ok({
            "new_peer": f"{blockchain_peer_info.ip}:{blockchain_peer_info.port}"
        })
