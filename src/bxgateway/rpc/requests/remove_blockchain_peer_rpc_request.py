from bxcommon import constants
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc import rpc_constants
from bxgateway.rpc.requests.abstract_blockchain_peer_rpc_request import AbstractBlockchainPeerRpcRequest
from bxgateway.utils.logging.status import status_log


class RemoveBlockchainPeerRpcRequest(AbstractBlockchainPeerRpcRequest):
    help = {
        "params": f"{rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY}: "
                "For Ethereum, the format is enode://<eth node public key>@<eth node ip>:<port>. "
                "For other blockchain protocols, the format is <ip>:<port>",
        "description": "Remove blockchain peer"
    }

    async def process_request(self) -> JsonRpcResponse:
        blockchain_peer_info = self._blockchain_peer_info
        assert blockchain_peer_info is not None
        self.node.blockchain_peers.discard(blockchain_peer_info)
        if self.node.connection_pool.has_connection(blockchain_peer_info.ip, blockchain_peer_info.port):
            peer_conn_to_remove = self.node.connection_pool.get_by_ipport(
                blockchain_peer_info.ip, blockchain_peer_info.port
            )
            peer_conn_to_remove.mark_for_close(False)
        else:
            self.node.time_blockchain_peer_conn_destroyed_by_ip.pop(
                (blockchain_peer_info.ip, blockchain_peer_info.port), None
            )
            self.node.alarm_queue.register_approx_alarm(
                2 * constants.MIN_SLEEP_TIMEOUT, constants.MIN_SLEEP_TIMEOUT, status_log.update_alarm_callback,
                self.node.connection_pool, self.node.opts.use_extensions, self.node.opts.source_version,
                self.node.opts.external_ip, self.node.opts.continent, self.node.opts.country,
                self.node.opts.should_update_source_version, self.node.blockchain_peers, self.node.account_id,
                self.node.quota_level
            )

        return self.ok({
            "removed_peer": f"{blockchain_peer_info.ip}:{blockchain_peer_info.port}"
        })
