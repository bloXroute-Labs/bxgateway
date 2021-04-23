import datetime
import time
import unittest
from abc import abstractmethod
from typing import cast
from unittest.mock import MagicMock

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.network.ip_endpoint import IpEndpoint
from bxcommon.rpc import rpc_constants
from bxcommon.test_utils import helpers
from bxcommon.test_utils.fixture import eth_fixtures
from bxcommon.test_utils.mocks.mock_connection import MockConnection
from bxcommon.test_utils.mocks.mock_socket_connection import MockSocketConnection
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.connections.eth.eth_node_connection import EthNodeConnection
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.testing.mocks.mock_blockchain_connection import MockMessageConverter
from bxutils import constants as utils_constants
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.bdn_service_model_base import BdnServiceModelBase, FeedServiceModelBase
from bxcommon.models.bdn_service_model_config_base import BdnServiceModelConfigBase, BdnFeedServiceModelConfigBase
from bxcommon.models.bdn_service_type import BdnServiceType
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.rpc.bx_json_rpc_request import BxJsonRpcRequest
from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_request_type import RpcRequestType
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test, AsyncMock
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.gateway_opts import GatewayOpts
from bxgateway.rpc.gateway_status_details_level import GatewayStatusDetailsLevel
from bxgateway.rpc.requests import gateway_memory_rpc_request
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode
from bxgateway.testing.mocks.mock_eth_ws_proxy_publisher import MockEthWsProxyPublisher
from bxgateway.utils.stats.gateway_bdn_performance_stats_service import \
    gateway_bdn_performance_stats_service


RAW_TRANSACTION_HEX = (
    "fffefdfc747800000000000000000000ab000000ffd59870844e5411f9e4043e654146b054bdcabe726a4bc4bd716049bfa"
    "54a690500000000000000000000000000000000000000000000000100000000f86b01847735940082520894a2f6090c0483"
    "d6e6ac90a9c23d42e461fee2ac5188016147191f13b0008025a0784537f9801331b707ceedd5388d318d86b0bb43c6f5b32"
    "b30c9df960f596b05a042fe22aa47f2ae80cbb2c9272df2f8975c96a8a99020d8ac19d4d4b0e0b5821901"
)

TRANSACTION_JSON = {'value': "", 'gas': "", 'gasPrice': "", 'from': '0xc305c901078781C232A2a521C2aF7980f8385ee9', 'to': '0xc305c901078781C232A2a521C2aF7980f8385ee9', 'data': '0x477a5c98'}

# TODO: this is different from relays? Not sure which is correct.
TRANSACTION_HASH = "74daac93cf5ff756e6fd5d10926a466533b4c901f64883e7f8b6b4a37cca808d"
ACCOUNT_ID = "bx_premium"


class AbstractGatewayRpcIntegrationTest(AbstractTestCase):
    def __init__(self, *args, **kwargs):
        # hack to avoid unit test discovery of this class
        super().__init__(*args, **kwargs)
        if self.__class__ != AbstractGatewayRpcIntegrationTest:
            # pylint: disable=no-value-for-parameter
            self.run = unittest.TestCase.run.__get__(self, self.__class__)
        else:
            self.run = lambda self, *args, **kwargs: None
        self._account_model = None

    # pyre-ignore async setup problems
    async def setUp(self) -> None:
        self.gateway_node = MockGatewayNode(self.get_gateway_opts())
        self.gateway_node.requester = ThreadedRequestService(
            "mock_thread_service",
            self.gateway_node.alarm_queue,
            constants.THREADED_HTTP_POOL_SLEEP_INTERVAL_S
        )
        self.gateway_node.requester.start()
        self.gateway_node.account_id = ACCOUNT_ID
        self.node_endpoint_1 = IpEndpoint("127.0.0.1", 7000)
        self.blockchain_connection_1 = EthNodeConnection(
            MockSocketConnection(1, node=self.gateway_node, ip_address="127.0.0.1", port=7000), cast(EthGatewayNode, self.gateway_node)
        )
        self.blockchain_connection_1.on_connection_established()

    @abstractmethod
    def get_gateway_opts(self) -> GatewayOpts:
        feed_service_model = FeedServiceModelBase(
            allow_filtering=True,
            available_fields=["all"]
        )
        base_feed_service_model = BdnFeedServiceModelConfigBase(
            expire_date="2999-01-01",
            feed=feed_service_model
        )
        self._account_model = BdnAccountModelBase(
            account_id="",
            logical_account_name="",
            certificate="",
            expire_date=utils_constants.DEFAULT_EXPIRATION_DATE.isoformat(),
            cloud_api=BdnServiceModelConfigBase(
                msg_quota=None,
                permit=BdnServiceModelBase(
                    service_type=BdnServiceType.PERMIT
                ),
                expire_date=utils_constants.DEFAULT_EXPIRATION_DATE.isoformat()
            ),
            new_transaction_streaming=base_feed_service_model,
            new_pending_transaction_streaming=base_feed_service_model,
            on_block_feed=base_feed_service_model,
            new_block_streaming=base_feed_service_model,
            transaction_state_feed=base_feed_service_model,
            blockchain_protocol="Ethereum",
            blockchain_network="Mainnet",
        )

    @abstractmethod
    async def request(self, req: BxJsonRpcRequest) -> JsonRpcResponse:
        pass

    @async_test
    async def test_blxr_tx(self):
        result = await self.request(BxJsonRpcRequest(
            "1",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_PARAMS_KEY: RAW_TRANSACTION_HEX,
                rpc_constants.STATUS_TRACK_PARAMS_KEY: "True"
            }
        ))
        self.assertEqual("1", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(TRANSACTION_HASH, result.result["tx_hash"])

        self.assertEqual(1, len(self.gateway_node.broadcast_messages))
        self.assertEqual(
            Sha256Hash(convert.hex_to_bytes(TRANSACTION_HASH)),
            self.gateway_node.broadcast_messages[0][0].tx_hash()
        )

    @async_test
    async def test_blxr_tx_ethereum_berlin(self):
        self.gateway_node.message_converter = EthNormalMessageConverter()
        self.gateway_node.network_num = 5

        # legacy
        result = await self.request(BxJsonRpcRequest(
            "1",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_PARAMS_KEY: convert.bytes_to_hex(
                    eth_fixtures.LEGACY_TRANSACTION
                ),
                rpc_constants.STATUS_TRACK_PARAMS_KEY: "True"
            }
        ))
        self.assertEqual("1", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(eth_fixtures.LEGACY_TRANSACTION_HASH, result.result["tx_hash"])

        self.assertEqual(1, len(self.gateway_node.broadcast_messages))
        self.assertEqual(
            Sha256Hash(convert.hex_to_bytes(eth_fixtures.LEGACY_TRANSACTION_HASH)),
            self.gateway_node.broadcast_messages[0][0].tx_hash()
        )

        # access list
        result = await self.request(BxJsonRpcRequest(
            "1",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_PARAMS_KEY: convert.bytes_to_hex(
                    eth_fixtures.ACCESS_LIST_TRANSACTION
                ),
                rpc_constants.STATUS_TRACK_PARAMS_KEY: "True"
            }
        ))
        self.assertEqual("1", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(eth_fixtures.ACCESS_LIST_TRANSACTION_HASH, result.result["tx_hash"])

        self.assertEqual(2, len(self.gateway_node.broadcast_messages))
        self.assertEqual(
            Sha256Hash(convert.hex_to_bytes(eth_fixtures.ACCESS_LIST_TRANSACTION_HASH)),
            self.gateway_node.broadcast_messages[1][0].tx_hash()
        )

    @async_test
    async def test_blxr_tx_expired(self):
        self.gateway_node.account_model.is_account_valid = MagicMock(return_value=False)
        result = await self.request(BxJsonRpcRequest(
            "2",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_PARAMS_KEY: RAW_TRANSACTION_HEX,
            }
        ))
        self.assertEqual("2", result.id)
        self.assertEqual("Invalid Account ID", result.error.message)
        self.assertIsNone(result.result)
        self.assertIsNotNone(result.error)

    @async_test
    async def test_blxr_tx_quota_exceeded(self):
        self.gateway_node.quota_level = 100
        result = await self.request(BxJsonRpcRequest(
            "3",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_PARAMS_KEY: RAW_TRANSACTION_HEX,
            }
        ))
        self.assertEqual("3", result.id)
        self.assertEqual("Insufficient quota", result.error.message)
        self.assertIsNone(result.result)
        self.assertIsNotNone(result.error)

    @async_test
    async def test_blxr_tx_from_json(self):
        tx_json = {
            'from': '0xc165599b5e418bb9d8a19090696ea2403b2927ed',
            'gas': "0x5208",
            'gasPrice': "0xc1b759d70",
            'hash': '0xd569674ad9fcaaedcb6867b7896067b445d4a838316be4292c474df17bf4bd50',
            'input': '0x',
            'nonce': "0x14",
            'to': '0xdb5f0c1f4198bc6ffa98b35f7188f82740b8caf7',
            'value': "0x3e871b540c000",
            'v': '0x26',
            'r': '0x484bc950fb2d595500baa604774cb8d156a677198e087801936f38ca0b27049',
            's': '0x7ca82ef7b6938c2e96966dd940b186c9cdc0c7f42b2843adbc0751bb6e67a2d4'
        }

        result = await self.request(BxJsonRpcRequest(
            "1",
            RpcRequestType.BLXR_TX,
            {
                rpc_constants.TRANSACTION_JSON_PARAMS_KEY: tx_json,
            }
        ))

        self.assertEqual("1", result.id)
        self.assertIsNone(result.error)
        self.assertEqual("ad6f9332384194f80d8e49af8f093ad019b3f6b7173eb2956a46c9a0d8c4d03c", result.result["tx_hash"])

        self.assertEqual(1, len(self.gateway_node.broadcast_messages))
        self.assertEqual(
            Sha256Hash(convert.hex_to_bytes("ad6f9332384194f80d8e49af8f093ad019b3f6b7173eb2956a46c9a0d8c4d03c")),
            self.gateway_node.broadcast_messages[0][0].tx_hash()
        )

    @async_test
    async def test_blxr_eth_call(self):
        self.gateway_node.eth_ws_proxy_publisher = MockEthWsProxyPublisher(None, None, None, None)
        self.gateway_node.eth_ws_proxy_publisher.call_rpc = AsyncMock(
            return_value=JsonRpcResponse(request_id=1)
        )
        result = await self.request(BxJsonRpcRequest(
            "1",
            RpcRequestType.BLXR_ETH_CALL,
            {
                rpc_constants.TRANSACTION_JSON_PARAMS_KEY: TRANSACTION_JSON,
                rpc_constants.TAG_PARAMS_KEY: "latest"
            }
        ))
        self.gateway_node.eth_ws_proxy_publisher.call_rpc.mock.assert_called_with(
            "eth_call", [TRANSACTION_JSON, "latest"])
        self.assertIsNone(result.error)

    @async_test
    async def test_gateway_status(self):
        result_summary = await self.request(BxJsonRpcRequest(
            "2",
            RpcRequestType.GATEWAY_STATUS,
            None
        ))
        self.assertEqual("2", result_summary.id)
        self.assertIsNone(result_summary.error)
        self.assertEqual(constants.LOCALHOST, result_summary.result["ip_address"])
        self.assertEqual("NA", result_summary.result["continent"])
        self.assertEqual("United States", result_summary.result["country"])

        result_detailed = await self.request(BxJsonRpcRequest(
            "2.5",
            RpcRequestType.GATEWAY_STATUS,
            {
                rpc_constants.DETAILS_LEVEL_PARAMS_KEY: GatewayStatusDetailsLevel.DETAILED.name
            }
        ))
        self.assertEqual("2.5", result_detailed.id)
        self.assertIsNone(result_detailed.error)
        self.assertEqual(constants.LOCALHOST, result_detailed.result["summary"]["ip_address"])
        self.assertEqual("NA", result_detailed.result["summary"]["continent"])
        self.assertEqual("United States", result_detailed.result["summary"]["country"])

    @async_test
    async def test_stop(self):
        result = await self.request(BxJsonRpcRequest(
            "3",
            RpcRequestType.STOP,
            None
        ))
        self.assertEqual("3", result.id)
        self.assertIsNone(result.error)
        self.assertEqual({}, result.result)
        self.gateway_node.should_force_exit = True
        self.gateway_node.should_restart_on_high_memory = False

    @async_test
    async def test_memory(self):
        result = await self.request(BxJsonRpcRequest(
            "4",
            RpcRequestType.MEMORY,
            None
        ))
        self.assertEqual("4", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(0, result.result[gateway_memory_rpc_request.TOTAL_CACHED_TX])
        self.assertEqual("0 bytes", result.result[gateway_memory_rpc_request.TOTAL_CACHED_TX_SIZE])

    @async_test
    async def test_peers(self):
        result = await self.request(BxJsonRpcRequest(
            "5",
            RpcRequestType.PEERS,
            None
        ))
        self.assertEqual("5", result.id)
        self.assertIsNone(result.error)
        self.assertEqual([], result.result)

    @async_test
    async def test_bdn_performance_single_node(self):
        self.gateway_node.connection_pool.add(
            20, self.node_endpoint_1.ip_address, self.node_endpoint_1.port, self.blockchain_connection_1
        )
        gateway_bdn_performance_stats_service.set_node(self.gateway_node)
        gateway_bdn_performance_stats_service.log_block_from_bdn()
        gateway_bdn_performance_stats_service.log_block_from_bdn()
        gateway_bdn_performance_stats_service.log_block_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.log_tx_from_bdn()
        gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.close_interval_data()

        result = await self.request(BxJsonRpcRequest(
            "6",
            RpcRequestType.BDN_PERFORMANCE,
            None
        ))
        self.assertEqual("6", result.id)
        self.assertIsNone(result.error)
        node_stats = result.result[str(self.node_endpoint_1)]
        self.assertEqual("66.67%", node_stats["blocks_from_bdn_percentage"])
        self.assertEqual("33.33%", node_stats["transactions_from_bdn_percentage"])
        self.assertEqual(3, node_stats["total_blocks_seen"])

    @async_test
    async def test_bdn_performance_multi_node(self):
        self.gateway_node.connection_pool.add(
            20, self.node_endpoint_1.ip_address, self.node_endpoint_1.port, self.blockchain_connection_1
        )

        blockchain_connection_2 = EthNodeConnection(
            MockSocketConnection(1, node=self.gateway_node, ip_address="127.0.0.1", port=333), self.gateway_node)
        blockchain_connection_2.on_connection_established()
        self.gateway_node.mock_add_blockchain_peer(blockchain_connection_2)
        node_endpoint_2 = IpEndpoint("127.0.0.1", 333)
        self.gateway_node.connection_pool.add(
            21, node_endpoint_2.ip_address, node_endpoint_2.port, blockchain_connection_2
        )

        gateway_bdn_performance_stats_service.set_node(self.gateway_node)
        gateway_bdn_performance_stats_service.log_block_from_bdn()
        gateway_bdn_performance_stats_service.log_block_from_bdn()
        gateway_bdn_performance_stats_service.log_block_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.log_tx_from_bdn()
        gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.log_tx_from_blockchain_node(self.node_endpoint_1)
        gateway_bdn_performance_stats_service.close_interval_data()

        result = await self.request(BxJsonRpcRequest(
            "6",
            RpcRequestType.BDN_PERFORMANCE,
            None
        ))
        self.assertEqual("6", result.id)
        self.assertIsNone(result.error)
        node_stats = result.result[str(self.node_endpoint_1)]
        self.assertEqual("66.67%", node_stats["blocks_from_bdn_percentage"])
        self.assertEqual("33.33%", node_stats["transactions_from_bdn_percentage"])
        self.assertEqual(3, node_stats["total_blocks_seen"])

        node_stats_2 = result.result[str(node_endpoint_2)]
        self.assertEqual("100.00%", node_stats_2["blocks_from_bdn_percentage"])
        self.assertEqual("100.00%", node_stats_2["transactions_from_bdn_percentage"])
        self.assertEqual(3, node_stats_2["total_blocks_seen"])

    @async_test
    async def test_transaction_status(self):
        time.time = MagicMock(return_value=time.time())
        expected_assignment_time = datetime.datetime.fromtimestamp(time.time()).isoformat()

        short_id = 123
        transaction_hash = helpers.generate_object_hash()
        transaction_contents = helpers.generate_bytearray(250)

        tx_service = self.gateway_node.get_tx_service()
        transaction_key = tx_service.get_transaction_key(transaction_hash)
        tx_service.set_transaction_contents_by_key(transaction_key, transaction_contents)
        tx_service.assign_short_id_by_key(transaction_key, short_id)

        result = await self.request(BxJsonRpcRequest(
            "7",
            RpcRequestType.TX_STATUS,
            {"transaction_hash": convert.bytes_to_hex(transaction_hash.binary)}
        ))
        self.assertEqual("7", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(
            {
                "status": "assigned short ID",
                "short_ids": [123],
                "assignment_time": expected_assignment_time
            },
            result.result
        )

    @async_test
    async def test_add_blockchain_peer(self):
        self.gateway_node.enqueue_connection = MagicMock()
        result = await self.request(BxJsonRpcRequest(
            "8",
            RpcRequestType.ADD_BLOCKCHAIN_PEER,
            {"peer": "enode://d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396@127.0.0.1:30302"}
        ))
        self.gateway_node.enqueue_connection.assert_called_once_with("127.0.0.1", 30302, ConnectionType.BLOCKCHAIN_NODE)
        self.assertIn(BlockchainPeerInfo("127.0.0.1", 30302), self.gateway_node.blockchain_peers)
        self.assertEqual("8", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(
            {
                "new_peer": "127.0.0.1:30302"
            },
            result.result
        )

    @async_test
    async def test_remove_blockchain_peer(self):
        conn = MockConnection(MockSocketConnection(9, self.gateway_node, ip_address="127.0.0.1", port=30302), self.gateway_node)
        conn.mark_for_close = MagicMock()
        self.gateway_node.connection_pool.add(9, "127.0.0.1", 30302, conn)
        self.gateway_node.blockchain_peers.add(BlockchainPeerInfo("127.0.0.1", 30302, "d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396"))
        result = await self.request(BxJsonRpcRequest(
            "9",
            RpcRequestType.REMOVE_BLOCKCHAIN_PEER,
            {"peer": "enode://d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396@127.0.0.1:30302"}
        ))
        self.assertNotIn(BlockchainPeerInfo("127.0.0.1", 30302), self.gateway_node.blockchain_peers)
        conn.mark_for_close.assert_called_once_with(False)
        self.assertEqual("9", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(
            {
                "removed_peer": "127.0.0.1:30302"
            },
            result.result
        )

    @async_test
    async def test_remove_blockchain_peer_not_in_pool(self):
        self.gateway_node.alarm_queue.register_alarm = MagicMock()
        self.gateway_node.blockchain_peers.add(BlockchainPeerInfo("127.0.0.1", 30302, "d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396"))
        result = await self.request(BxJsonRpcRequest(
            "9",
            RpcRequestType.REMOVE_BLOCKCHAIN_PEER,
            {"peer": "enode://d76d7d11a822fab02836f8b0ea462205916253eb630935d15191fb6f9d218cd94a768fc5b3d5516b9ed5010a4765f95aea7124a39d0ab8aaf6fa3d57e21ef396@127.0.0.1:30302"}
        ))
        self.assertNotIn(BlockchainPeerInfo("127.0.0.1", 30302), self.gateway_node.blockchain_peers)
        self.gateway_node.alarm_queue.register_alarm.assert_called()
        self.assertEqual("9", result.id)
        self.assertIsNone(result.error)
        self.assertEqual(
            {
                "removed_peer": "127.0.0.1:30302"
            },
            result.result
        )
