import time
from unittest.mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_node_ssl_service import MockNodeSSLService
from bxcommon.feed.feed_source import FeedSource
from bxcommon.utils.blockchain_utils.eth import crypto_utils
from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode
from bxgateway.feed.eth.eth_new_block_feed import EthNewBlockFeed
from bxgateway.feed.eth.eth_raw_block import EthRawBlock
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.messages.eth.internal_eth_block_info import InternalEthBlockInfo
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import NewBlockEthProtocolMessage
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks import mock_eth_messages


SAMPLE_TRANSACTION_FROM_WS = {
    "from": "0xbd4e113ee68bcbbf768ba1d6c7a14e003362979a",
    "gas": "0x9bba",
    "gas_price": "0x41dcf5dbe",
    "hash": "0x0d96b711bdcc89b59f0fdfa963158394cea99cedce52d0e4f4a56839145a814a",
    "input": "0xea1790b90000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000005ee3f95400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000041d880a37ae74a2593900da75b3cae6335b5f58997c6f426e98e42f55d3d5cd6487369ec0250a923cf1f45a39aa551b68420ec04582d1a68bcab9a70240ae39f261b00000000000000000000000000000000000000000000000000000000000000",
    "nonce": "0x1e8",
    "r": "0x561fc2c4428e8d3ff1e48ce07322a98ea6c8c5836bc79e7d60a6ed5d37a124a2",
    "s": "0x7ab1477ccb14143ba9afeb2f98099c85dd4175f09767f03d47f0733467eadde2",
    "to": "0xd7bec4d6bf6fc371eb51611a50540f0b59b5f896",
    "v": "0x25",
    "value": "0x0"
}

SAMPLE_BLOCK_FROM_WS = {
    'hash': '0x6d7dc7588dfa1ace9a29e5acd73985589618a7e898885da4f1931c08e3d9203f',
    'header': {
        'parentHash': '0x87c47498e717b407b2a169e67b03d35db07ad878f033856b023996c6ad562834',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'miner': '0x5a0b54d5dc17e0aadh383d2db43b0a0d3e029c4c',
        'stateRoot': '0xc19957406eadgbae114cb8866d5654b9eb71072080f199c8ef0fd62dfea5d22b',
        'transactionsRoot': '0xd44a9a0a3ae5dge666fceb6c91a72dba79e1207738a6cb8cf04c70b3bbcea2f1',
        'receiptsRoot': '0x16af3be621850f13bt6ee6046ccd2c07aca29e86d551ea3001741aff071cc023',
        'logsBloom': '0x802204b00d50421c05672a45dc20a220634889858e810c02410db8184833914130a6185918000133191e0822005305ec9e39060a8f0b045108500007942d6c6007204e3004406c59c9c639ab08448aa9afcab8290a29201c5e4c898c800432021c4249002a422014473c441c03210c24a1046e50c23200140848403b0cf78b70046703870f835960307d82c83a0440500c4319b361ed85ba45501944043444800a258800b31c28909000b9be490078494c1a1a98d0c00c22202470fc8002c0c000001202a54305040405c16100fa248a84025a488a4e8018b01c21e2f12c220a0210a5311a0811095a03990d804860520c68b809df35a85000180aa328580088',
        'difficulty': '0x87f59ded98db2',
        'number': '0xa07f61',
        'gasLimit': '0xb2dc2f',
        'gasUsed': '0xb2928b',
        'timestamp': '0x5f1a0dc4',
        'extraData': '0x6574682d70726f2d687a6f2d74303033',
        'mixHash': '0x1ab019ac8faa1de6b3400283cbcb75bc643b427c76165b2393da42337d807a58',
        'nonce': '0x1131ff9405312732'
    },
    'transactions': [
        {
            "from": "0xbd4e113ee68bcbbf768ba1d6c7a14e003362979a",
            "gas": "0x9bba",
            "gasPrice": "0x41dcf5dbe",
            "hash": "0x0d96b711bdcc89b59f0fdfa963158394cea99cedce52d0e4f4a56839145a814a",
            "input": "0xea1790b90000000000000000000000000000000000000000000000000000000000000060000000000000000000000000000000000000000000000000000000005ee3f95400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000041d880a37ae74a2593900da75b3cae6335b5f58997c6f426e98e42f55d3d5cd6487369ec0250a923cf1f45a39aa551b68420ec04582d1a68bcab9a70240ae39f261b00000000000000000000000000000000000000000000000000000000000000",
            "nonce": "0x1e8",
            "r": "0x561fc2c4428e8d3ff1e48ce07322a98ea6c8c5836bc79e7d60a6ed5d37a124a2",
            "s": "0x7ab1477ccb14143ba9afeb2f98099c85dd4175f09767f03d47f0733467eadde2",
            "to": "0xd7bec4d6bf6fc371eb51611a50540f0b59b5f896",
            "v": "0x25",
            "value": "0x0"
        }
    ],
    'uncles': [
        {
            'parentHash': '0x54c47498e717b407b2a169e67b03d35db07ad878f033856b023996c6ad562834',
            'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'miner': '0x5a0b74d5dc17e0aadc383d2db43b0a0d3e029c4c',
            'stateRoot': '0xc19957406eaddbae114cb8866d5654b9eb71072080f199c8ef0fd62dfea5d22b',
            'transactionsRoot': '0xd44a9a0a3ae5d5e666fceb6c91a72dba79e1207738a6cb8cf04c70b3bbcea2f1',
            'receiptsRoot': '0x16af3be621850f13bf6ee6046ccd2c07aca29e86d551ea3001741aff071cc023',
            'logsBloom': '0x802204b00d50421c05612a45dc20a220634889858e810c02410db8184833914130a6185918000133191e0822005305ec9e39060a8f0b045108500007942d6c6007204e3004406c59c9c639ab08448aa9afcab8290a29201c5e4c898c800432021c4249002a422014473c441c03210c24a1046e50c23200140848403b0cf78b70046703870f835960307d82c83a0440500c4319b361ed85ba45501944043444800a258800b31c28909000b9be490078494c1a1a98d0c00c22202470fc8002c0c000001202a54305040405c16100fa248a84025a488a4e8018b01c21e2f12c220a0210a5311a0811095a03990d804860520c68b809df35a85000180aa328580088',
            'difficulty': '0x87f59ded98db2',
            'number': '0xa07f61',
            'gasLimit': '0xb2dc2f',
            'gasUsed': '0xb2928b',
            'timestamp': '0x5f1a0dc4',
            'extraData': '0x6574682d70726f2d687a6f2d74303033',
            'mixHash': '0x1ab019ac8faa1de6b3400283cbcb75bc643b427c76165b2393da42337d807a58',
            'nonce': '0x1131ff9405312732'
        }
    ]
}

_recover_public_key = crypto_utils.recover_public_key


class EthNewBlockFeedTest(AbstractTestCase):

    def setUp(self) -> None:
        crypto_utils.recover_public_key = MagicMock(
            return_value=bytes(32))

        pub_key = "a04f30a45aae413d0ca0f219b4dcb7049857bc3f91a6351288cce603a2c9646294a02b987bf6586b370b2c22d74662355677007a14238bb037aedf41c2d08866"
        opts = gateway_helpers.get_gateway_opts(8000, include_default_eth_args=True, pub_key=pub_key,
                                                                  track_detailed_sent_messages=True)
        if opts.use_extensions:
            helpers.set_extensions_parallelism()
        node_ssl_service = MockNodeSSLService(EthGatewayNode.NODE_TYPE, MagicMock())
        self.node = EthGatewayNode(opts, node_ssl_service)
        self.sut = EthNewBlockFeed(self.node)
        self.message_converter = EthNormalMessageConverter()
        crypto_utils.recover_public_key = MagicMock(
            return_value=bytes(32))

    def tearDown(self) -> None:
        crypto_utils.recover_public_key = _recover_public_key

    def generate_new_eth_block(self) -> NewBlockEthProtocolMessage:
        block_message = NewBlockEthProtocolMessage(
            None,
            mock_eth_messages.get_dummy_block(1, mock_eth_messages.get_dummy_block_header(5, int(time.time()))),
            10
        )
        block_message.serialize()
        return block_message

    @async_test
    async def test_publish_from_internal_block(self):
        subscriber = self.sut.subscribe({})

        block_message = self.generate_new_eth_block()
        internal_block_message = InternalEthBlockInfo.from_new_block_msg(block_message)
        block_hash_str = f"0x{str(internal_block_message.block_hash())}"
        self.sut.publish(
            EthRawBlock(
                block_message.number(),
                block_message.block_hash(),
                FeedSource.BLOCKCHAIN_SOCKET,
                iter([internal_block_message])
            )
        )
        received_block = await subscriber.receive()
        self._verify_block(block_hash_str, received_block)

    @async_test
    async def test_publish_block_no_subscribers(self):
        self.sut.serialize = MagicMock(wraps=self.sut.serialize)
        block_message = self.generate_new_eth_block()
        internal_block_message = InternalEthBlockInfo.from_new_block_msg(block_message)
        self.sut.publish(
            EthRawBlock(
                block_message.number(),
                block_message.block_hash(),
                FeedSource.BLOCKCHAIN_SOCKET,
                iter([internal_block_message])
            )
        )

        self.sut.serialize.assert_not_called()

    def _verify_block(self, block_hash_str, received_block):
        self.assertEqual(block_hash_str, received_block["hash"])
        block_items = [
            "parent_hash", "sha3_uncles", "miner", "state_root", "transactions_root",
            "receipts_root", "number", "logs_bloom", "difficulty", "gas_used",
            "gas_limit", "timestamp", "extra_data", "mix_hash", "nonce",
        ]
        tx_items = [
            "from", "gas", "gas_price", "input", "value", "to", "nonce", "v", "r", "s",
        ]

        for item in block_items:
            self.assertIn(item, received_block["header"])
        for uncle in received_block["uncles"]:
            for item in block_items:
                self.assertIn(item, uncle)

        for tx in received_block["transactions"]:
            for item in tx_items:
                self.assertIn(item, tx)




