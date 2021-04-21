import blxr_rlp as rlp

from bxcommon.messages.eth.serializers.transaction import LegacyTransaction, \
    AccessListTransaction
from bxcommon.messages.eth.serializers.transaction_type import EthTransactionType
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.fixture import eth_fixtures
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import (
    TransactionsEthProtocolMessage,
)
from bxgateway.testing.mocks import mock_eth_messages


class TransactionTest(AbstractTestCase):
    def test_legacy_transaction_from_bytes(self):
        transaction = Transaction.deserialize(eth_fixtures.LEGACY_TRANSACTION)
        re_encoded = rlp.encode(transaction)
        self.assertEqual(
            transaction, rlp.decode(re_encoded, Transaction)
        )

        self.assertEqual(EthTransactionType.LEGACY, transaction.transaction_type)
        self.assertIsInstance(transaction, LegacyTransaction)
        self.assertEqual(
            Sha256Hash.from_string(eth_fixtures.LEGACY_TRANSACTION_HASH),
            transaction.hash(),
        )
        self.assertEqual(
            "0xc296825bf94ca41b881390955c2731c1d3eaa059",
            transaction.from_address()
        )
        self.assertEqual(37, transaction.v)

    def test_legacy_transaction_to_json(self):
        transaction_json = Transaction.deserialize(eth_fixtures.LEGACY_TRANSACTION).to_json()
        self.assertEqual(
            f"0x{eth_fixtures.LEGACY_TRANSACTION_HASH}",
            transaction_json["hash"]
        )
        self.assertEqual(
            "0x0",
            transaction_json["type"]
        )
        self.assertEqual(
            "0xc296825bf94ca41b881390955c2731c1d3eaa059",
            transaction_json["from"]
        )
        self.assertEqual(
            "0x25",
            transaction_json["v"]
        )

    def test_legacy_transaction_eip_2718_from_bytes(self):
        transaction = Transaction.deserialize(eth_fixtures.LEGACY_TRANSACTION_EIP_2718)
        re_encoded = rlp.encode(transaction)
        self.assertEqual(
            Transaction.deserialize(eth_fixtures.LEGACY_TRANSACTION),
            transaction
        )
        self.assertEqual(
            transaction, rlp.decode(re_encoded, Transaction)
        )

    def test_access_list_transaction_from_bytes(self):
        transaction: Transaction = Transaction.deserialize(eth_fixtures.ACCESS_LIST_TRANSACTION)
        re_encoded = rlp.encode(transaction)
        self.assertEqual(transaction, rlp.decode(re_encoded, Transaction))

        self.assertEqual(
            EthTransactionType.ACCESS_LIST, transaction.transaction_type
        )
        self.assertIsInstance(transaction, AccessListTransaction)
        self.assertEqual(
            Sha256Hash.from_string(eth_fixtures.ACCESS_LIST_TRANSACTION_HASH),
            transaction.hash(),
        )
        self.assertEqual(1, transaction.chain_id())
        self.assertEqual(0, transaction.v)
        self.assertEqual(
            "0x0087c5900b9bbc051b5f6299f5bce92383273b28",
            transaction.from_address()
        )
        self.assertEqual(3, len(transaction.access_list))

    def test_access_list_transaction_to_json(self):
        transaction_json = Transaction.deserialize(eth_fixtures.ACCESS_LIST_TRANSACTION).to_json()
        self.assertEqual(
            f"0x{eth_fixtures.ACCESS_LIST_TRANSACTION_HASH}",
            transaction_json["hash"]
        )
        self.assertEqual(
            "0x1",
            transaction_json["type"]
        )
        self.assertEqual(
            "0x0087c5900b9bbc051b5f6299f5bce92383273b28",
            transaction_json["from"]
        )
        self.assertEqual(
            "0x0",
            transaction_json["v"]
        )
        self.assertEqual(3, len(transaction_json["access_list"]))

    def test_eip_155_to_json(self):
        transactions_message = mock_eth_messages.EIP_155_TRANSACTIONS_MESSAGE

        transactions = transactions_message.get_transactions()
        assert len(transactions) == 1

        parsed_transaction_json = transactions[0].to_json()
        self.assertEqual(
            "0x622961e7f76b5e573df44afdeb712749bbee398d", parsed_transaction_json["from"]
        )
        self.assertEqual(hex(165969), parsed_transaction_json["gas"])
        self.assertEqual(hex(53000000000), parsed_transaction_json["gas_price"])
        self.assertEqual(
            f"0x{mock_eth_messages.EIP_155_TRANSACTION_HASH}", parsed_transaction_json["hash"],
        )
        self.assertEqual(
            "0x7ff36ab50000000000000000000000000000000000000000000000f63ad7b170"
            "466de7d80000000000000000000000000000000000000000000000000000000000"
            "000080000000000000000000000000622961e7f76b5e573df44afdeb712749bbee"
            "398d000000000000000000000000000000000000000000000000000000005ee26a"
            "790000000000000000000000000000000000000000000000000000000000000002"
            "000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc200"
            "00000000000000000000006b175474e89094c44da98b954eedeac495271d0f",
            parsed_transaction_json["input"],
        )
        self.assertEqual(hex(294), parsed_transaction_json["nonce"])
        self.assertEqual(
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", parsed_transaction_json["to"]
        )
        self.assertEqual(hex(20000000000000000000), parsed_transaction_json["value"])
        self.assertEqual("0x25", parsed_transaction_json["v"])
        self.assertEqual(
            "0xcf5e7717042a53b761dad24b9e6873f2da6bb381ab0bec1a1ba7e15bc924b0b2",
            parsed_transaction_json["r"],
        )
        self.assertEqual(
            "0x5a6d627f0345f84ac6fbf708d30a3ddac8e12105d4430ab6768d81a7a8db7191",
            parsed_transaction_json["s"],
        )

    def test_not_eip_155_to_json(self):
        transactions_message = mock_eth_messages.NOT_EIP_155_TRANSACTIONS_MESSAGE

        transactions = transactions_message.get_transactions()
        assert len(transactions) == 1

        parsed_transaction_json = transactions[0].to_json()
        self.assertEqual(
            "0x96e82255174536dc53e83f23b339f25255174e2b", parsed_transaction_json["from"]
        )
        self.assertEqual(hex(60000), parsed_transaction_json["gas"])
        self.assertEqual(hex(31000000000), parsed_transaction_json["gas_price"])
        self.assertEqual(
            f"0x{mock_eth_messages.NOT_EIP_155_TRANSACTION_HASH}", parsed_transaction_json["hash"],
        )
        self.assertEqual(
            "0xa9059cbb0000000000000000000000000f302271d0dfbd66c5f78a32162eebf0"
            "a8b3512e00000000000000000000000000000000000000000000000075491cca15"
            "133c00",
            parsed_transaction_json["input"],
        )
        self.assertEqual(hex(100492), parsed_transaction_json["nonce"])
        self.assertEqual(
            "0xa1b19bcd50a24be0cb399c1ec0f7ca546b94a2b0", parsed_transaction_json["to"]
        )
        self.assertEqual(hex(0), parsed_transaction_json["value"])
        self.assertEqual("0x1b", parsed_transaction_json["v"])
        self.assertEqual(
            "0x190466ba930f7cd8f4f0e0fc564bfb75cb2b55d0807fe0a743e586c22757518e",
            parsed_transaction_json["r"],
        )
        self.assertEqual(
            "0x6bb11a089946493e4aa7559d578da39862d8d144f2b3c49afe9fa3591bab052d",
            parsed_transaction_json["s"],
        )

    def test_from_json(self):
        result = Transaction.from_json(eth_fixtures.LEGACY_TRANSACTION_JSON_FROM_WS)
        self.assertEqual(488, result.nonce)
        self.assertEqual(17679998398, result.gas_price)
        self.assertEqual(39866, result.start_gas)
        self.assertEqual(
            convert.hex_to_bytes(eth_fixtures.LEGACY_TRANSACTION_JSON["to"][2:]), result.to
        )
        self.assertEqual(0, result.value)
        self.assertEqual(
            convert.hex_to_bytes(eth_fixtures.LEGACY_TRANSACTION_JSON["input"][2:]), result.data
        )
        self.assertEqual(int(eth_fixtures.LEGACY_TRANSACTION_JSON["v"], 16), result.v)
        self.assertEqual(int(eth_fixtures.LEGACY_TRANSACTION_JSON["r"], 16), result.r)
        self.assertEqual(int(eth_fixtures.LEGACY_TRANSACTION_JSON["s"], 16), result.s)
        self.assertEqual(EthTransactionType.LEGACY, result.transaction_type)

        result_json = result.to_json()
        for key, val in eth_fixtures.LEGACY_TRANSACTION_JSON_FROM_WS.items():
            # camelcase problems
            if key == "gasPrice":
                key = "gas_price"
            self.assertEqual(val, result_json[key])

    def test_from_json_access_list(self):
        result = Transaction.from_json(eth_fixtures.ACCESS_LIST_TRANSACTION_JSON)
        self.assertEqual(EthTransactionType.ACCESS_LIST, result.transaction_type)

        result_json = result.to_json()
        for key, val in eth_fixtures.ACCESS_LIST_TRANSACTION_JSON.items():
            # camelcase problems
            if key == "gasPrice":
                key = "gas_price"
            elif key == "chainId":
                key = "chain_id"
            elif key == "accessList":
                continue
            self.assertEqual(val, result_json[key], f"failed on key: {key}")

        for i, access in enumerate(eth_fixtures.ACCESS_LIST_TRANSACTION_JSON["accessList"]):
            self.assertEqual(access["address"], result_json["access_list"][i]["address"])
            self.assertEqual(access["storageKeys"], result_json["access_list"][i]["storage_keys"])

    def test_empty_to_serializes(self):
        sample_transactions = dict(eth_fixtures.LEGACY_TRANSACTION_JSON_FROM_WS)
        sample_transactions["to"] = None

        Transaction.from_json(sample_transactions).to_json()

    def test_openethereum_berlin_transactions(self):
        msg = TransactionsEthProtocolMessage(
            convert.hex_to_bytes(eth_fixtures.OPEN_ETHEREUM_BERLIN_TX_MESSAGE)
        )
        txs = msg.get_transactions()
