from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.testing.mocks import mock_eth_messages


class TransactionTest(AbstractTestCase):
    def test_eip_155_to_json(self):
        transactions_message = mock_eth_messages.EIP_155_TRANSACTIONS_MESSAGE

        transactions = transactions_message.get_transactions()
        assert len(transactions) == 1

        parsed_transaction_json = transactions[0].to_json()
        self.assertEqual(
            "0x622961e7f76b5e573df44afdeb712749bbee398d", parsed_transaction_json["from"]
        )
        self.assertEqual(165969, parsed_transaction_json["gas"])
        self.assertEqual(53000000000, parsed_transaction_json["gas_price"])
        self.assertEqual(
            f"0x{mock_eth_messages.EIP_155_TRANSACTION_HASH}",
            parsed_transaction_json["hash"],
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
        self.assertEqual(294, parsed_transaction_json["nonce"])
        self.assertEqual(
            "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", parsed_transaction_json["to"]
        )
        self.assertEqual(20000000000000000000, parsed_transaction_json["value"])
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
        self.assertEqual(60000, parsed_transaction_json["gas"])
        self.assertEqual(31000000000, parsed_transaction_json["gas_price"])
        self.assertEqual(
            f"0x{mock_eth_messages.NOT_EIP_155_TRANSACTION_HASH}",
            parsed_transaction_json["hash"],
        )
        self.assertEqual(
            "0xa9059cbb0000000000000000000000000f302271d0dfbd66c5f78a32162eebf0"
            "a8b3512e00000000000000000000000000000000000000000000000075491cca15"
            "133c00",
            parsed_transaction_json["input"],
        )
        self.assertEqual(100492, parsed_transaction_json["nonce"])
        self.assertEqual(
            "0xa1b19bcd50a24be0cb399c1ec0f7ca546b94a2b0", parsed_transaction_json["to"]
        )
        self.assertEqual(0, parsed_transaction_json["value"])
        self.assertEqual("0x1b", parsed_transaction_json["v"])
        self.assertEqual(
            "0x190466ba930f7cd8f4f0e0fc564bfb75cb2b55d0807fe0a743e586c22757518e",
            parsed_transaction_json["r"],
        )
        self.assertEqual(
            "0x6bb11a089946493e4aa7559d578da39862d8d144f2b3c49afe9fa3591bab052d",
            parsed_transaction_json["s"],
        )
