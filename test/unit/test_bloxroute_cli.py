import sys

from argparse import ArgumentParser

from bxcommon.test_utils.helpers import async_test
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bloxroute_cli import main


class BloxrouteCliTest(AbstractTestCase):

    @async_test
    async def test_cloud_api_good_invocation(self):
        sys.argv = ["main.py",
                    "--account-id", "9333",
                    "--secret-hash", "aaaa",
                    "--cloud-api",
                    "--blockchain-protocol", "Ethereum",
                    "--blockchain-network", "mainnet"
                    "command", "BLXR_TX"]
        arg_parser: ArgumentParser = ArgumentParser()
        main.add_run_arguments(arg_parser)
        main.add_base_arguments(arg_parser)
        opts, params = arg_parser.parse_known_args()

        stdout_writer = sys.stdout

        valid = main.validate_args(opts, stdout_writer)
        self.assertEqual(True, valid)

    async def test_cloud_api_wrong_invocation(self):
        sys.argv = ["main.py",
                    "--account-id", "9333",
                    "--cloud-api",
                    "--blockchain-protocol", "Ethereum",
                    "--blockchain-network", "mainnet"
                    "command", "BLXR_TX"]
        arg_parser: ArgumentParser = ArgumentParser()
        main.add_run_arguments(arg_parser)
        main.add_base_arguments(arg_parser)
        opts, params = arg_parser.parse_known_args()

        stdout_writer = sys.stdout

        valid = main.validate_args(opts, stdout_writer)
        self.assertEqual(False, valid)

    async def test_bloxroute_cli_good_invocation(self):
        sys.argv = ["main.py",
                    "--rpc-user", "",
                    "--rpc-password", "",
                    "--rpc-host", "17.2.17.0.1",
                    "--rpc-port", "4444"
                    "--blockchain-protocol", "Ethereum",
                    "--blockchain-network", "mainnet"
                    "command", "BLXR_TX"]
        arg_parser: ArgumentParser = ArgumentParser()
        main.add_run_arguments(arg_parser)
        main.add_base_arguments(arg_parser)
        opts, params = arg_parser.parse_known_args()

        stdout_writer = sys.stdout

        valid = main.validate_args(opts, stdout_writer)
        self.assertEqual(True, valid)

    async def test_bloxroute_cli_wrong_invocation(self):
        sys.argv = ["main.py",
                    "--rpc-user", "",
                    "--rpc-port", "4444"
                    "--blockchain-protocol", "Ethereum",
                    "--blockchain-network", "mainnet"
                    "command", "BLXR_TX"]
        arg_parser: ArgumentParser = ArgumentParser()
        main.add_run_arguments(arg_parser)
        main.add_base_arguments(arg_parser)
        opts, params = arg_parser.parse_known_args()

        stdout_writer = sys.stdout

        valid = main.validate_args(opts, stdout_writer)
        self.assertEqual(True, valid)
