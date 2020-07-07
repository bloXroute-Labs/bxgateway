import sys
import mock
import argparse
from unittest import skip

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase

from bxgateway import main
from bxgateway import gateway_opts
from bxgateway import log_messages


class MainTest(AbstractTestCase):

    def test_parse_peer_string(self):
        peers = main.parse_peer_string("127.0.0.1:8000,192.168.1.1:80001")
        self.assertEqual(2, len(peers))
        self.assertEqual(OutboundPeerModel("127.0.0.1", 8000), peers[0])
        self.assertEqual(OutboundPeerModel("192.168.1.1", 80001), peers[1])

        peers2 = main.parse_peer_string("127.0.0.1:8000, 192.168.1.1:80001")
        self.assertEqual(peers, peers2)

    def test_get_opts_outbound_peers_provided(self):
        argv = ["--blockchain-ip", "172.17.0.1",
                "--blockchain-port", "9333",
                "--blockchain-protocol", "Bitcoin",
                "--blockchain-network", "Mainnet",
                "--peer-relays", "127.0.0.1:8000,127.0.0.1:8001",
                "--peer-gateways", "127.0.1.1:8000,127.0.1.0:8001",
                "--source-version", "1.2.3",
                "--external-port", "7000"]
        args = main.get_opts(argv)
        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(2, len(args.peer_gateways))
        self.assertEqual(2, len(args.peer_relays))
        self.assertEqual(4, len(args.outbound_peers))

    def test_get_opts_outbound_peers_not_provided(self):
        argv = ["--blockchain-ip", "172.17.0.1",
                "--blockchain-port", "9333",
                "--blockchain-protocol", "Bitcoin",
                "--blockchain-network", "Mainnet",
                "--source-version", "1.2.3",
                "--external-port", "7000"]
        args = main.get_opts(argv)
        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(0, len(args.peer_gateways))
        self.assertEqual(0, len(args.peer_relays))
        self.assertEqual(0, len(args.outbound_peers))

    def test_get_opts_blockchain_info_not_provided(self):
        argv = ["--blockchain-ip", "172.17.0.1",
                "--blockchain-port", "9333",
                "--source-version", "1.2.3",
                "--external-port", "7000"]
        args = main.get_opts(argv)
        account_model = BdnAccountModelBase(
            "fake_id", "fake_account_name", "fake_certificate", blockchain_network="Mainnet",
            blockchain_protocol="Bitcoin"
        )
        args.set_account_options(account_model)

        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(0, len(args.peer_gateways))
        self.assertEqual(0, len(args.peer_relays))
        self.assertEqual(0, len(args.outbound_peers))

    def test_enode_parsing(self):
        fake_public_key = "X" * 64 * 2
        fake_blockchain_ip = "172.17.0.1"
        fake_port = 30303
        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--enode", f"enode://{fake_public_key}@{fake_blockchain_ip}:{fake_port}?discport=0"]
        args = main.get_opts(argv)
        self.assertEqual(fake_public_key, args.node_public_key)
        self.assertEqual(fake_blockchain_ip, args.blockchain_ip)
        self.assertEqual(fake_port, args.blockchain_port)

    def test_network_is_optional(self):
        argv = ["--blockchain-ip", "172.17.0.1",
                "--blockchain-port", "9333",
                "--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "Bitcoin"]
        args = main.get_opts(argv)
        account_model = BdnAccountModelBase(
            "fake_id", "fake_account_name", "fake_certificate"
        )
        args.set_account_options(account_model)
        args.validate_network_opts()

        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("mainnet", args.blockchain_network)
        self.assertEqual(0, len(args.peer_gateways))
        self.assertEqual(0, len(args.peer_relays))
        self.assertEqual(0, len(args.outbound_peers))

    def test_blockchain_ip_missing_eth(self):
        mock_logger = mock.MagicMock()
        gateway_opts.logger.fatal = mock_logger

        argv = [
                "--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum"
                ]
        args = main.get_opts(argv)
        self.assertEqual(args.blockchain_protocol, "ethereum")
        self.assertIsNone(args.blockchain_ip)
        self.assertIsNone(args.enode)
        with self.assertRaises(SystemExit):
            args.validate_network_opts()
        mock_logger.assert_called_with(
            log_messages.ETH_MISSING_BLOCKCHAIN_IP, exc_info=False)

    def test_blockchain_ip_missing(self):
        mock_logger = mock.MagicMock()
        gateway_opts.logger.fatal = mock_logger

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "bitcoin"
                ]
        args = main.get_opts(argv)
        self.assertEqual(args.blockchain_protocol, "bitcoin")
        self.assertIsNone(args.blockchain_ip)
        self.assertIsNone(args.enode)
        with self.assertRaises(SystemExit):
            args.validate_network_opts()

        mock_logger.assert_called_with(
            log_messages.MISSING_BLOCKCHAIN_IP, exc_info=False)

    def test_validate_blockchain_ip(self):
        mock_logger = mock.MagicMock()
        gateway_opts.logger.fatal = mock_logger

        with self.assertRaises(SystemExit):
            gateway_opts.validate_blockchain_ip(None)

        mock_logger = mock.MagicMock()
        gateway_opts.logger.warning = mock_logger
        gateway_opts.validate_blockchain_ip("127.0.0.1", is_docker=True)
        mock_logger.assert_called_with(
            log_messages.INVALID_BLOCKCHAIN_IP, exc_info=False)

        self.assertEqual("127.0.0.1",
                         gateway_opts.validate_blockchain_ip("127.0.0.1", is_docker=False))
