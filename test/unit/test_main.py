import mock
import os

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase

from bxgateway import main, argument_parsers
from bxgateway import gateway_opts
from bxgateway import log_messages
from bxgateway.utils.blockchain_peer_info import BlockchainPeerInfo


class MainTest(AbstractTestCase):

    def test_parse_peer_string(self):
        peers = main.parse_peer_string("127.0.0.1:8000:GATEWAY,192.168.1.1:80001:GATEWAY")
        self.assertEqual(2, len(peers))
        self.assertEqual(OutboundPeerModel("127.0.0.1", 8000), peers[0])
        self.assertEqual(OutboundPeerModel("192.168.1.1", 80001), peers[1])

        peers2 = main.parse_peer_string("127.0.0.1:8000:GATEWAY, 192.168.1.1:80001:GATEWAY")
        self.assertEqual(peers, peers2)

    def test_get_opts_outbound_peers_provided(self):
        argv = ["--blockchain-ip", "172.17.0.1",
                "--blockchain-port", "9333",
                "--blockchain-protocol", "Bitcoin",
                "--blockchain-network", "Mainnet",
                "--peer-relays", "127.0.0.1:8000:RELAY,127.0.0.1:8001:RELAY",
                "--peer-gateways", "127.0.1.1:8000:GATEWAY,127.0.1.0:8001:GATEWAY",
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

    def test_eth_no_public_key(self):
        mock_logger = mock.MagicMock()
        gateway_opts.logger.fatal = mock_logger
        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum",
                "--blockchain-ip", "172.17.0.1",
                "--blokchain-port", "30303"]
        args = main.get_opts(argv)
        with self.assertRaises(SystemExit):
            args.validate_network_opts()

        mock_logger.assert_called_with(log_messages.ETH_MISSING_NODE_PUBLIC_KEY, exc_info=False)

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
        mock_logger.assert_called_with(log_messages.ETH_MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS, exc_info=False)

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

        mock_logger.assert_called_with(log_messages.MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS, exc_info=False)

    def test_validate_blockchain_ip(self):
        mock_logger = mock.MagicMock()
        gateway_opts.logger.warning = mock_logger

        with self.assertRaises(SystemExit):
            gateway_opts.validate_blockchain_ip(None)

        gateway_opts.validate_blockchain_ip("127.0.0.1", is_docker=True)
        mock_logger.assert_called_with(log_messages.INVALID_BLOCKCHAIN_IP, exc_info=False)
        self.assertEqual("127.0.0.1", gateway_opts.validate_blockchain_ip("127.0.0.1", is_docker=False))

    def test_blockchain_peers_eth_from_args(self):
        blockchain_ip_1 = "172.17.0.1"
        blockchain_port_1 = 30303
        public_key_1 = "X" * 64 * 2
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1, public_key_1)
        blockchain_ip_2 = "172.17.0.2"
        blockchain_port_2 = 30304
        public_key_2 = "Y" * 64 * 2
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2, public_key_2)

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum",
                "--blockchain-peers", f"enode://{public_key_1}@{blockchain_ip_1}:{blockchain_port_1}, "
                                      f"enode://{public_key_2}@{blockchain_ip_2}:{blockchain_port_2}"]
        args = main.get_opts(argv)
        self.assertEqual(2, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)

    def test_blockchain_peers_eth_wrong_format(self):
        mock_logger = mock.MagicMock()
        argument_parsers.logger.fatal = mock_logger
        blockchain_ip_1 = "172.17.0.1"
        blockchain_port_1 = 30303
        blockchain_peer_string_1 = f"{blockchain_ip_1}:{blockchain_port_1}"
        blockchain_ip_2 = "172.17.0.2"
        blockchain_port_2 = 30304
        blockchain_peer_string_2 = f"{blockchain_ip_2}:{blockchain_port_2}"

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum",
                "--blockchain-peers", f"{blockchain_peer_string_1}, {blockchain_peer_string_2}"]
        with self.assertRaises(SystemExit):
            main.get_opts(argv)

        mock_logger.assert_called_with(
            log_messages.ETH_PARSER_INVALID_ENODE_LENGTH,
            blockchain_peer_string_1, len(blockchain_peer_string_1), exc_info=False
        )

    def test_blockchain_peers_btc(self):
        blockchain_ip_1 = "172.17.0.1"
        blockchain_port_1 = 8333
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1)
        blockchain_ip_2 = "172.17.0.2"
        blockchain_port_2 = 8555
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2)

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "bitcoin",
                "--blockchain-peers", f"{blockchain_ip_1}:{blockchain_port_1}, {blockchain_ip_2}:{blockchain_port_2}"]
        args = main.get_opts(argv)
        self.assertEqual(2, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)

    def test_blockchain_peers_eth_from_file_only(self):
        blockchain_ip_1 = "1.2.3.4"
        blockchain_port_1 = 20202
        public_key_1 = "00029fb539bbbebc7bcc986bca2b1d3e262a1133901c3cc699f8dd9cba91df51ede5fed9c2c25b74425d64344a9a9d393904c6f0f8bd95cc0c5e2699b6a19ea1"
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1, public_key_1)
        blockchain_ip_2 = "5.6.7.8"
        blockchain_port_2 = 30303
        public_key_2 = "873235d21c380e2b7239cfdcb3a02bd1b92232aed81d36cec394749bd578bd47ee73a9c9270a2f196f4b7734b0edeeedc8788abea4abeffd1ae69f423e9f70ea"
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2, public_key_2)
        root_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(root_dir, "samples/blockchain_peers_eth_sample_file.txt")

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum",
                "--blockchain-peers-file", file_path]
        args = main.get_opts(argv)
        self.assertEqual(2, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)

    def test_blockchain_peers_eth_from_file_and_args(self):
        blockchain_ip_1 = "1.2.3.4"
        blockchain_port_1 = 20202
        public_key_1 = "00029fb539bbbebc7bcc986bca2b1d3e262a1133901c3cc699f8dd9cba91df51ede5fed9c2c25b74425d64344a9a9d393904c6f0f8bd95cc0c5e2699b6a19ea1"
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1, public_key_1)
        blockchain_ip_2 = "5.6.7.8"
        blockchain_port_2 = 30303
        public_key_2 = "873235d21c380e2b7239cfdcb3a02bd1b92232aed81d36cec394749bd578bd47ee73a9c9270a2f196f4b7734b0edeeedc8788abea4abeffd1ae69f423e9f70ea"
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2, public_key_2)
        blockchain_ip_3 = "172.17.0.1"
        blockchain_port_3 = 30303
        public_key_3 = "X" * 64 * 2
        blockchain_peer_3 = BlockchainPeerInfo(blockchain_ip_3, blockchain_port_3, public_key_3)
        root_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(root_dir, "samples/blockchain_peers_eth_sample_file.txt")

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "ethereum",
                "--blockchain-peers-file", file_path,
                "--blockchain-peers", f"enode://{public_key_3}@{blockchain_ip_3}:{blockchain_port_3}"]
        args = main.get_opts(argv)
        self.assertEqual(3, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_3 in args.blockchain_peers)

    def test_blockchain_peers_btc_from_file_only(self):
        blockchain_ip_1 = "1.2.3.4"
        blockchain_port_1 = 8200
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1)
        blockchain_ip_2 = "5.6.7.8"
        blockchain_port_2 = 8300
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2)
        root_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(root_dir, "samples/blockchain_peers_btc_sample_file.txt")

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "bitcoin",
                "--blockchain-peers-file", file_path]
        args = main.get_opts(argv)
        self.assertEqual(2, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)

    def test_blockchain_peers_btc_from_file_and_args(self):
        blockchain_ip_1 = "1.2.3.4"
        blockchain_port_1 = 8200
        blockchain_peer_1 = BlockchainPeerInfo(blockchain_ip_1, blockchain_port_1)
        blockchain_ip_2 = "5.6.7.8"
        blockchain_port_2 = 8300
        blockchain_peer_2 = BlockchainPeerInfo(blockchain_ip_2, blockchain_port_2)
        blockchain_ip_3 = "172.17.0.1"
        blockchain_port_3 = 8400
        blockchain_peer_3 = BlockchainPeerInfo(blockchain_ip_3, blockchain_port_3)
        root_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(root_dir, "samples/blockchain_peers_btc_sample_file.txt")

        argv = ["--source-version", "1.2.3",
                "--external-port", "7000",
                "--blockchain-protocol", "bitcoin",
                "--blockchain-peers-file", file_path,
                "--blockchain-peers", f"{blockchain_ip_3}:{blockchain_port_3}"]
        args = main.get_opts(argv)
        self.assertEqual(3, len(args.blockchain_peers))
        self.assertTrue(blockchain_peer_1 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_2 in args.blockchain_peers)
        self.assertTrue(blockchain_peer_3 in args.blockchain_peers)
