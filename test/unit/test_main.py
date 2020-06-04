import sys
from unittest import skip

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase

from bxgateway import main


class MainTest(AbstractTestCase):

    def test_parse_peer_string(self):
        peers = main.parse_peer_string("127.0.0.1:8000,192.168.1.1:80001")
        self.assertEqual(2, len(peers))
        self.assertEqual(OutboundPeerModel("127.0.0.1", 8000), peers[0])
        self.assertEqual(OutboundPeerModel("192.168.1.1", 80001), peers[1])

        peers2 = main.parse_peer_string("127.0.0.1:8000, 192.168.1.1:80001")
        self.assertEqual(peers, peers2)

    def test_get_opts_outbound_peers_provided(self):
        sys.argv = ["main.py",
                    "--blockchain-ip", "172.17.0.1",
                    "--blockchain-port", "9333",
                    "--blockchain-protocol", "Bitcoin",
                    "--blockchain-network", "Mainnet",
                    "--peer-relays", "127.0.0.1:8000,127.0.0.1:8001",
                    "--peer-gateways", "127.0.1.1:8000,127.0.1.0:8001",
                    "--source-version", "1.2.3",
                    "--external-port", "7000"]
        args = main.get_opts()
        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(2, len(args.peer_gateways))
        self.assertEqual(2, len(args.peer_relays))
        self.assertEqual(4, len(args.outbound_peers))

    def test_get_opts_outbound_peers_not_provided(self):
        sys.argv = ["main.py",
                    "--blockchain-ip", "172.17.0.1",
                    "--blockchain-port", "9333",
                    "--blockchain-protocol", "Bitcoin",
                    "--blockchain-network", "Mainnet",
                    "--source-version", "1.2.3",
                    "--external-port", "7000"]
        args = main.get_opts()
        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(0, len(args.peer_gateways))
        self.assertEqual(0, len(args.peer_relays))
        self.assertEqual(0, len(args.outbound_peers))

    def test_get_opts_blockchain_info_not_provided(self):
        sys.argv = ["main.py",
                    "--blockchain-ip", "172.17.0.1",
                    "--blockchain-port", "9333",
                    "--source-version", "1.2.3",
                    "--external-port", "7000"]
        args = main.get_opts()
        args.account_model = BdnAccountModelBase(
            "fake_id", "fake_account_name", "fake_certificate", blockchain_network="Mainnet",
            blockchain_protocol="Bitcoin"
        )
        args.post_init_tasks()

        self.assertEqual(9333, args.blockchain_port)
        self.assertEqual("bitcoin", args.blockchain_protocol)
        self.assertEqual("Mainnet", args.blockchain_network)
        self.assertEqual(0, len(args.peer_gateways))
        self.assertEqual(0, len(args.peer_relays))
        self.assertEqual(0, len(args.outbound_peers))
