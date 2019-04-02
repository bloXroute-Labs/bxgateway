from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway.utils import network_latency
from bxgateway.utils.node_latency_info import NodeLatencyInfo
from bxcommon.models.outbound_peer_model import OutboundPeerModel


class NetworkLatencyTests(AbstractTestCase):
    def test_get_best_relay(self):
        relays = [OutboundPeerModel("34.227.149.148", node_id="0"), OutboundPeerModel("35.198.90.230", node_id="1"),
                  OutboundPeerModel("52.221.211.38", node_id="2"), OutboundPeerModel("34.245.23.125", node_id="3"),
                  OutboundPeerModel("34.238.245.201", node_id="4")]
        sorted_relays_ping_latency = [NodeLatencyInfo(relays[4], 100), NodeLatencyInfo(relays[1], 101),
                                      NodeLatencyInfo(relays[1], 109), NodeLatencyInfo(relays[2], 120),
                                      NodeLatencyInfo(relays[0], 130)]
        best_relay = network_latency._get_best_relay(sorted_relays_ping_latency, relays)
        self.assertEqual("1", best_relay.node.node_id)

    def test_get_best_relay_1(self):
        relays = [OutboundPeerModel("34.227.149.148", node_id="0"), OutboundPeerModel("35.198.90.230", node_id="1"),
                  OutboundPeerModel("52.221.211.38", node_id="2"), OutboundPeerModel("34.245.23.125", node_id="3"),
                  OutboundPeerModel("34.238.245.201", node_id="4")]
        sorted_relays_ping_latency = [NodeLatencyInfo(relays[3], 100), NodeLatencyInfo(relays[1], 101),
                                      NodeLatencyInfo(relays[4], 109), NodeLatencyInfo(relays[2], 120),
                                      NodeLatencyInfo(relays[0], 130)]
        best_relay = network_latency._get_best_relay(sorted_relays_ping_latency, relays)
        self.assertEqual("1", best_relay.node.node_id)

    def test_get_best_relay_sorted_relays(self):
        relays = [OutboundPeerModel("34.227.149.148", node_id="0"), OutboundPeerModel("35.198.90.230", node_id="1"),
                  OutboundPeerModel("52.221.211.38", node_id="2"), OutboundPeerModel("34.245.23.125", node_id="3"),
                  OutboundPeerModel("34.238.245.201", node_id="4")]
        sorted_relays_ping_latency = [NodeLatencyInfo(relays[0], 100), NodeLatencyInfo(relays[1], 101),
                                      NodeLatencyInfo(relays[2], 109), NodeLatencyInfo(relays[3], 120),
                                      NodeLatencyInfo(relays[4], 130)]
        best_relay = network_latency._get_best_relay(sorted_relays_ping_latency, relays)
        self.assertEqual("0", best_relay.node.node_id)

    def test_get_best_relay_one_relay(self):
        relays = [OutboundPeerModel("34.227.149.148", node_id="0")]
        sorted_relays_ping_latency = [NodeLatencyInfo(relays[0], 100)]
        best_relay = network_latency._get_best_relay(sorted_relays_ping_latency, relays)
        self.assertEqual("0", best_relay.node.node_id)
