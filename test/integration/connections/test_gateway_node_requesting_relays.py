import time
from unittest.mock import MagicMock, call

from bxcommon import constants
from bxcommon.connections.connection_state import ConnectionState
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.models.node_type import NodeType
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.services import sdn_http_service
from bxcommon.services.threaded_request_service import ThreadedRequestService
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import ping_latency
from bxcommon.utils.alarm_queue import AlarmId, AlarmQueue
from bxcommon.utils.ping_latency import NodeLatencyInfo
from bxgateway.testing import gateway_helpers
from bxgateway.testing.mocks.mock_gateway_node import MockGatewayNode


class GatewayNodeRequestingRelaysTest(AbstractTestCase):

    def setUp(self) -> None:
        self.gateway_node = MockGatewayNode(
            gateway_helpers.get_gateway_opts(8000, split_relays=True)
        )
        self.gateway_node.alarm_queue = AlarmQueue()
        self.gateway_node.requester = ThreadedRequestService(
            "mock_thread_service",
            self.gateway_node.alarm_queue,
            constants.THREADED_HTTP_POOL_SLEEP_INTERVAL_S
        )
        self.gateway_node.requester.start()

        self.outbound_peer_models = [
            OutboundPeerModel("1.1.1.1", 1609, node_type=NodeType.RELAY_BLOCK),
            OutboundPeerModel("1.1.1.2", 1609, node_type=NodeType.RELAY_BLOCK),
            OutboundPeerModel("1.1.1.3", 1609, node_type=NodeType.RELAY_BLOCK),
        ]
        sdn_http_service.fetch_potential_relay_peers_by_network = MagicMock(
            side_effect=lambda *args: self.outbound_peer_models
        )
        sdn_http_service.submit_peer_connection_error_event = MagicMock()
        self.latencies = [10, 20, 30]
        ping_latency.get_ping_latencies = MagicMock(
            side_effect=lambda *args: [
                NodeLatencyInfo(self.outbound_peer_models[0], self.latencies[0]),
                NodeLatencyInfo(self.outbound_peer_models[1], self.latencies[1]),
                NodeLatencyInfo(self.outbound_peer_models[2], self.latencies[2]),
            ]
        )
        self.gateway_node.enqueue_connection = MagicMock()

    def test_simple_relay_connect_flow(self):
        self._run_connection_process()
        self._assert_connects_to_relays(0)
        self._assert_reevaluation_alarm_set()

    def test_simple_relay_connect_flow_recommended_peer_bad(self):
        self.latencies = [100, 20, 30]
        self._run_connection_process()

        self._assert_connects_to_relays(1)
        self._assert_reevaluation_alarm_set()

    def test_simple_relay_connect_flow_recommend_peer_worse_but_close(self):
        self.latencies = [21, 20, 30]
        self._run_connection_process()

        self._assert_connects_to_relays(0)
        self._assert_reevaluation_alarm_set()

    def test_relay_connection_failure_flow(self):
        self._run_connection_process()
        self._assert_reevaluation_alarm_set()
        reevaluation_alarm = self.gateway_node.check_relay_alarm_id

        self.gateway_node.on_failed_connection_retry(
            self.outbound_peer_models[0].ip,
            self.outbound_peer_models[0].port,
            ConnectionType.RELAY_BLOCK,
            ConnectionState.ESTABLISHED
        )

        # alarm should be scheduled to get new peers
        next_scheduled_alarm = self.gateway_node.check_relay_alarm_id
        self.assertLess(next_scheduled_alarm.fire_time - time.time(), 10)

        time.time = MagicMock(return_value=time.time() + 10)
        self.gateway_node.alarm_queue.fire_alarms()

        self.assertNotEqual(reevaluation_alarm, next_scheduled_alarm)
        self._assert_alarm_not_in_queue(reevaluation_alarm)

        self._wait_for_connection_process()
        self._assert_connects_to_relays(0)
        self._assert_reevaluation_alarm_set()

        final_scheduled_alarm = self.gateway_node.check_relay_alarm_id
        self.assertNotEqual(final_scheduled_alarm, next_scheduled_alarm)
        self.assertNotEqual(final_scheduled_alarm, reevaluation_alarm)

        self._assert_alarm_in_queue(final_scheduled_alarm)
        self._assert_alarm_not_in_queue(reevaluation_alarm)
        self._assert_alarm_not_in_queue(next_scheduled_alarm)

    def test_better_relays_from_sdn(self):
        self.gateway_node.peer_relays = {
            OutboundPeerModel("2.1.1.1", 1609, node_type=NodeType.RELAY_BLOCK),
            OutboundPeerModel("2.1.1.2", 1609, node_type=NodeType.RELAY_BLOCK),
        }
        self.gateway_node.peer_transaction_relays = {
            OutboundPeerModel("2.1.1.1", 1610, node_type=NodeType.RELAY_TRANSACTION),
            OutboundPeerModel("2.1.1.2", 1610, node_type=NodeType.RELAY_TRANSACTION),
        }
        self._run_connection_process()
        self._assert_connects_to_relays(0)
        self._assert_reevaluation_alarm_set()

    def _run_connection_process(self):
        self.assertIsNotNone(self.gateway_node.check_relay_alarm_id)
        alarm_id = self.gateway_node.check_relay_alarm_id

        # fire alarm on checking connections
        alarm_id.alarm.fire()

        self._wait_for_connection_process()

    def _wait_for_connection_process(self):
        self._reset_mocks()

        # give threaded request pool time to complete
        time.sleep(0.01)
        self.gateway_node.alarm_queue.fire_alarms()

        sdn_http_service.fetch_potential_relay_peers_by_network.assert_called_once()

        time.sleep(0.01)
        self.gateway_node.alarm_queue.fire_alarms()

        ping_latency.get_ping_latencies.assert_called_once()

    def _assert_connects_to_relays(self, index: int):
        self.gateway_node.enqueue_connection.assert_has_calls(
            [
                call(
                    self.outbound_peer_models[index].ip,
                    self.outbound_peer_models[index].port,
                    ConnectionType.RELAY_ALL
                ),
            ],
            any_order=True
        )

    def _assert_reevaluation_alarm_set(self):
        # new alarm set for four hours in the future
        self.assertIsNotNone(self.gateway_node.check_relay_alarm_id)
        time_to_fire = self.gateway_node.check_relay_alarm_id.fire_time - time.time()
        self.assertTrue(abs(4 * 60 * 60 - time_to_fire) < 1)
        self._assert_only_one_relay_alarm()

    def _assert_alarm_not_in_queue(self, alarm_id: AlarmId):
        for scheduled_alarm_id in self.gateway_node.alarm_queue.alarms:
            if scheduled_alarm_id == alarm_id and scheduled_alarm_id.is_active:
                self.fail("Alarm was still in queue.")

    def _assert_alarm_in_queue(self, alarm_id: AlarmId):
        for scheduled_alarm_id in self.gateway_node.alarm_queue.alarms:
            if alarm_id == scheduled_alarm_id:
                return
        else:
            self.fail("Alarm not found in queue.")

    def _assert_only_one_relay_alarm(self):
        found = False
        for scheduled_alarm_id in self.gateway_node.alarm_queue.alarms:
            if (
                scheduled_alarm_id.is_active
                and scheduled_alarm_id.alarm.fn
                == self.gateway_node.sync_and_send_request_for_relay_peers
            ):
                if found:
                    self.fail("Multiple scheduled alarms for getting relay peers.")
                else:
                    found = True

    def _reset_mocks(self):
        sdn_http_service.fetch_potential_relay_peers_by_network.reset_mock()
        ping_latency.get_ping_latencies.reset_mock()
