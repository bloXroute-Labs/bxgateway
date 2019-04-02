from typing import List
import subprocess
from bxgateway.gateway_constants import PING_TIMEOUT_S
from bxgateway.utils.node_latency_info import NodeLatencyInfo
from bxgateway import gateway_constants
from bxcommon.utils import logger


def get_ping_latency(ip):
    try:
        res = subprocess.Popen(["ping", "-c", "1", ip], stdout=subprocess.PIPE)
        if res:
            output = res.communicate(timeout=PING_TIMEOUT_S)[0].decode()
            ping_latency = float(output.split("time=", 1)[1].split("ms", 1)[0])
        else:
            ping_latency = PING_TIMEOUT_S * 1000
    except subprocess.TimeoutExpired:
        ping_latency = PING_TIMEOUT_S * 1000

    return ping_latency


def get_best_relay_by_ping_latency(relays: list):
    """
    get best relay by pinging each relay and check its latency calculate with its inbound peers
    :param relays:
    :return:
    """
    if len(relays) == 1:
        return relays[0]

    relays_ping_latency = _get_ping_latencies(relays)
    sorted_ping_latencies = sorted(relays_ping_latency, key=lambda relay_ts: relay_ts.latency)
    return_best_relay = _get_best_relay(sorted_ping_latencies, relays)
    logger.info("First relay recommended from api is: {} with latency {} ms, "
                "fastest ping latency relay is: {} with latency {} ms, selected relay is: {} with latency {} ms".
                format(relays[0].node_id,
                       "".join([str(relay.latency) for relay in relays_ping_latency if relay.node == relays[0]]),
                       sorted_ping_latencies[0].node.node_id, sorted_ping_latencies[0].latency,
                       return_best_relay.node.node_id, return_best_relay.latency))

    return return_best_relay.node


def _get_ping_latencies(relays):
    relays_ping_latency = []
    for relay in relays:
        relays_ping_latency.append(NodeLatencyInfo(relay, get_ping_latency(relay.ip)))
    return relays_ping_latency


def _get_best_relay(sorted_ping_latencies: List[NodeLatencyInfo], relays: list):
    fastest_relay = sorted_ping_latencies[0]
    return_best_relay = fastest_relay
    ping_latency_threshold = gateway_constants.FASTEST_PING_LATENCY_THRESHOLD_PERCENT * fastest_relay.latency

    for relay in sorted_ping_latencies[1:]:
        if relay.latency - fastest_relay.latency < ping_latency_threshold:
            if relays.index(relay.node) < relays.index(return_best_relay.node):
                return_best_relay = relay
        else:
            break

    return return_best_relay
