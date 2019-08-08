from typing import List
import subprocess
from concurrent.futures import ThreadPoolExecutor
from bxgateway.utils.node_latency_info import NodeLatencyInfo
from bxgateway import gateway_constants
from bxcommon.utils import logger
from bxcommon.models.outbound_peer_model import OutboundPeerModel


def get_ping_latency(relay: OutboundPeerModel):
    try:
        res = subprocess.Popen(["ping", "-c", "1", relay.ip], stdout=subprocess.PIPE)
        if res:
            output = res.communicate(timeout=gateway_constants.PING_TIMEOUT_S)[0].decode()
            ping_latency = float(output.split("time=", 1)[1].split("ms", 1)[0])
        else:
            ping_latency = gateway_constants.PING_TIMEOUT_S * 1000
            logger.error("ping {} is empty".format(relay.ip))
    except subprocess.TimeoutExpired:
        ping_latency = gateway_constants.PING_TIMEOUT_S * 1000
        logger.error("pinging to {} returned timeout".format(relay.ip))
    except Exception as ex:
        logger.error("Error getting ping command output for IP:{}, Error:{}".format(relay.ip, str(ex)))
        ping_latency = gateway_constants.PING_TIMEOUT_S * 1000

    return NodeLatencyInfo(relay, ping_latency)


def get_best_relay_by_ping_latency(relays: List[OutboundPeerModel]):
    """
    get best relay by pinging each relay and check its latency calculate with its inbound peers
    :param relays:
    :return:
    """
    if len(relays) == 1:
        logger.info("First (and only) recommended relay from api is: {}".format(relays[0]))
        return relays[0]

    relays_ping_latency = _get_ping_latencies(relays)
    sorted_ping_latencies = sorted(relays_ping_latency, key=lambda relay_ts: relay_ts.latency)
    best_relay_by_latency = _get_best_relay(sorted_ping_latencies, relays)

    # if best_relay_by_latency's latency is less than RELAY_LATENCY_THRESHOLD_MS, select api's relay
    best_relay_node = relays[0] if best_relay_by_latency.latency < gateway_constants.RELAY_LATENCY_THRESHOLD_MS else best_relay_by_latency.node

    logger.info(
        "First recommended relay from api is: {} with latency {} ms, "
        "fastest ping latency relay is: {} with latency {} ms, selected relay is: {}".format(
            relays[0].node_id, "".join([str(relay.latency) for relay in relays_ping_latency if relay.node == relays[0]]),
            sorted_ping_latencies[0].node.node_id, sorted_ping_latencies[0].latency,
            best_relay_node.node_id
        )
    )

    return best_relay_node


def _get_ping_latencies(relays: List[OutboundPeerModel]):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_ping_latency, relay) for relay in relays]
        results = [future.result() for future in futures]
    return results


def _get_best_relay(sorted_ping_latencies: List[NodeLatencyInfo], relays: List[OutboundPeerModel]):
    fastest_relay = sorted_ping_latencies[0]
    return_best_relay = fastest_relay
    ping_latency_threshold = gateway_constants.FASTEST_PING_LATENCY_THRESHOLD_PERCENT * fastest_relay.latency

    for relay in sorted_ping_latencies[1:]:
        if relay.latency - fastest_relay.latency < ping_latency_threshold and \
                relays.index(relay.node) < relays.index(return_best_relay.node):
                return_best_relay = relay
        else:
            break

    return return_best_relay

