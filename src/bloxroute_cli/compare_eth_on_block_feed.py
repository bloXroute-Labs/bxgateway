import asyncio
import distutils
import json
import logging
import time
import operator
import argparse
import sys

from abc import abstractmethod
from collections import defaultdict, Counter
from distutils.util import strtobool
from enum import Enum
from typing import Union, Optional, Dict
from datetime import datetime

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.rpc.provider.abstract_ws_provider import AbstractWsProvider
from bxcommon.rpc.external.eth_ws_subscriber import EthWsSubscriber
from bloxroute_cli.provider.cloud_wss_provider import CloudWssProvider
from bxcommon.rpc.rpc_errors import RpcError

logger_summary = logging.getLogger("summary")
logger = logging.getLogger(f"{__name__}")
logger_block_result = logging.getLogger(f"{__name__}.block_result")


def setup_logger() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(handler)
    logging.getLogger("bxgateway").setLevel(logging.CRITICAL)
    logger.setLevel(logging.INFO)

    logger_summary.addHandler(logging.StreamHandler(sys.stdout))
    logger_summary.setLevel(logging.INFO)


class ResultCounter:
    def __init__(self, duration: int) -> None:
        self.notifications = dict()
        self.results = defaultdict(dict)
        self.summary = Counter()
        self.delta = Counter()
        self.duration = duration
        self.start = datetime.now()

    def log_results_summary(self, final: bool = True) -> None:
        current_time = datetime.now()
        blocks = sum(self.summary.values())
        node_types = ["Blxr", "Eth"]
        logger_summary.info("")
        if final:
            logger_summary.info(f"Start: {self.start}, End: {current_time}")
        else:
            logger_summary.info(f"Interval summary, Start: {self.start}, Current time: {current_time}")
        logger_summary.info(f"Blocks seen in duration: {len(self.results)}")
        logger_summary.info(f"Number of Blocks with results: {blocks}")
        for node_type in node_types:
            if node_type in self.summary:
                logger_summary.info(f"Number of results from {node_type} first: {self.summary[node_type]}")
        if blocks:
            ratio_from_gateway_first = self.summary.get("Blxr", 0) / blocks
            logger_summary.info(f"Percentage of results seen first from gateway: {ratio_from_gateway_first:.2%}")
            for node_type in node_types:
                if node_type in self.delta:
                    avg_delta = self.delta[node_type] / self.summary[node_type]
                    logger_summary.info(
                        f"Average time difference for results received first from {node_type} (ms): {avg_delta * 1000:.5f}"
                    )

    def log_notification(self, block_number: Union[str, int]) -> None:
        if block_number not in self.notifications:
            self.notifications[block_number] = time.time()

    def log_result_row(self, block_number: Union[str, int], name: str) -> None:
        if name in self.results[block_number]:
            logger.debug("Result was previously logged. Ignore block {} #{}.", name, block_number)
            return
        self.results[block_number][name] = time.time()
        row = self.results[block_number]
        if len(row) == 2:
            (winner, winner_res), (loser, loser_res) = sorted(row.items(), key=operator.itemgetter(1), reverse=False)
            delta = loser_res - winner_res
            if block_number in self.notifications:
                duration_winner = winner_res - self.notifications[block_number]
                duration_loser = loser_res - self.notifications[block_number]
            else:
                duration_winner, duration_loser = None, None
            logger_block_result.info(
                f"Block number: {block_number}: First response {winner}, "
                f"Duration from Block time {duration_winner * 1000:.5f} (ms). "
                f"{loser} Duration from Block time {duration_loser * 1000:.5f} (ms). "
                f"Delta {delta * 1000:.5f} (ms)"
            )
            self.summary[winner] += 1
            self.delta[winner] += delta
        else:
            logger_block_result.debug(
                f"Block number partial results: {block_number}: First: {name} "
                f"Duration first: {self.results[block_number][name] - self.notifications[block_number]:.5f} "
            )


class AbstractCallFeed:

    def __init__(self, call_params,  uri: str, name: str, counter: ResultCounter) -> None:
        self.call_params = call_params
        self.uri = uri
        self.name = name
        self.ws: Optional[AbstractWsProvider] = None
        self.subscription_id: Optional[str] = None
        self.counter: ResultCounter = counter

    async def __aenter__(self):
        await self.initialize()
        return self

    @abstractmethod
    async def initialize(self) -> None:
        pass


class CallFeed(AbstractCallFeed):

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def initialize(self) -> None:
        ws = await EthWsSubscriber(self.uri)
        assert ws.running
        self.subscription_id = await ws.subscribe("newHeads")
        logger.debug(f"Initialized subscription {self.name}: {self.subscription_id}")
        self.ws = ws
        asyncio.create_task(self.handle_block_notifications())

    async def close(self) -> None:
        ws = self.ws
        subscription_id = self.subscription_id
        if ws:
            if subscription_id:
                ws.unsubscribe(subscription_id)
            ws.close()

    async def handle_block_notifications(self, ) -> None:
        ws = self.ws
        subscription_id = self.subscription_id
        assert subscription_id is not None
        while ws and ws.running:
            next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
            block_header = next_notification.notification
            block_number = block_header["number"]
            if isinstance(block_number, str):
                block_number = int(block_number, 16)

            self.counter.log_notification(block_number)
            await self.process_block_notification(block_number)

    async def process_block_notification(self, block_number) -> None:
        tasks = []
        for call_params in self.call_params:
            tasks.append(
                asyncio.create_task(self.eth_call(block_number, **call_params))
            )

        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        self.counter.log_result_row(block_number, self.name)

    async def eth_call(self, block_number, **kwargs) -> None:
        ws = self.ws
        name = kwargs.pop("name", None)
        command = kwargs.pop("method", "eth_call")
        address = kwargs.pop("address", "0x0000000000000000000000000000000000000000")
        pos = kwargs.pop("pos", "0x")
        tag = hex(block_number)

        if command == "eth_call":
            request_payload = [kwargs, tag]
        elif command in {"eth_getBalance", "eth_getCode", "eth_getTransactionCount"}:
            request_payload = [address, tag]
        elif command in {"eth_getStorageAt"}:
            request_payload = [address, pos, tag]
        elif command in {"eth_blockNumber"}:
            request_payload = []
        else:
            raise ValueError(f"Invalid EthCommand Option: {command}")

        assert ws is not None
        for i in range(10):
            try:
                _response = await ws.call_rpc(
                    command, request_payload
                )
                logger.debug(f"{self.name} #{block_number}: {_response.to_jsons()}")
                break
            except RpcError as e:
                if e.message == "header not found":
                    logger.debug(f"{e.message} retry {i}")
                else:
                    raise e
        return None


class BlxrCallFeed(CallFeed):
    connection_opts: Dict

    def __init__(
        self,
        call_params,
        uri: str,
        name: str,
        counter: ResultCounter,
    ) -> None:
        super(BlxrCallFeed, self).__init__(call_params, uri, name, counter)
        self.connection_opts = {}

    async def get_ws_connection(self):
        return await WsProvider(
            uri=self.uri,
            **self.connection_opts
        )

    async def initialize(self) -> None:
        ws = await self.get_ws_connection()
        assert ws.running

        sub_id = await ws.subscribe("ethOnBlock", options={
            "call_params": [
                {
                    "method": "eth_call",
                    **call_params
                }
                for call_params in self.call_params
                ]
            }
        )
        self.ws = ws
        self.subscription_id = sub_id
        logger.debug(f"Initialized subscription {self.name}: {self.subscription_id}")
        asyncio.create_task(self.wait_on_sub_id(sub_id))

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        ws = self.ws
        if ws is not None:
            await ws.close()

    async def wait_on_sub_id(self, sub_id: str) -> None:
        ws = self.ws
        while ws and ws.running:
            response = await ws.get_next_subscription_notification_by_id(sub_id)
            await self.handle_response(response)

    async def handle_response(self, response) -> None:
        block_number = response.notification["blockHeight"]
        logger.debug(f"{self.name} #{block_number}: {response.notification['response']}")
        self.counter.log_notification(block_number)
        if response.notification["name"] == "TaskDisabledEvent":
            logger.error(f"Invalid subscription task, task was disabled {response.notification}")
        if response.notification["name"] == "TaskCompletedEvent":
            self.counter.log_result_row(block_number, self.name)


class BlxrCloudCallFeed(BlxrCallFeed):
    def __init__(
        self,
        call_params,
        uri: str,
        name: str,
        counter: ResultCounter,
        ca_url: Optional[str] = None,
        ssl_dir: Optional[str] = None
    ) -> None:
        super(BlxrCloudCallFeed, self).__init__(call_params, uri, name, counter)
        if ca_url:
            self.connection_opts["ca_url"] = ca_url
        if ssl_dir:
            self.connection_opts["ssl_dir"] = ssl_dir

    async def get_ws_connection(self):
        return await CloudWssProvider(
            ws_uri=self.uri,
            **self.connection_opts
        )


def _get_feed_connection(
    eth_call_params, conn_type, conn_uri, conn_name, counter, ca_url=None, ssl_dir=None
) -> AbstractCallFeed:
    if conn_type == "Node":
        conn = CallFeed(eth_call_params, conn_uri, conn_name, counter)
    elif conn_type == "Local":
        conn = BlxrCallFeed(eth_call_params, conn_uri, conn_name, counter)
    elif conn_type == "Cloud":
        conn = BlxrCloudCallFeed(eth_call_params, conn_uri, conn_name, counter, ca_url=ca_url, ssl_dir=ssl_dir)
    else:
        raise ValueError
    return conn


async def log_counter_stats(counter: ResultCounter, summary_interval: int = 0) -> None:
    while summary_interval > 0:
        await asyncio.sleep(summary_interval)
        counter.log_results_summary(final=False)


async def main(opts: argparse.Namespace, counter: ResultCounter) -> None:
    logger.info(f"Started feed comparison. Duration {opts.duration}s.")
    conn_a = _get_feed_connection(
        opts.eth_call_params, "Node", opts.eth, "Eth", counter,
    )
    conn_b = _get_feed_connection(
        opts.eth_call_params,
        opts.gateway_type, opts.gateway, "Blxr", counter,
        ssl_dir=opts.gateway_ssl_dir, ca_url=opts.gateway_ca_cert_url
    )
    if opts.use_eth_node:
        await conn_a.initialize()
    await conn_b.initialize()
    if opts.summary_interval:
        asyncio.create_task(log_counter_stats(counter, summary_interval=opts.summary_interval))

    await asyncio.sleep(opts.duration)
    counter.log_results_summary(final=True)


def get_opts(argv=None) -> argparse.Namespace:
    DEFAULT_ETH_NODE_URI = "ws://127.0.0.1:8546"
    DEFAULT_GATEWAY_NODE_URI = "wss://eth.feed.blxrbdn.com:28333"

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--eth",
                            help=f"Eth node uri. Default: {DEFAULT_ETH_NODE_URI}",
                            type=str,
                            default=DEFAULT_ETH_NODE_URI)
    arg_parser.add_argument("--use-eth-node",
                            help=f"Default: True, set to false to test only Gateway performance",
                            type=strtobool,
                            default=True)
    arg_parser.add_argument("--gateway",
                            help=f"Gateway node uri. Default: {DEFAULT_GATEWAY_NODE_URI}",
                            type=str,
                            default=DEFAULT_GATEWAY_NODE_URI)
    arg_parser.add_argument("--use-cloud-api",
                            help="Default True(CloudApi) False for local Gateway node",
                            type=strtobool,
                            default=True)
    arg_parser.add_argument("--gateway-ssl-dir",
                            help="for cloud connection only path to ssl certificate",
                            type=str,
                            )
    arg_parser.add_argument("--gateway-ca_cert_url",
                            help="for bdn cloud only url for ca cert",
                            default="https://certificates.blxrbdn.com/ca/ca_cert.pem",
                            type=str,
                            )
    arg_parser.add_argument("--duration",
                            help="test duration in seconds",
                            type=int,
                            default=600
                            )
    arg_parser.add_argument("--summary-interval",
                            help="display summary every N seconds",
                            type=int,
                            default=0
                            ),
    arg_parser.add_argument("-v", "--verbose",
                            help="display per block results",
                            action="store_true",
                            default=False
                            ),
    arg_parser.add_argument("-d", "--debug",
                            help="log debug information",
                            action="store_true",
                            default=False
                            ),

    arg_parser.add_argument("--call-params-file",
                            help="Json File that contains the ETH call settings, please see example file",
                            type=str,
                            default="on_block_feed_call_params_example.json"
                            )

    opts = arg_parser.parse_args(argv)
    with open(opts.call_params_file, "rb+") as f:
        opts.eth_call_params = json.loads(f.read())
    if opts.debug:
        log_level = logging.DEBUG
        logger.setLevel(logging.DEBUG)
    else:
        log_level = logging.INFO
    if opts.verbose:
        logger_block_result.setLevel(log_level)

    if opts.use_cloud_api:
        opts.gateway_type = "Cloud"
    else:
        opts.gateway_type = "Local"
    return opts


if __name__ == "__main__":
    setup_logger()
    opts = get_opts()
    result_counter = ResultCounter(
        duration=opts.duration
    )
    try:
        asyncio.run(
            main(opts, result_counter)
        )
    except KeyboardInterrupt:
        logger.error("Exit requested. Shutting down")
        result_counter.log_results_summary()
