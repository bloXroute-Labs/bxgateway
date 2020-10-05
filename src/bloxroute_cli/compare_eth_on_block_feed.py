import asyncio
import json
import logging
import time
import operator
import argparse
import sys

from abc import abstractmethod
from collections import defaultdict, Counter
from typing import Union, Optional, Dict

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.rpc.provider.abstract_ws_provider import AbstractWsProvider
from bxcommon.rpc.external.eth_ws_subscriber import EthWsSubscriber
from bloxroute_cli.provider.cloud_wss_provider import CloudWssProvider
from bxcommon.rpc.rpc_errors import RpcError

logger = logging.getLogger(__name__)


def setup_logger() -> None:
    root_logger = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    root_logger.addHandler(handler)
    logging.getLogger("bxgateway").setLevel(logging.CRITICAL)
    logging.getLogger(__name__).setLevel(logging.INFO)


class ResultCounter:
    def __init__(self) -> None:
        self.notifications = dict()
        self.results = defaultdict(dict)
        self.summary = Counter()

    def log_results_summary(self) -> None:
        logger.info("Results: {}", " ".join([f"{k}:{v}" for k, v in self.summary.items()]))

    def log_notification(self, block_number: Union[str, int]) -> None:
        if block_number not in self.notifications:
            self.notifications[block_number] = time.time()

    def log_result_row(self, block_number: Union[str, int], name: str) -> None:
        if name in self.results[block_number]:
            logger.debug("result previously logged ignore {} {}", block_number, name)
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
            logger.info(
                f"{block_number}: First: {winner} "
                f"Duration first: {duration_winner:.5f} "
                f"Duration second: {duration_loser:.5f} "
                f"delta {delta:.5f} "
            )
            self.summary[winner] += 1


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
        logger.debug(f"init subscription {self.name}: {self.subscription_id}")
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
        assert ws is not None
        for i in range(10):
            try:
                _response = await ws.call_rpc(
                    "eth_call", [kwargs, hex(block_number)]
                )
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
        self.counter.log_notification(block_number)
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
        counter.log_results_summary()
    else:
        counter.log_results_summary()


async def main(opts: argparse.Namespace, counter: ResultCounter) -> None:
    conn_a = _get_feed_connection(
        opts.eth_call_params, "Node", opts.eth, "Eth", counter,
    )
    conn_b = _get_feed_connection(
        opts.eth_call_params,
        opts.gateway_type, opts.gateway, "Blxr", counter,
        ssl_dir=opts.gateway_ssl_dir, ca_url=opts.gateway_ca_cert_url
    )
    await conn_a.initialize()
    await conn_b.initialize()
    asyncio.create_task(log_counter_stats(counter, summary_interval=opts.summary_interval))

    await asyncio.sleep(opts.interval)
    await log_counter_stats(counter)


def get_opts(argv=None) -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--eth",
                            help="example ws://127.0.0.1:8546",
                            type=str,
                            default="ws://127.0.0.1:8546")
    arg_parser.add_argument("--gateway",
                            help="example ws://127.0.0.1:28333",
                            type=str,
                            default="wss://eth.feed.blxrbdn.com:28333")
    arg_parser.add_argument("--gateway-type",
                            help="options Local/BdnCloud",
                            choices=["Local", "Cloud"],
                            type=str,
                            default="Cloud")
    arg_parser.add_argument("--gateway-ssl-dir",
                            help="for cloud connection only path to ssl certificate",
                            type=str,
                            )
    arg_parser.add_argument("--gateway-ca_cert_url",
                            help="for bdn cloud only url for ca cert",
                            type=str,
                            )
    arg_parser.add_argument("--interval",
                            help="test duration in seconds",
                            type=int,
                            default=600
                            )
    arg_parser.add_argument("--summary-interval",
                            help="display summary every N seconds",
                            type=int,
                            default=60
                            ),
    arg_parser.add_argument("--call-params-file",
                            help="Json File that contains the ETH call settings, please see example file",
                            type=str,
                            default="on_block_feed_call_params_example.json"
                            )

    opts = arg_parser.parse_args(argv)
    with open(opts.call_params_file, "rb+") as f:
        opts.eth_call_params = json.loads(f.read())
    return opts


if __name__ == "__main__":
    setup_logger()
    result_counter = ResultCounter()
    try:
        asyncio.get_event_loop().run_until_complete(
            main(get_opts(), result_counter)
        )
    except KeyboardInterrupt:
        logger.error("Exit requested. Shutting down")
        result_counter.log_results_summary()
