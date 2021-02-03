import asyncio
import time

from typing import Dict, Any, TYPE_CHECKING, Union, List, Optional
from asyncio import QueueFull, Task
from dataclasses import dataclass, asdict

from bxcommon import constants
from bxcommon.feed.feed_source import FeedSource
from bxcommon.models.serializeable_enum import SerializeableEnum
from bxcommon.rpc.rpc_errors import RpcInvalidParams, RpcError
from bxcommon.rpc import rpc_constants
from bxcommon.feed.feed import Feed
from bxcommon.feed.subscriber import Subscriber

from bxgateway import log_messages

from bxgateway.utils.stats.eth_on_block_feed_stats_service import (
    eth_on_block_feed_stats_service,
)

from bxutils import logging

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    # pylint: disable=ungrouped-imports,cyclic-import
    from bxgateway.connections.eth.eth_gateway_node import EthGatewayNode

logger = logging.get_logger(__name__)

SUPPORTED_PAYLOAD_OPTIONS = {"data", "from", "to", "gasPrice", "gas", "address", "pos"}
RETRIES_MAX_ATTEMPTS = 2
RETRIES_SLEEP_INTERVAL = 0.01

TAG_TYPE = Union[str, int]


class EventType(SerializeableEnum):
    TASK_COMPLETED_EVENT = "TaskCompletedEvent"
    TASK_DISABLED_EVENT = "TaskDisabledEvent"


class EthCommandMethod(SerializeableEnum):
    ETH_CALL = "eth_call"  # https://eth.wiki/json-rpc/API#eth_call [{payload}, TAG]
    ETH_GET_BALANCE = "eth_getBalance"  # https://eth.wiki/json-rpc/API#eth_getbalance [address, TAG]
    ETH_GET_TRANSACTION_COUNT = "eth_getTransactionCount"  # https://eth.wiki/json-rpc/API#eth_gettransactioncount
    ETH_GET_STORAGE_AT = "eth_getStorageAt"  # https://eth.wiki/json-rpc/API#eth_getstorageat [address, pos, TAG]
    ETH_GET_CODE = "eth_getCode"  # https://eth.wiki/json-rpc/API#eth_getcode [address, TAG]
    ETH_GET_BLOCK_NUMBER = "eth_blockNumber"  # https://eth.wiki/json-rpc/API#eth_blockNumber []

    @classmethod
    def from_string(cls, value) -> "EthCommandMethod":
        return EthCommandMethod(value)


DEFAULT_METHOD = EthCommandMethod.ETH_CALL


class OnBlockFeedEntry:
    name: str
    response: Dict
    block_height: int
    tag: TAG_TYPE

    def __init__(
        self, name: str, response: Dict, block_height: int, tag: TAG_TYPE
    ) -> None:
        self.name = name
        self.response = response
        self.block_height = block_height
        self.tag = tag


@dataclass()
class EthCallOption:
    command_method: EthCommandMethod
    block_offset: int
    call_name: str
    call_payload: Dict[str, Any]
    active: bool
    
    def validate_item_in_payload(self, item) -> None:
        if item not in self.call_payload:
            raise RpcInvalidParams(
                f"Expected {item} element in request payload for {self.command_method}"
            )

    def __post_init__(self) -> None:
        if self.command_method in {EthCommandMethod.ETH_CALL}:
            self.validate_item_in_payload("data")
        if self.command_method in {
            EthCommandMethod.ETH_GET_BALANCE,
            EthCommandMethod.ETH_GET_CODE,
            EthCommandMethod.ETH_GET_TRANSACTION_COUNT,
            EthCommandMethod.ETH_GET_STORAGE_AT,
        }:
            self.validate_item_in_payload("address")
        if self.command_method in {
            EthCommandMethod.ETH_GET_STORAGE_AT,
        }:
            self.validate_item_in_payload("pos")


class EventNotification:
    block_height: int

    def __init__(self, block_height: int,) -> None:
        self.block_height = block_height


def process_call_params_tag(call_tag: Optional[TAG_TYPE]) -> int:
    if call_tag is None or (isinstance(call_tag, str) and call_tag == "latest"):
        block_offset = 0
    elif isinstance(call_tag, int) and call_tag <= 0:
        block_offset = call_tag
    else:
        raise RpcInvalidParams(
            f"Invalid Value for tag provided {call_tag} use latest, 0 or a negative number"
        )
    return block_offset


def process_call_params_method(method: Optional[str]) -> EthCommandMethod:
    if method is None:
        eth_command_method = DEFAULT_METHOD
    else:
        try:
            eth_command_method = EthCommandMethod.from_string(method)
        except ValueError:
            raise RpcInvalidParams(
                f"Invalid Value for method provided {method} use {[str(item) for item in EthCommandMethod]}"
            )
    return eth_command_method


def process_call_params_payload(call: Dict[str, Any]) -> Dict[str, Any]:
    call_payload = {k: v for (k, v) in call.items() if k in SUPPORTED_PAYLOAD_OPTIONS}
    return call_payload


def process_call_params(call_params: List[Dict[str, Any]]) -> Dict[str, Any]:
    if call_params is None or not isinstance(call_params, list):
        raise RpcInvalidParams("call_params must be a list")
    calls = {}
    for counter, call in enumerate(call_params):

        call_name = call.get("name", str(counter))
        if call_name in calls:
            raise RpcInvalidParams("unique name must be provided for each call")

        block_offset = process_call_params_tag(call.get("tag"))
        eth_command_method = process_call_params_method(call.get("method"))
        call_payload = process_call_params_payload(call)

        calls[call_name] = EthCallOption(
            eth_command_method,
            block_offset,
            call_name,
            call_payload,
            active=call.get("active", True),
        )
    return calls


class EthOnBlockFeed(Feed[OnBlockFeedEntry, EventNotification]):
    NAME = rpc_constants.ETH_ON_BLOCK_FEED_NAME
    FIELDS = ["name", "response", "block_height", "tag"]
    ALL_FIELDS = FIELDS
    VALID_SOURCES = {FeedSource.BLOCKCHAIN_RPC}
    last_block_height: int

    def __init__(self, node: "EthGatewayNode", network_num: int = constants.ALL_NETWORK_NUM,) -> None:
        self.node = node
        self.bad_subscribers = set()
        self.last_block_height = 0
        super().__init__(self.NAME, network_num)

    def subscribe(self, options: Dict[str, Any]) -> Subscriber[OnBlockFeedEntry]:
        call_params = options.get("call_params", [])
        calls = process_call_params(call_params)
        subscription_id = options.get("subscription_id")
        if subscription_id:
            subscriber = self.subscribers.get(subscription_id)
            if subscriber is None:
                raise RpcInvalidParams(
                    f"Subscriber id {subscription_id} for feed {self.NAME} not found"
                )

            subscriber.options["calls"].update(calls)
            return subscriber
        else:
            options["calls"] = calls
            return super().subscribe(options)

    def publish(self, raw_message: EventNotification) -> None:
        block_height = raw_message.block_height
        if block_height and block_height <= self.last_block_height:
            # ignore older blocks by height
            logger.debug(
                "Ignore EthOnBlockFeed block notification for old block height: {} last: {}",
                block_height, self.last_block_height
            )
            return
        logger.debug("Processing EthOnBlockFeed notification for block height: {}", block_height)
        self.last_block_height = block_height
        event_init_time = time.time()

        # remove bad subscribers
        while self.bad_subscribers:
            self.unsubscribe(self.bad_subscribers.pop())

        # process subscriptions
        for subscriber in self.subscribers.values():
            subscriber_tasks = []
            for call in subscriber.options["calls"].values():
                if call.active:
                    task = asyncio.create_task(
                        self._publish(subscriber, call, block_height)
                    )
                    subscriber_tasks.append(task)
            asyncio.create_task(
                self.wait_for_all_subscriber_tasks(
                    subscriber, subscriber_tasks, block_height, event_init_time
                )
            )

    async def wait_for_all_subscriber_tasks(
        self,
        subscriber: Subscriber[OnBlockFeedEntry],
        subscriber_tasks: List[Task],
        block_height: int,
        event_init_time: float,
    ) -> None:
        logger.trace(
            "Execution Completed for block height {} duration {} s",
            block_height,
            time.time() - event_init_time,
        )
        eth_on_block_feed_stats_service.log_subscriber_tasks(
            len(subscriber_tasks), time.time() - event_init_time
        )
        if subscriber_tasks:
            await asyncio.wait(subscriber_tasks, return_when=asyncio.ALL_COMPLETED)
        subscriber.queue(
            self.serialize_response(
                str(EventType.TASK_COMPLETED_EVENT), {}, block_height, block_height
            )
        )

    async def _publish(
        self,
        subscriber: Subscriber[OnBlockFeedEntry],
        call: EthCallOption,
        block_height: int,
        retry_count: int = 0
    ) -> None:
        tag = block_height + call.block_offset
        try:
            response = await self.execute_eth_call(call, tag)
        except RpcError as e:
            response = e.to_json()
            if e.message == "header not found" and retry_count <= RETRIES_MAX_ATTEMPTS:
                await asyncio.sleep(RETRIES_SLEEP_INTERVAL)
                await self._publish(subscriber, call, block_height, retry_count + 1)
                retry = True
            else:
                call.active = False
                subscriber.queue(
                    self.serialize_response(
                        str(EventType.TASK_DISABLED_EVENT),
                        asdict(call),
                        block_height,
                        block_height,
                    )
                )
                retry = False
            logger.info(
                "{}, Error response from node {}, call details {} retry: {}",
                self,
                response,
                call,
                retry,
            )

        serialized_message = self.serialize_response(
            call.call_name, response, block_height, tag
        )
        try:
            subscriber.queue(serialized_message)
        except QueueFull:
            logger.error(
                log_messages.GATEWAY_BAD_FEED_SUBSCRIBER, subscriber.subscription_id, self.name
            )
            self.bad_subscribers.add(subscriber.subscription_id)

    def serialize(self, raw_message: EventNotification) -> OnBlockFeedEntry:
        raise NotImplementedError

    def serialize_response(
        self, name: str, response: Dict, block_height: int, tag: TAG_TYPE
    ) -> OnBlockFeedEntry:
        sanitized_response = {
            k: v for (k, v) in response.items() if k not in rpc_constants.ETH_RPC_INTERNAL_RESPONSE_ITEMS
        }
        return OnBlockFeedEntry(name, sanitized_response, block_height, tag)

    async def execute_eth_call(
        self, call_option: EthCallOption, tag: TAG_TYPE
    ) -> Dict:
        if isinstance(tag, int):
            tag = hex(tag)
        payload = call_option.call_payload
        command = call_option.command_method
        if command == EthCommandMethod.ETH_CALL:
            request_payload = [payload, tag]
        elif command in {
            EthCommandMethod.ETH_GET_BALANCE,
            EthCommandMethod.ETH_GET_CODE,
            EthCommandMethod.ETH_GET_TRANSACTION_COUNT,
        }:
            request_payload = [payload["address"], tag]
        elif command in {
            EthCommandMethod.ETH_GET_STORAGE_AT,
        }:
            request_payload = [
                payload["address"],
                payload["pos"],
                tag,
            ]
        elif command in {EthCommandMethod.ETH_GET_BLOCK_NUMBER}:
            request_payload = []
        else:
            raise ValueError(f"Invalid EthCommand Option: {command}")
        response = await self.node.eth_ws_proxy_publisher.call_rpc(
            str(command), request_payload,
        )
        return response.to_json()
