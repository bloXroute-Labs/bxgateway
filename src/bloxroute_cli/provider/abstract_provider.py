from abc import ABCMeta, abstractmethod
from typing import Optional, List, NamedTuple, Any, Union, Dict, Callable, Tuple

from bxcommon.rpc.json_rpc_response import JsonRpcResponse
from bxcommon.rpc.rpc_errors import RpcError
from bxcommon.rpc.rpc_request_type import RpcRequestType


class SubscriptionNotification(NamedTuple):
    subscription_id: str
    notification: Any


class AbstractProvider(metaclass=ABCMeta):
    """
    Provider interface for code that wants to hook into subscription feed available
    in the websockets interface of bxgateway. An IPC interface should also be
    available soon.

    To make use of this functionality, bxgateway must be started with `--ws True`.
    """

    async def __aenter__(self) -> 'AbstractProvider':
        await self.initialize()
        return self

    def __await__(self):
        yield from self.initialize().__await__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @abstractmethod
    async def initialize(self) -> None:
        pass

    @abstractmethod
    async def call(
        self,
        method: RpcRequestType,
        params: Union[List[Any], Dict[Any, Any], None]
    ) -> JsonRpcResponse:
        pass

    @abstractmethod
    async def subscribe(self, channel: str, fields: Optional[List[str]] = None) -> str:
        pass

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> Tuple[bool, Optional[RpcError]]:
        pass

    @abstractmethod
    def subscribe_with_callback(
        self,
        callback: Callable[[SubscriptionNotification], None],
        channel: str,
        fields: Optional[List[str]] = None
    ) -> None:
        pass

    @abstractmethod
    async def get_next_subscription_notification(self) -> SubscriptionNotification:
        pass

    @abstractmethod
    async def get_next_subscription_notification_by_id(
        self, subscription_id: str
    ) -> SubscriptionNotification:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass