import asyncio
from typing import Dict

from bxcommon.rpc.json_rpc_response import JsonRpcResponse


class ResponseQueue:
    """
    Queue for matching RPC requests with responses.
    Usage of this class expects all RPC requests to contain request IDs.

    This class doesn't clean up its memory usage, so if the corresponding RPC
    server doesn't respond to request IDs this queue will continue growing.
    """
    def __init__(self, only_once: bool = True):
        self.only_once = only_once
        self.message_notifiers: Dict[str, asyncio.Event] = {}
        self.message_by_request_id: Dict[str, JsonRpcResponse] = {}

    async def put(self, message: JsonRpcResponse) -> None:
        request_id = message.id
        if request_id is None:
            raise ValueError(
                "Response queue cannot accept RPC messages with no request ID."
            )
        self.message_by_request_id[request_id] = message

        if request_id in self.message_notifiers:
            self.message_notifiers[request_id].set()

    async def get_by_request_id(self, request_id: str) -> JsonRpcResponse:
        if request_id in self.message_by_request_id:
            message = self.message_by_request_id[request_id]
            if self.only_once:
                self.cleanup(request_id)
            return message

        if request_id in self.message_notifiers:
            event = self.message_notifiers[request_id]
        else:
            event = asyncio.Event()
            self.message_notifiers[request_id] = event

        await event.wait()

        assert request_id in self.message_by_request_id
        message = self.message_by_request_id[request_id]

        if self.only_once:
            self.cleanup(request_id)

        return message

    def cleanup(self, request_id: str) -> None:
        self.message_notifiers.pop(request_id, None)
        self.message_by_request_id.pop(request_id, None)
