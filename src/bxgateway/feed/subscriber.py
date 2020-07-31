import asyncio
import uuid
from typing import Generic, TypeVar, Union, Dict, Any

from bxgateway import gateway_constants

T = TypeVar("T")


class Subscriber(Generic[T]):
    """
    Subscriber object for asynchronous listening to a feed, with functionality
    to filter received messages on certain fields.

    Any object can published on a feed (and received by this subscriber), though
    users should be careful about ensuring that the object is serializable if
    the object should be received over the websocket or IPC subscriptions.

    If `options` is None, the message will be passed onward with no changes.

    Make sure to update documentation page with any input format change
    """

    subscription_id: str
    messages: 'asyncio.Queue[Union[T, Dict[str, Any]]]'
    options: Dict[str, Any]

    def __init__(self, options: Dict[str, Any]) -> None:
        self.options = options
        self.subscription_id = str(uuid.uuid4())
        self.messages = asyncio.Queue(
            gateway_constants.RPC_SUBSCRIBER_MAX_QUEUE_SIZE
        )

    async def receive(self) -> Union[T, Dict[str, Any]]:
        """
        Receives the next message in the queue.

        This function will block until a new message is posted to the queue.
        """
        message = await self.messages.get()
        return message

    def queue(self, message: T) -> None:
        """
        Queues up a message, releasing all receiving listeners.

        If too many messages are queued without a listener, this task
        will eventually fail and must be handled.
        """
        include_fields = self.options.get("include", None)
        if include_fields is not None:
            if isinstance(message, dict):
                filtered_message = {
                    key: message[key] for key in include_fields
                }
            else:
                filtered_message = {
                    key: getattr(message, key) for key in include_fields
                }
            self.messages.put_nowait(filtered_message)
        else:
            self.messages.put_nowait(message)
