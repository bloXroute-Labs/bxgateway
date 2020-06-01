from asyncio import QueueFull
from typing import TypeVar, Generic, List, Dict, Optional

from bxgateway.feed.subscriber import Subscriber
from bxutils import logging

logger = logging.get_logger(__name__)
T = TypeVar("T")


class Feed(Generic[T]):
    FIELDS: List[str] = []

    name: str
    subscribers: Dict[str, Subscriber[T]]

    def __init__(self, name: str) -> None:
        self.name = name
        self.subscribers = {}

    def subscribe(
        self, include_fields: Optional[List[str]] = None
    ) -> Subscriber[T]:
        subscriber: Subscriber[T] = Subscriber(include_fields)
        self.subscribers[subscriber.subscription_id] = subscriber
        return subscriber

    def unsubscribe(self, subscriber_id: str) -> Optional[Subscriber[T]]:
        return self.subscribers.pop(subscriber_id, None)

    def publish(self, message: T) -> None:
        bad_subscribers = []
        for subscriber in self.subscribers.values():
            try:
                subscriber.queue(message)
            except QueueFull:
                logger.error(
                    "Subscriber {} was not receiving messages and emptying its queue from "
                    "feed {}. Disconnecting.",
                    subscriber.subscription_id,
                    self.name
                )
                bad_subscribers.append(subscriber)

        for bad_subscriber in bad_subscribers:
            self.unsubscribe(bad_subscriber.subscription_id)

    def subscriber_count(self) -> int:
        return len(self.subscribers)
