from asyncio import QueueFull
from typing import TypeVar, Generic, List, Dict, Optional

from bxgateway import log_messages
from bxgateway.feed.subscriber import Subscriber
from bxutils import logging

logger = logging.get_logger(__name__)
T = TypeVar("T")


class Feed(Generic[T]):
    FIELDS: List[str] = []

    name: str
    subscribers: Dict[str, Subscriber[T]]
    _active: bool

    def __init__(self, name: str) -> None:
        self.name = name
        self.subscribers = {}
        self._active = True

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
                    log_messages.BAD_FEED_SUBSCRIBER, subscriber.subscription_id, self.name
                )
                bad_subscribers.append(subscriber)

        for bad_subscriber in bad_subscribers:
            self.unsubscribe(bad_subscriber.subscription_id)

    def subscriber_count(self) -> int:
        return len(self.subscribers)

    @property
    def active(self) -> bool:
        return self._active

    def keep_alive(self) -> None:
        pass
