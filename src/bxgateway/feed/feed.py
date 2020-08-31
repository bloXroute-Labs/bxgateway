from abc import abstractmethod, ABCMeta
from asyncio import QueueFull
from typing import TypeVar, Generic, List, Dict, Optional, Any

from bxgateway import log_messages
from bxgateway.feed.subscriber import Subscriber
from bxutils import logging

logger = logging.get_logger(__name__)
T = TypeVar("T")
S = TypeVar("S")


class Feed(Generic[T, S], metaclass=ABCMeta):
    FIELDS: List[str] = []

    name: str
    subscribers: Dict[str, Subscriber[T]]

    def __init__(self, name: str) -> None:
        self.name = name
        self.subscribers = {}

    def __repr__(self) -> str:
        return f"Feed<{self.name}>"

    def subscribe(
        self, options: Dict[str, Any]
    ) -> Subscriber[T]:
        subscriber: Subscriber[T] = Subscriber(options)
        self.subscribers[subscriber.subscription_id] = subscriber
        return subscriber

    def unsubscribe(self, subscriber_id: str) -> Optional[Subscriber[T]]:
        return self.subscribers.pop(subscriber_id, None)

    def publish(self, raw_message: S) -> None:
        if self.subscriber_count() == 0:
            return

        try:
            serialized_message = self.serialize(raw_message)
        except Exception:
            logger.error(log_messages.COULD_NOT_SERIALIZE_FEED_ENTRY, exc_info=True)
            return

        bad_subscribers = []
        for subscriber in self.subscribers.values():
            if not self.should_publish_message_to_subscriber(
                subscriber, raw_message, serialized_message
            ):
                continue

            try:
                subscriber.queue(serialized_message)
            except QueueFull:
                logger.error(
                    log_messages.BAD_FEED_SUBSCRIBER, subscriber.subscription_id, self
                )
                bad_subscribers.append(subscriber)

        for bad_subscriber in bad_subscribers:
            self.unsubscribe(bad_subscriber.subscription_id)

    @abstractmethod
    def serialize(self, raw_message: S) -> T:
        """
        Operations to serialize raw data for publication.

        Any potential CPU expensive operations should be moved here, to optimize
        serialization time.
        """
        pass

    def subscriber_count(self) -> int:
        return len(self.subscribers)

    def should_publish_message_to_subscriber(
        self, subscriber: Subscriber[T], raw_message: S, serialized_message: T
    ) -> bool:
        return True
