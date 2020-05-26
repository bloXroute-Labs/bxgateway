from typing import TypeVar, Generic, List, Dict, Optional

from bxgateway.feed.subscriber import Subscriber

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
        for subscriber in self.subscribers.values():
            subscriber.queue(message)

    def subscriber_count(self) -> int:
        return len(self.subscribers)
