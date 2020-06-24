from typing import Dict, Optional, List, Any

from bxgateway.feed.feed import Feed
from bxgateway.feed.subscriber import Subscriber
from bxutils import logging

logger = logging.get_logger(__name__)


class FeedManager:
    feeds: Dict[str, Feed]

    def __init__(self) -> None:
        self.feeds = {}

    def __contains__(self, item):
        return item in self.feeds

    def register_feed(self, feed: Feed) -> None:
        self.feeds[feed.name] = feed

    def subscribe_to_feed(
        self, name: str, include_fields: Optional[List[str]] = None
    ) -> Optional[Subscriber]:
        if name in self.feeds:
            subscriber = self.feeds[name].subscribe(include_fields)
            logger.debug(
                "Creating new subscriber ({}) to {}",
                subscriber.subscription_id,
                name
            )
            return subscriber
        else:
            return None

    def unsubscribe_from_feed(
        self, name: str, subscriber_id: str
    ) -> Optional[Subscriber]:
        subscriber = self.feeds[name].unsubscribe(subscriber_id)
        if subscriber is not None:
            logger.debug(
                "Unsubscribing subscriber ({}) from {}",
                subscriber.subscription_id,
                name
            )
        return subscriber

    def publish_to_feed(self, name: str, message: Any) -> None:
        if name in self.feeds:
            self.feeds[name].publish(message)

    def get_feed_fields(self, feed_name: str) -> List[str]:
        return self.feeds[feed_name].FIELDS

    def any_subscribers(self) -> bool:
        return any(feed.subscriber_count() > 0 for feed in self.feeds.values())

    def keep_feed_alive(self, name: str) -> None:
        if name in self.feeds:
            self.feeds[name].keep_alive()
