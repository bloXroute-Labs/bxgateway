from typing import Dict, Optional, List, Any, Union, Set, TYPE_CHECKING

from bxgateway.feed.feed import Feed
from bxgateway.feed.subscriber import Subscriber
from bxutils import logging

if TYPE_CHECKING:
    from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode

logger = logging.get_logger(__name__)


class FeedManager:
    feeds: Dict[str, Feed]
    _node: "AbstractGatewayNode"

    def __init__(self, node: "AbstractGatewayNode") -> None:
        self.feeds = {}
        self._node = node

    def __contains__(self, item):
        return item in self.feeds

    def register_feed(self, feed: Feed) -> None:
        if feed.name in self.feeds:
            raise ValueError(
                f"Cannot register two feeds with the same name: {feed.name}"
            )

        self.feeds[feed.name] = feed

    def subscribe_to_feed(
        self, name: str, options: Dict[str, Any]
    ) -> Optional[Subscriber]:
        if name in self.feeds:
            subscriber = self.feeds[name].subscribe(options)
            logger.debug(
                "Creating new subscriber ({}) to {}", subscriber.subscription_id, name
            )
            self._node.reevaluate_transaction_streamer_connection()
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
                name,
            )
        self._node.reevaluate_transaction_streamer_connection()
        return subscriber

    def publish_to_feed(self, name: str, message: Any) -> None:
        if name in self.feeds:
            self.feeds[name].publish(message)

    def get_feed_fields(self, feed_name: str) -> List[str]:
        return self.feeds[feed_name].FIELDS

    def any_subscribers(self) -> bool:
        return any(feed.subscriber_count() > 0 for feed in self.feeds.values())

    def get_valid_feed_filters(self, feed_name: str) -> Set[str]:
        return self.feeds[feed_name].FILTERS

    def reformat_feed_filters(
        self, feed_name: str, filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        return self.feeds[feed_name].reformat_filters(filters)
