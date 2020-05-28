from typing import Dict, Optional, List, Any

from bxgateway.feed.feed import Feed
from bxgateway.feed.subscriber import Subscriber


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
            return self.feeds[name].subscribe(include_fields)
        else:
            return None

    def unsubscribe_from_feed(
        self, name: str, subscriber_id: str
    ) -> Optional[Subscriber]:
        return self.feeds[name].unsubscribe(subscriber_id)

    def publish_to_feed(self, name: str, message: Any) -> None:
        self.feeds[name].publish(message)

    def get_feed_fields(self, feed_name: str) -> List[str]:
        return self.feeds[feed_name].FIELDS