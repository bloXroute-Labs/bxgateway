from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.feed.feed import Feed


class TestFeed(Feed[int, int]):
    def __init__(self):
        super().__init__("testfeed")
        self.expensive_serialization_count = 0

    def serialize(self, raw_message: int) -> int:
        self.expensive_serialization_count += 1
        return raw_message


class FeedTest(AbstractTestCase):

    @async_test
    async def test_serialization_avoided_if_no_subscribers(self):
        test_feed = TestFeed()

        test_feed.publish(1)
        self.assertEqual(0, test_feed.expensive_serialization_count)

        subscriber = test_feed.subscribe()
        test_feed.publish(2)
        self.assertEqual(1, test_feed.expensive_serialization_count)
        self.assertEqual(2, await subscriber.receive())
