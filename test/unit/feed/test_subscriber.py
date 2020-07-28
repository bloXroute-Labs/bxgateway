import asyncio

from dataclasses import dataclass
from typing import Dict

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.feed.subscriber import Subscriber


class SubscriberTest(AbstractTestCase):
    def setUp(self) -> None:
        self.subscriber: Subscriber[int] = Subscriber({})

    @async_test
    async def test_receive_message(self):
        asyncio.get_event_loop().call_later(
            0.1, lambda: self.subscriber.queue(2)
        )
        message = await self.subscriber.receive()
        self.assertEqual(2, message)

    @async_test
    async def test_receive_message_multiple(self):
        def mass_publish():
            self.subscriber.queue(1)
            self.subscriber.queue(2)
            self.subscriber.queue(3)

        asyncio.get_event_loop().call_later(
            0.1, mass_publish
        )
        self.assertEqual(1, await self.subscriber.receive())
        self.assertEqual(2, await self.subscriber.receive())
        self.assertEqual(3, await self.subscriber.receive())

    @async_test
    async def test_receive_message_multiple_messages(self):
        task = asyncio.ensure_future(self.subscriber.receive())
        await asyncio.sleep(0)
        self.assertFalse(task.done())

        self.subscriber.queue(2)
        await asyncio.sleep(0)
        self.assertTrue(task.done())
        self.assertIsNone(task.exception())
        self.assertEqual(2, task.result())

        task2 = asyncio.ensure_future(self.subscriber.receive())
        await asyncio.sleep(0)
        self.assertFalse(task2.done())

        self.subscriber.queue(3)
        await asyncio.sleep(0)
        self.assertTrue(task2.done())
        self.assertIsNone(task2.exception())
        self.assertEqual(3, task2.result())

    @async_test
    async def test_field_filtering_class(self):
        @dataclass
        class Meal:
            food: str
            drink: str
            dessert: str

        subscriber: Subscriber[Meal] = Subscriber({"include": ["drink", "dessert"]})
        subscriber.queue(Meal("steak", "wine", "chocolate cake"))
        subscriber.queue(Meal("lobster", "lemonade", "cookie"))

        first_meal = await subscriber.receive()
        self.assertIsInstance(first_meal, dict)
        self.assertEqual("wine", first_meal["drink"])
        self.assertEqual("chocolate cake", first_meal["dessert"])
        self.assertNotIn("food", first_meal)

        second_meal = await subscriber.receive()
        self.assertIsInstance(second_meal, dict)
        self.assertEqual("lemonade", second_meal["drink"])
        self.assertEqual("cookie", second_meal["dessert"])
        self.assertNotIn("food", second_meal)

    @async_test
    async def test_field_filtering_dict(self):
        subscriber: Subscriber[Dict[str, str]] = Subscriber({"include": ["food", "drink"]})
        subscriber.queue({
            "food": "steak",
            "drink": "wine",
            "dessert": "chocolate cake",
        })
        subscriber.queue({
            "food": "lobster",
            "drink": "lemonade",
            "dessert": "cookie",
        })

        first_meal = await subscriber.receive()
        self.assertIsInstance(first_meal, dict)
        self.assertEqual("steak", first_meal["food"])
        self.assertEqual("wine", first_meal["drink"])
        self.assertNotIn("dessert", first_meal)

        second_meal = await subscriber.receive()
        self.assertIsInstance(second_meal, dict)
        self.assertEqual("lobster", second_meal["food"])
        self.assertEqual("lemonade", second_meal["drink"])
        self.assertNotIn("dessert", second_meal)
