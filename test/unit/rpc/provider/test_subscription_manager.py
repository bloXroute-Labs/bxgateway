import asyncio
from typing import List, Optional

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.helpers import async_test
from bxgateway.rpc.provider.abstract_provider import SubscriptionNotification
from bxgateway.rpc.provider.subscription_manager import SubscriptionManager


async def consume_notifications(
    subscription_manager: SubscriptionManager,
    subscription_id: Optional[str] = None,
) -> List[SubscriptionNotification]:
    if subscription_id is None:
        fn = lambda: subscription_manager.get_next_subscription_notification()
    else:
        fn = lambda: subscription_manager.get_next_subscription_notification_for_id(
            subscription_id
        )

    notifications = []
    while True:
        try:
            notification = await asyncio.wait_for(fn(), 0.01)
            notifications.append(notification)
        except asyncio.TimeoutError:
            break
    return notifications


class SubscriptionManagerTest(AbstractTestCase):
    @async_test
    async def test_queue_limits(self):
        subscription_id = "123"
        subscription_id_2 = "234"
        subscription_manager = SubscriptionManager(10)
        subscription_manager.register_subscription(subscription_id)
        subscription_manager.register_subscription(subscription_id_2)

        for i in range(100):
            await subscription_manager.receive_message(
                SubscriptionNotification(subscription_id, i)
            )

        for i in range(100, 200):
            await subscription_manager.receive_message(
                SubscriptionNotification(subscription_id_2, i)
            )

        all_notifications = await consume_notifications(subscription_manager)
        self.assertEqual(10, len(all_notifications))

        expected_content = iter(range(190, 200))
        for notification in all_notifications:
            self.assertEqual(subscription_id_2, notification.subscription_id)
            self.assertEqual(next(expected_content), notification.notification)

        notifications_1 = await consume_notifications(
            subscription_manager, subscription_id
        )
        self.assertEqual(10, len(notifications_1))

        expected_content = iter(range(90, 100))
        for notification in notifications_1:
            self.assertEqual(subscription_id, notification.subscription_id)
            self.assertEqual(next(expected_content), notification.notification)

        notifications_2 = await consume_notifications(
            subscription_manager, subscription_id_2
        )
        self.assertEqual(10, len(notifications_2))

        expected_content = iter(range(190, 200))
        for notification in notifications_2:
            self.assertEqual(subscription_id_2, notification.subscription_id)
            self.assertEqual(next(expected_content), notification.notification)
