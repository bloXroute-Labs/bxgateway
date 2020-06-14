import asyncio
from typing import Dict

from bxgateway.rpc.provider.abstract_provider import SubscriptionNotification


class SubscriptionManager:
    all_subscription_notifications: "asyncio.Queue[SubscriptionNotification]"
    subscription_notifications_by_id: Dict[str, "asyncio.Queue[SubscriptionNotification]"]

    def __init__(self):
        self.all_subscription_notifications = asyncio.Queue()
        self.subscription_notifications_by_id = {}

    def register_subscription(self, subscription_id: str) -> None:
        self.subscription_notifications_by_id[subscription_id] = asyncio.Queue()

    def unregister_subscription(self, subscription_id: str) -> None:
        del self.subscription_notifications_by_id[subscription_id]

    async def receive_message(self, subscription_notification: SubscriptionNotification) -> None:
        await self.all_subscription_notifications.put(subscription_notification)
        await self.subscription_notifications_by_id[subscription_notification.subscription_id].put(
            subscription_notification
        )

    async def get_next_subscription_notification(self) -> SubscriptionNotification:
        """
        NOTE: this does not consume notifications from subscription id specific queues.
        :return:
        """
        return await self.all_subscription_notifications.get()

    async def get_next_subscription_notification_for_id(
        self, subscription_id: str
    ) -> SubscriptionNotification:
        return await self.subscription_notifications_by_id[subscription_id].get()
