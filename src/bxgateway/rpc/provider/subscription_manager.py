from typing import Dict

from bxgateway.rpc.provider.abstract_provider import SubscriptionNotification
from bxgateway.utils.async_live_queue import AsyncLiveQueue


class SubscriptionManager:
    queue_limit: int
    all_subscription_notifications: AsyncLiveQueue[SubscriptionNotification]
    subscription_notifications_by_id: Dict[str, AsyncLiveQueue[SubscriptionNotification]]

    def __init__(self, queue_limit: int):
        self.queue_limit = queue_limit
        self.all_subscription_notifications = AsyncLiveQueue(self.queue_limit)
        self.subscription_notifications_by_id = {}

    def register_subscription(self, subscription_id: str) -> None:
        self.subscription_notifications_by_id[subscription_id] = AsyncLiveQueue(self.queue_limit)

    def unregister_subscription(self, subscription_id: str) -> None:
        del self.subscription_notifications_by_id[subscription_id]

    async def receive_message(self, subscription_notification: SubscriptionNotification) -> None:
        """
        Due to asynchronous scheduling it's possible for a message to be received
        before the subscription is registered. These messages will be dropped.
        """
        await self.all_subscription_notifications.put(subscription_notification)
        if subscription_notification.subscription_id in self.subscription_notifications_by_id:
            await self.subscription_notifications_by_id[
                subscription_notification.subscription_id
            ].put(subscription_notification)

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
