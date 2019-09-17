from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.messages.abstract_message import AbstractMessage
from bxcommon.services.broadcast_service import BroadcastService


class GatewayBroadcastService(BroadcastService[AbstractMessage, AbstractConnection]):
    def should_broadcast_to_connection(self, message: AbstractMessage, connection: AbstractConnection) -> bool:
        # gateway does not really care about network numbers
        return True
