from bxcommon.messages.versioning.abstract_version_manager import AbstractVersionManager
from bxgateway.messages.gateway.gateway_message_factory import gateway_message_factory
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class _GatewayVersionManager(AbstractVersionManager):
    CURRENT_PROTOCOL_VERSION = 1
    _PROTOCOL_TO_CONVERTER_FACTORY_MAPPING = {
    }
    _PROTOCOL_TO_FACTORY_MAPPING = {
        1: gateway_message_factory,
    }

    def __init__(self):
        super(_GatewayVersionManager, self).__init__()
        self.protocol_to_factory_mapping = self._PROTOCOL_TO_FACTORY_MAPPING
        self.protocol_to_converter_factory_mapping = self._PROTOCOL_TO_CONVERTER_FACTORY_MAPPING
        self.version_message_command = GatewayMessageType.HELLO

gateway_version_manager = _GatewayVersionManager()
