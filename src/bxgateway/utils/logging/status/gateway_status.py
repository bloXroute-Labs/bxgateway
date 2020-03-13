from bxcommon.models.serializeable_enum import SerializeableEnum


class GatewayStatus(SerializeableEnum):
    OFFLINE = "Offline"
    WITH_ERRORS = "Online with Errors"
    ONLINE = "Online"
