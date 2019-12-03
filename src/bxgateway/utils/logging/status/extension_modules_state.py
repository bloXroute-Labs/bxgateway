from bxcommon.models.serializeable_enum import SerializeableEnum


class ExtensionModulesState(SerializeableEnum):
    UNAVAILABLE = "Extensions Unavailable"
    INVALID_VERSION = "Invalid Version"
    OK = "OK"
