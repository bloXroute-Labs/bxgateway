from bxcommon.models.serializeable_enum import SerializeableEnum


class RpcRequestType(SerializeableEnum):
    BLXR_TX = "BLXR_TX"
    GATEWAY_STATUS = "GATEWAY_STATUS"
    STOP = "STOP"
    MEMORY = "MEMORY"
    PEERS = "PEERS"
