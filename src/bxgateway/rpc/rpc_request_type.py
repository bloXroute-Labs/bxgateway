from bxcommon.models.serializable_flag import SerializableFlag


class RpcRequestType(SerializableFlag):
    BLXR_TX = 1
    GATEWAY_STATUS = 2
