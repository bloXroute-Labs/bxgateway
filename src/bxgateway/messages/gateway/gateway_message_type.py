class GatewayMessageType(object):
    HELLO = "gw_hello"

    # Sync messages types are currently unused. See `blockchain_sync_service.py`.
    SYNC_REQUEST = "syncreq"
    SYNC_RESPONSE = "syncres"
