class GatewayMessageType(object):
    HELLO = "gw_hello"
    BLOCK_RECEIVED = "blockrecv"
    BLOCK_PROPAGATION_REQUEST = "blockprop"
    BLOCK_HOLDING = "blockhold"

    # Sync messages types are currently unused. See `blockchain_sync_service.py`.
    SYNC_REQUEST = "syncreq"
    SYNC_RESPONSE = "syncres"
