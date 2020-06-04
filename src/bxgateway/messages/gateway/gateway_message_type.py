class GatewayMessageType:
    HELLO = b"gw_hello"
    BLOCK_RECEIVED = b"blockrecv"
    BLOCK_PROPAGATION_REQUEST = b"blockprop"

    # Sync messages types are currently unused. See `blockchain_sync_service.py`.
    SYNC_REQUEST = b"syncreq"
    SYNC_RESPONSE = b"syncres"

    REQUEST_TX_STREAM = b"rqtx"
    CONFIRMED_TX = b"cnfrmtx"
