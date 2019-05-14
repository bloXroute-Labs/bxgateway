from bxcommon.utils import crypto
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF, BTC_BLOCK_HDR_SIZE, BTC_SHA_HASH_LEN
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class CompactBlockBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.COMPACT_BLOCK

    def __init__(self, buf=None):

        assert not isinstance(buf, str)
        self.buf = buf
        self._memoryview = memoryview(self.buf)
        self._magic = self._command = self._payload_len = self._checksum = None
        self._payload = None
        self._hash_val = None

    def block_hash(self):
        if self._hash_val is None:
            header = self._memoryview[BTC_HDR_COMMON_OFF:BTC_HDR_COMMON_OFF + BTC_BLOCK_HDR_SIZE]
            raw_hash = crypto.bitcoin_hash(header)
            self._hash_val = BtcObjectHash(buf=raw_hash, length=BTC_SHA_HASH_LEN)
        return self._hash_val
