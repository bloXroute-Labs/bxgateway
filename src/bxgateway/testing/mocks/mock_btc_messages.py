from bxcommon.constants import LISTEN_ON_IP_ADDRESS
from bxcommon.utils import crypto
from bxcommon.utils.crypto import SHA256_HASH_LEN
from bxgateway.btc_constants import BTC_HDR_COMMON_OFF
from bxgateway.messages.btc.block_btc_message import BlockBtcMessage
from bxgateway.messages.btc.tx_btc_message import TxBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


def btc_block():
    magic = 12345
    version = 23456
    prev_block_hash = bytearray(crypto.double_sha256("123"))
    prev_block = BtcObjectHash(prev_block_hash, length=SHA256_HASH_LEN)
    merkle_root_hash = bytearray(crypto.double_sha256("234"))
    merkle_root = BtcObjectHash(merkle_root_hash, length=SHA256_HASH_LEN)
    timestamp = 1
    bits = 2
    nonce = 3

    txns = [TxBtcMessage(magic, version, [], [], i).rawbytes()[BTC_HDR_COMMON_OFF:] for i in xrange(10)]

    return BlockBtcMessage(magic, version, prev_block, merkle_root, timestamp, bits, nonce, txns)

def btc_version_message():
    return VersionBtcMessage(12345, 12345, LISTEN_ON_IP_ADDRESS, 1000, LISTEN_ON_IP_ADDRESS,
                             1000, 1, 2, "bloxroute")
