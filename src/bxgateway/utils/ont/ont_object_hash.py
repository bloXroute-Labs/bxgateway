from bxcommon.utils import convert
from bxgateway.ont_constants import ONT_HASH_LEN
from bxgateway.utils.btc.btc_object_hash import BtcObjectHash


class OntObjectHash(BtcObjectHash):
    def __repr__(self):
        return "OntObjectHash<binary: {}>".format(convert.bytes_to_hex(self.binary))


NULL_ONT_BLOCK_HASH = OntObjectHash(binary=bytearray(ONT_HASH_LEN))
