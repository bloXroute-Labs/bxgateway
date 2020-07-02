from typing import Dict, Any, Optional

import rlp

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth import eth_common_util
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.utils.eth import crypto_utils
from bxutils import utils


# pyre-fixme[13]: Attribute `data` is never initialized.
# pyre-fixme[13]: Attribute `gas_price` is never initialized.
# pyre-fixme[13]: Attribute `nonce` is never initialized.
# pyre-fixme[13]: Attribute `start_gas` is never initialized.
# pyre-fixme[13]: Attribute `to` is never initialized.
# pyre-fixme[13]: Attribute `value` is never initialized.
class UnsignedTransaction(rlp.Serializable):
    fields = [
        ("nonce", rlp.sedes.big_endian_int),
        ("gas_price", rlp.sedes.big_endian_int),
        ("start_gas", rlp.sedes.big_endian_int),
        ("to", rlp.sedes.Binary.fixed_length(eth_constants.ADDRESS_LEN, allow_empty=True)),
        ("value", rlp.sedes.big_endian_int),
        ("data", rlp.sedes.binary),
    ]

    nonce: int
    gas_price: int
    start_gas: int
    to: Optional[bytearray]
    value: int
    data: bytearray
