from bxcommon import constants
from bxcommon.messages.bloxroute.message import Message
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import ObjectHash
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class BlockReceivedMessage(Message):
    MESSAGE_TYPE = GatewayMessageType.BLOCK_RECEIVED
    PAYLOAD_LENGTH = crypto.SHA256_HASH_LEN

    def __init__(self, block_hash=None, buf=None):
        if buf is None:
            buf = bytearray(constants.HDR_COMMON_OFF + self.PAYLOAD_LENGTH)

            off = constants.HDR_COMMON_OFF
            buf[off:off + crypto.SHA256_HASH_LEN] = block_hash.binary
            off += crypto.SHA256_HASH_LEN

        self.buf = buf
        self._block_hash = None
        super(BlockReceivedMessage, self).__init__(self.MESSAGE_TYPE, self.PAYLOAD_LENGTH, buf)

    def block_hash(self):
        if self._block_hash is None:
            off = constants.HDR_COMMON_OFF
            self._block_hash = ObjectHash(self._memoryview[off:off + crypto.SHA256_HASH_LEN])
        return self._block_hash

    def __repr__(self):
        return "BlockReceivedMessage<block_hash: {}>".format(self.block_hash())
