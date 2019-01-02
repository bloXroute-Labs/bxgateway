import struct
from abc import ABCMeta

from bxcommon import constants
from bxcommon.constants import MSG_NULL_BYTE
from bxcommon.messages.bloxroute.message import Message


class AbstractBlockchainSyncMessage(Message):
    """
    Message type for requesting/receiving direct blockchain messages for syncing chainstate.
    """

    __metaclass__ = ABCMeta

    MESSAGE_TYPE = ""
    BASE_LENGTH = 12

    def __init__(self, command=None, payload=None, buf=None):
        if buf is None:
            buf = bytearray(constants.HDR_COMMON_OFF + constants.MSG_TYPE_LEN + len(payload))

            off = constants.HDR_COMMON_OFF
            struct.pack_into("<12s", buf, off, str(command))

            off += constants.MSG_TYPE_LEN
            buf[off:off + len(payload)] = payload

        self.buf = buf
        self._command = None
        self._payload = None
        payload_length = len(buf) - constants.HDR_COMMON_OFF
        super(AbstractBlockchainSyncMessage, self).__init__(self.MESSAGE_TYPE, payload_length, self.buf)

    def command(self):
        """
        Blockchain command. Can be either a string or an int.
        :return:
        """
        if self._command is None:
            off = constants.HDR_COMMON_OFF
            self._command, = struct.unpack_from("<12s", self.buf, off)
            self._command = str(self._command).rstrip(MSG_NULL_BYTE)

            try:
                self._command = int(self._command)
            except:
                pass


        return self._command

    def payload(self):
        if self._payload is None:
            off = constants.HDR_COMMON_OFF + constants.MSG_TYPE_LEN
            self._payload = self._memoryview[off:off + self.payload_len()]
        return self._payload
