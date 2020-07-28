import struct
from abc import ABCMeta

from bxcommon import constants
from bxcommon.constants import MSG_NULL_BYTE
from bxcommon.messages.bloxroute.abstract_bloxroute_message import AbstractBloxrouteMessage


class AbstractBlockchainSyncMessage(AbstractBloxrouteMessage):
    """
    Message type for requesting/receiving direct blockchain messages for syncing chainstate.
    """

    __metaclass__ = ABCMeta

    MESSAGE_TYPE = ""
    BASE_LENGTH = 12

    def __init__(self, command=None, payload=None, buf=None):
        if buf is None:
            buf = bytearray(self.HEADER_LENGTH + constants.MSG_TYPE_LEN + len(payload))

            off = self.HEADER_LENGTH
            struct.pack_into("<12s", buf, off, command)

            off += constants.MSG_TYPE_LEN
            buf[off:off + len(payload)] = payload

        self.buf = buf
        self._command = None
        self._payload = None
        payload_length = len(buf) - self.HEADER_LENGTH
        super(AbstractBlockchainSyncMessage, self).__init__(self.MESSAGE_TYPE, payload_length, self.buf)

    def command(self):
        """
        Blockchain command. Can be either a string or an int.
        :return:
        """
        if self._command is None:
            off = self.HEADER_LENGTH
            self._command, = struct.unpack_from("<12s", self.buf, off)
            self._command = str(self._command).rstrip(MSG_NULL_BYTE)

            try:
                self._command = int(self._command)
            except:
                pass

        return self._command

    def payload(self):
        if self._payload is None:
            off = self.HEADER_LENGTH + constants.MSG_TYPE_LEN
            self._payload = self._memoryview[off:off + self.payload_len()]
        return self._payload
