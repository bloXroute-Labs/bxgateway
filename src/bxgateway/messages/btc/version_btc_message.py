import struct
import time

from bxgateway.btc_constants import BTC_NODE_SERVICES, BTC_HDR_COMMON_OFF, BTC_IP_ADDR_PORT_SIZE
from bxgateway.messages.btc.btc_message import BtcMessage
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.btc_messages_util import btcbytearray_to_ipaddrport, ipaddrport_to_btcbytearray


class VersionBtcMessage(BtcMessage):
    MESSAGE_TYPE = BtcMessageType.VERSION

    def __init__(self, magic=None, version=None, dst_ip=None, dst_port=None,
                 src_ip=None, src_port=None, nonce=None, start_height=None, user_agent=None, services=BTC_NODE_SERVICES,
                 buf=None):

        if buf is None:
            buf = bytearray(109 + len(user_agent))
            self.buf = buf

            off = BTC_HDR_COMMON_OFF
            struct.pack_into('<IQQQ', buf, off, version, services, int(time.time()), 1)
            off += 28
            buf[off:off + BTC_IP_ADDR_PORT_SIZE] = ipaddrport_to_btcbytearray(dst_ip, dst_port)
            off += BTC_IP_ADDR_PORT_SIZE
            struct.pack_into('<Q', buf, off, 1)
            off += 8
            buf[off:off + BTC_IP_ADDR_PORT_SIZE] = ipaddrport_to_btcbytearray(src_ip, src_port)
            off += BTC_IP_ADDR_PORT_SIZE
            struct.pack_into('<Q%dpL' % (len(user_agent) + 1,), buf, off, nonce, user_agent, start_height)
            off += 8 + len(user_agent) + 1 + 4
            # we do not send or parse the relay boolean. if present, we benignly ignore it.

            BtcMessage.__init__(self, magic, self.MESSAGE_TYPE, off - BTC_HDR_COMMON_OFF, buf)

        else:
            self.buf = buf
            self._magic = self._command = self._payload_len = self._checksum = None
            self._payload = None

        self._memoryview = memoryview(buf)
        self._version = self._services = self._timestamp = self._dst_ip = self._dst_port = None
        self._src_ip = self._src_port = self._nonce = self._start_height = self._user_agent = None

    def version(self):
        if self._version is None:
            off = BTC_HDR_COMMON_OFF
            self._version, self._services, self._timestamp = struct.unpack_from('<IQQ', self.buf, off)

        return self._version

    def services(self):
        if self._services is None:
            self.version()

        return self._services

    def timestamp(self):
        if self._version is None:
            self.version()
        return self._timestamp

    def dst_ip(self):
        if self._dst_ip is None:
            dst_ip_off = BTC_HDR_COMMON_OFF + 28
            self._dst_ip, self._dst_port = btcbytearray_to_ipaddrport(self.buf[dst_ip_off:dst_ip_off + BTC_IP_ADDR_PORT_SIZE])
        return self._dst_ip

    def dst_port(self):
        if self._dst_port is None:
            self.dst_ip()
        return self._dst_port

    def src_ip(self):
        if self._src_ip is None:
            src_ip_off = BTC_HDR_COMMON_OFF + 54
            self._src_ip, self._src_port = btcbytearray_to_ipaddrport(self.buf[src_ip_off:src_ip_off + BTC_IP_ADDR_PORT_SIZE])
        return self._src_ip

    def src_port(self):
        if self._src_port is None:
            self.src_ip()
        return self._src_port

    def nonce(self):
        if self._nonce is None:
            nonce_off = BTC_HDR_COMMON_OFF + 72
            self._nonce = struct.unpack_from('<Q', self.buf, nonce_off)[0]
        return self._nonce

    def user_agent(self):
        if self._user_agent is None:
            user_agent_off = BTC_HDR_COMMON_OFF + 80
            self._user_agent = struct.unpack_from('%dp' % (len(self.buf) - user_agent_off,), self.buf, user_agent_off)[
                0]
            height_off = user_agent_off + len(self._user_agent) + 1
            self._start_height = struct.unpack_from('<L', self.buf, height_off)[0]
        return self._user_agent

    def start_height(self):
        if self._user_agent is None:
            self.user_agent()
        return self._start_height
