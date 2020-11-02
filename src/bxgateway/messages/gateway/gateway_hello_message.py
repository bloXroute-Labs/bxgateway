import struct

from bxcommon import constants
from bxcommon.messages.bloxroute.version_message import VersionMessage
from bxcommon.utils import uuid_pack
from bxcommon.utils.stats import message_utils
from bxgateway.messages.gateway.gateway_message_type import GatewayMessageType


class GatewayHelloMessage(VersionMessage):
    """
    Hello message type for Gateway-Gateway connections.

    Exchanges ip/port info for registration. Duplicate ip/ports will be dropped.
    Ordering is provided so both nodes drop the same connection (e.g. the one with lower ordering).
    """
    MESSAGE_TYPE = GatewayMessageType.HELLO
    PAYLOAD_LENGTH = (VersionMessage.VERSION_MESSAGE_LENGTH +
                      constants.IP_ADDR_SIZE_IN_BYTES +
                      constants.UL_SHORT_SIZE_IN_BYTES +
                      constants.UL_INT_SIZE_IN_BYTES +
                      constants.NODE_ID_SIZE_IN_BYTES
                      )
    ADDITIONAL_LENGTH = constants.NODE_ID_SIZE_IN_BYTES

    def __init__(self, protocol_version=None, network_num=None, ip=None, port=None, ordering=None, node_id=None, buf=None):
        if buf is None:
            buf = bytearray(self.HEADER_LENGTH + self.PAYLOAD_LENGTH)

            off = VersionMessage.BASE_LENGTH

            message_utils.pack_ip_port(buf, off, ip, port)
            off += constants.IP_ADDR_SIZE_IN_BYTES + constants.UL_SHORT_SIZE_IN_BYTES

            struct.pack_into("<L", buf, off, ordering)
            off += constants.UL_INT_SIZE_IN_BYTES
            struct.pack_into("%ss" % constants.NODE_ID_SIZE_IN_BYTES, buf, off, uuid_pack.to_bytes(node_id))
        self.buf = buf
        self._ip = None
        self._port = None
        self._ordering = None
        self._node_id = None
        super(GatewayHelloMessage, self).__init__(self.MESSAGE_TYPE, self.PAYLOAD_LENGTH, protocol_version,
                                                  network_num, buf)

    def _unpack_buffer(self):
        off = VersionMessage.BASE_LENGTH

        self._ip, self._port = message_utils.unpack_ip_port(self._memoryview[off:].tobytes())
        off += constants.IP_ADDR_SIZE_IN_BYTES + constants.UL_SHORT_SIZE_IN_BYTES

        self._ordering, = struct.unpack_from("<L", self._memoryview, off)
        off += constants.UL_INT_SIZE_IN_BYTES
        _node_id, = struct.unpack_from("%ss" % constants.NODE_ID_SIZE_IN_BYTES, self._memoryview, off)
        self._node_id = uuid_pack.from_bytes(_node_id)

    def ip(self):
        if self._ip is None:
            self._unpack_buffer()
        return self._ip

    def port(self):
        if self._port is None:
            self._unpack_buffer()
        return self._port

    def ordering(self):
        if self._ordering is None:
            self._unpack_buffer()
        return self._ordering

    def node_id(self):
        if self._node_id is None:
            self._unpack_buffer()
        return self._node_id

    def __repr__(self):
        return "GatewayHelloMessage<ip: {}, port: {}, ordering: {}>".format(self.ip(), self.port(), self.ordering())
