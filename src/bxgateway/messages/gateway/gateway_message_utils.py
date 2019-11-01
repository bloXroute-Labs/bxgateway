import socket
import struct

from bxutils import logging

from bxcommon import constants

logger = logging.get_logger(__name__)

PORT_PACKING_FORMAT = "<H"


def pack_ip_port(buf, offset, ip, port):
    try:
        buf[offset + 10] = 0xff
        buf[offset + 11] = 0xff
        buf[offset + constants.IP_V4_PREFIX_LENGTH:offset + constants.IP_ADDR_SIZE_IN_BYTES] = \
            socket.inet_pton(socket.AF_INET, ip)
        struct.pack_into(PORT_PACKING_FORMAT, buf, offset + constants.IP_ADDR_SIZE_IN_BYTES, port)
    except socket.error:
        try:
            buf[offset: offset + 16] = socket.inet_pton(socket.AF_INET6, ip)
            struct.pack_into(PORT_PACKING_FORMAT, buf, offset + 16, port)
        except socket.error as e:
            logger.debug("Packing ip port failed!: {}", e)


def unpack_ip_port(buf):
    port, = struct.unpack_from(PORT_PACKING_FORMAT, buf, constants.IP_ADDR_SIZE_IN_BYTES)
    if buf[:+ constants.IP_V4_PREFIX_LENGTH] == constants.IP_V4_PREFIX[:]:
        ip_bytes = buf[constants.IP_V4_PREFIX_LENGTH:constants.IP_ADDR_SIZE_IN_BYTES]
        return socket.inet_ntop(socket.AF_INET, ip_bytes), port
    else:
        return socket.inet_ntop(socket.AF_INET6, buf[:constants.IP_ADDR_SIZE_IN_BYTES]), port
