from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.constants import IP_ADDR_SIZE_IN_BYTES, UL_SHORT_SIZE_IN_BYTES

from bxgateway.messages.gateway.gateway_message_utils import pack_ip_port, unpack_ip_port


class GatewayMessageUtilsTest(AbstractTestCase):

    def test_pack_unpack_ipv4(self):
        ipv4_ip = "127.0.0.1"
        buf = bytearray((IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES) * 2)
        memview = memoryview(buf)

        pack_ip_port(buf, 0, ipv4_ip, 8000)
        ip, port = unpack_ip_port(memview[:IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES].tobytes())
        self.assertEqual(ipv4_ip, ip)
        self.assertEqual(8000, port)

        pack_ip_port(buf, IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES, ipv4_ip, 9000)
        ip, port = unpack_ip_port(memview[IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES:].tobytes())
        self.assertEqual(ipv4_ip, ip)
        self.assertEqual(9000, port)

    def test_pack_unpack_ipv6(self):
        ipv6_ip = "ffff::ffff:c0a8:101"
        buf = bytearray((IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES) * 2)
        memview = memoryview(buf)

        pack_ip_port(buf, 0, ipv6_ip, 8000)
        ip, port = unpack_ip_port(memview[:IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES].tobytes())
        self.assertEqual(ipv6_ip, ip)
        self.assertEqual(8000, port)

        pack_ip_port(buf, IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES, ipv6_ip, 9000)
        ip, port = unpack_ip_port(memview[IP_ADDR_SIZE_IN_BYTES + UL_SHORT_SIZE_IN_BYTES:].tobytes())
        self.assertEqual(ipv6_ip, ip)
        self.assertEqual(9000, port)
