from bxcommon.exceptions import ParseError
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import convert
from bxgateway.utils.eth import rlp_utils


class RlpUtilsTest(AbstractTestCase):

    def test_encode_decode_int(self):
        self.assertEqual(1, len(rlp_utils.encode_int(0)))
        self.assertEqual(1, len(rlp_utils.encode_int(127)))
        self.assertEqual(2, len(rlp_utils.encode_int(128)))
        self.assertEqual(3, len(rlp_utils.encode_int(256)))
        self.assertEqual(4, len(rlp_utils.encode_int(1000000)))

    def test_decode_int(self):
        encoded_0 = memoryview(rlp_utils.encode_int(0))
        decoded_0 = rlp_utils.decode_int(encoded_0, 0)
        self.assertEqual(0, decoded_0[0])
        self.assertEqual(1, decoded_0[1])

        encoded_127 = memoryview(rlp_utils.encode_int(127))
        decoded_127 = rlp_utils.decode_int(encoded_127, 0)
        self.assertEqual(127, decoded_127[0])
        self.assertEqual(1, decoded_127[1])

        encoded_128 = memoryview(rlp_utils.encode_int(128))
        decoded_128 = rlp_utils.decode_int(encoded_128, 0)
        self.assertEqual(128, decoded_128[0])
        self.assertEqual(2, decoded_128[1])

        encoded_256 = memoryview(rlp_utils.encode_int(256))
        decoded_256 = rlp_utils.decode_int(encoded_256, 0)
        self.assertEqual(256, decoded_256[0])
        self.assertEqual(3, decoded_256[1])

        encoded_1M = memoryview(rlp_utils.encode_int(1000000))
        decoded_1M = rlp_utils.decode_int(encoded_1M, 0)
        self.assertEqual(1000000, decoded_1M[0])
        self.assertEqual(4, decoded_1M[1])

        encoded_1M_lpad = memoryview(b"0" + rlp_utils.encode_int(1000000))
        decoded_1M_lpad = rlp_utils.decode_int(encoded_1M_lpad, 1)
        self.assertEqual(1000000, decoded_1M_lpad[0])
        self.assertEqual(4, decoded_1M_lpad[1])

    def test_int_to_big_endian(self):
        big_endian_0 = rlp_utils.int_to_big_endian(0)
        self.assertEqual(b"\x00", big_endian_0)

        big_endian_1 = rlp_utils.int_to_big_endian(1)
        self.assertEqual("01", convert.bytes_to_hex(big_endian_1))

        big_endian_1025 = rlp_utils.int_to_big_endian(1025)
        self.assertEqual("0401", convert.bytes_to_hex(big_endian_1025))

        big_endian_133124 = rlp_utils.int_to_big_endian(133124)
        self.assertEqual("020804", convert.bytes_to_hex(big_endian_133124))

    def test_big_endian_to_int(self):
        big_endian_0 = b"\x00"
        self.assertEqual(0, rlp_utils.big_endian_to_int(big_endian_0))

        big_endian_1 = b"\x01"
        self.assertEqual(1, rlp_utils.big_endian_to_int(big_endian_1))

        big_endian_1025 = b"\x04\x01"
        self.assertEqual(1025, rlp_utils.big_endian_to_int(big_endian_1025))

        big_endian_133124 = b"\x02\x08\x04"
        self.assertEqual(133124, rlp_utils.big_endian_to_int(big_endian_133124))

    def test_get_length_prefix_str(self):
        self.assertEqual(b"\x81",  rlp_utils.get_length_prefix_str(1))
        self.assertEqual(b"\x82", rlp_utils.get_length_prefix_str(2))
        self.assertEqual(b"\xb8\x38", rlp_utils.get_length_prefix_str(56))
        self.assertEqual(b"\xb8\x80", rlp_utils.get_length_prefix_str(128))
        self.assertEqual(b"\xb9\x04\x00", rlp_utils.get_length_prefix_str(1024))
        self.assertRaises(ValueError, rlp_utils.get_length_prefix_str, None)
        self.assertRaises(ValueError, rlp_utils.get_length_prefix_str, 256**8)

    def test_get_length_prefix_list(self):
        self.assertEqual(b"\xc1", rlp_utils.get_length_prefix_list(1))
        self.assertEqual(b"\xc2", rlp_utils.get_length_prefix_list(2))
        self.assertEqual(b"\xf8\x38", rlp_utils.get_length_prefix_list(56))
        self.assertEqual(b"\xf8\x80", rlp_utils.get_length_prefix_list(128))
        self.assertEqual(b"\xf9\x04\x00", rlp_utils.get_length_prefix_list(1024))
        self.assertRaises(ValueError, rlp_utils.get_length_prefix_str, None)
        self.assertRaises(ValueError, rlp_utils.get_length_prefix_str, 256 ** 8)

    def test_consume_length_prefix(self):
        self.assertRaises(ParseError, rlp_utils.consume_length_prefix, memoryview(b"\x81\x00"), 0)
        self.assertEqual((str, 2, 1), rlp_utils.consume_length_prefix(memoryview(b"\x82"), 0))
        self.assertEqual((str, 56, 2), rlp_utils.consume_length_prefix(memoryview(b"\xb8\x38"), 0))
        self.assertEqual((str, 128, 2), rlp_utils.consume_length_prefix(memoryview(b"\xb8\x80"), 0))
        self.assertEqual((str, 1024, 3), rlp_utils.consume_length_prefix(memoryview(b"\xb9\x04\x00"), 0))

        self.assertEqual((list, 1, 1), rlp_utils.consume_length_prefix(memoryview(b"\xc1"), 0))
        self.assertEqual((list, 56, 2), rlp_utils.consume_length_prefix(memoryview(b"\xf8\x38"), 0))
        self.assertEqual((list, 128, 2), rlp_utils.consume_length_prefix(memoryview(b"\xf8\x80"), 0))
        self.assertEqual((list, 1024, 3), rlp_utils.consume_length_prefix(memoryview(b"\xf9\x04\x00"), 0))

        self.assertEqual((str, 2, 3), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\x82"), 2))
        self.assertEqual((str, 56, 4), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xb8\x38"), 2))
        self.assertEqual((str, 128, 4), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xb8\x80"), 2))
        self.assertEqual((str, 1024, 5), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xb9\x04\x00"), 2))

        self.assertEqual((list, 1, 3), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xc1"), 2))
        self.assertEqual((list, 56, 4), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xf8\x38"), 2))
        self.assertEqual((list, 128, 4), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xf8\x80"), 2))
        self.assertEqual((list, 1024, 5), rlp_utils.consume_length_prefix(memoryview(b"\x00\x00\xf9\x04\x00"), 2))

        self.assertRaises(TypeError, rlp_utils.consume_length_prefix, b"\x82", 0)
        self.assertRaises(ValueError, rlp_utils.consume_length_prefix, memoryview(b"\x82"), None)

    def test_safe_ord(self):
        self.assertEqual(65, rlp_utils.safe_ord("A"))
        self.assertEqual(97, rlp_utils.safe_ord("a"))
        self.assertEqual(49, rlp_utils.safe_ord("1"))
        self.assertEqual(1, rlp_utils.safe_ord(1))
        self.assertEqual(64, rlp_utils.safe_ord(64))

    def test_ascii_chr(self):
        self.assertEqual("A", rlp_utils.ascii_chr(65))
        self.assertEqual("a", rlp_utils.ascii_chr(97))
        self.assertEqual("1", rlp_utils.ascii_chr(49))
