from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils import convert
from bxgateway import eth_constants
from bxgateway.utils.eth import crypto_utils


class CryptoUtilsTests(AbstractTestCase):

    def test_sign_and_verify_signature__valid_signature(self):
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        public_key = crypto_utils.private_to_public_key(dummy_private_key)

        # generate random bytes
        msg = helpers.generate_bytearray(222)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = crypto_utils.sign(msg_hash, dummy_private_key)

        self.assertTrue(crypto_utils.verify_signature(public_key, signature, msg_hash))

    def test_sign_and_verify_signature__invalid_signature(self):
        dummy_private_key1 = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        dummy_private_key2 = crypto_utils.make_private_key(helpers.generate_bytearray(222))
        public_key2 = crypto_utils.private_to_public_key(dummy_private_key2)

        # generate random bytes
        msg = helpers.generate_bytearray(333)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = crypto_utils.sign(msg_hash, dummy_private_key1)

        self.assertFalse(crypto_utils.verify_signature(public_key2, signature, msg_hash))

    def test_sha3(self):
        dummy_msg1 = helpers.generate_bytearray(111)
        dummy_msg2 = helpers.generate_bytearray(1111)

        sha1 = crypto_utils.keccak_hash(dummy_msg1)
        sha2 = crypto_utils.keccak_hash(dummy_msg2)

        self.assertEqual(len(sha1), eth_constants.SHA3_LEN_BYTES)
        self.assertEqual(len(sha2), eth_constants.SHA3_LEN_BYTES)

        self.assertNotEqual(sha1, sha2)

    def test_get_sha3_calculator(self):
        input1 = helpers.generate_bytearray(111)
        sha_calculator = crypto_utils.get_sha3_calculator(input1)
        sha1 = sha_calculator.digest()

        input2 = helpers.generate_bytearray(222)
        sha_calculator.update(input2)
        sha2 = sha_calculator.digest()

        sha_calculator.update(input1)
        sha1_2 = sha_calculator.digest()

        self.assertEqual(len(sha1), eth_constants.SHA3_LEN_BYTES)
        self.assertEqual(len(sha2), eth_constants.SHA3_LEN_BYTES)

        self.assertNotEqual(sha1, sha2)
        self.assertNotEqual(sha1, sha1_2)

    def test_recover_public_key(self):
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        public_key = crypto_utils.private_to_public_key(dummy_private_key)

        msg = helpers.generate_bytearray(222)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = crypto_utils.sign(msg_hash, dummy_private_key)

        recovered_pub_key = crypto_utils.recover_public_key(msg_hash, signature)

        self.assertEqual(recovered_pub_key, public_key)

    def test_encode_decode_signature(self):
        dummy_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        msg = helpers.generate_bytearray(222)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = crypto_utils.sign(msg_hash, dummy_private_key)

        v, r, s = crypto_utils.decode_signature(signature)

        self.assertIsNotNone(v)
        self.assertIsNotNone(r)
        self.assertIsNotNone(s)

        encoded_signature = crypto_utils.encode_signature(v, r, s)

        self.assertEqual(encoded_signature, signature)

    def test_ecies_kdf(self):
        input1 = self._from_hex("0x0de72f1223915fa8b8bf45dffef67aef8d89792d116eb61c9a1eb02c422a4663")
        expect1 = self._from_hex("0x1d0c446f9899a3426f2b89a8cb75c14b")
        test1 = crypto_utils.ecies_kdf(input1, 16)
        self.assertEqual(len(test1), len(expect1))
        self.assertEqual(test1, expect1)

        kdfInput2 = self._from_hex("0x961c065873443014e0371f1ed656c586c6730bf927415757f389d92acf8268df")
        kdfExpect2 = self._from_hex("0x4050c52e6d9c08755e5a818ac66fabe478b825b1836fd5efc4d44e40d04dabcc")
        kdfTest2 = crypto_utils.ecies_kdf(kdfInput2, 32)
        self.assertEqual(len(kdfTest2), len(kdfExpect2))
        self.assertEqual(kdfTest2, kdfExpect2)

    def test_string_xor(self):
        str1 = helpers.generate_bytearray(111)
        str2 = helpers.generate_bytearray(111)

        sxor = crypto_utils.string_xor(str1, str2)

        self.assertIsNotNone(sxor)

        self.assertEqual(len(sxor), len(str1))

    def test_padded_len_16(self):
        self.assertEqual(crypto_utils.get_padded_len_16(1), 16)
        self.assertEqual(crypto_utils.get_padded_len_16(16), 16)
        self.assertEqual(crypto_utils.get_padded_len_16(20), 32)
        self.assertEqual(crypto_utils.get_padded_len_16(32), 32)

    def test_right_0_pad_16(self):
        msg = helpers.generate_bytearray(22)
        padded_msg = crypto_utils.right_0_pad_16(msg)

        msg_from_padded = padded_msg[:22]
        padding = msg[22:]

        self.assertEqual(msg_from_padded, msg[:22])
        self.assertEqual(padding, bytearray(10))

    def _from_hex(self, x):
        return convert.hex_to_bytes(x[2:])
