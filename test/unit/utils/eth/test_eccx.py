from bxcommon.test_utils import helpers
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxgateway import eth_constants
from bxgateway.utils.eth import crypto_utils
from bxgateway.utils.eth.eccx import ECCx


class ECCxTests(AbstractTestCase):

    def setUp(self):
        self._private_key = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        self._eccx = ECCx(raw_private_key=self._private_key)

    def test_get_raw_private_key(self):
        private_key = self._eccx.get_raw_private_key()
        self.assertEqual(private_key, self._private_key)

    def test_get_raw_public_key(self):
        public_key = self._eccx.get_raw_public_key()
        expected_public_key = crypto_utils.private_to_public_key(self._private_key)
        self.assertEqual(public_key, expected_public_key)

    def test_sign_and_verify__valid(self):
        msg = helpers.generate_bytearray(111)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = self._eccx.sign(msg_hash)

        self.assertTrue(self._eccx.verify(signature, msg_hash))

    def test_sign_and_verify__invalid(self):
        msg = helpers.generate_bytearray(111)
        msg_hash = crypto_utils.keccak_hash(msg)

        signature = self._eccx.sign(msg_hash)

        other_private_key = crypto_utils.make_private_key(helpers.generate_bytearray(222))
        another_eccx = ECCx(raw_private_key=other_private_key)

        self.assertFalse(another_eccx.verify(signature, msg_hash))

    def test_encrypt_decrypt(self):
        msg = b"test message"

        encrypted_message = self._eccx.encrypt(msg, self._eccx.get_raw_public_key())
        self.assertNotEqual(encrypted_message, msg)

        decrypted_message = self._eccx.decrypt(encrypted_message)
        self.assertEqual(decrypted_message, msg)

    def test_get_ecdh_key(self):
        private_key1 = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        public_key1 = crypto_utils.private_to_public_key(private_key1)

        private_key2 = crypto_utils.make_private_key(helpers.generate_bytearray(222))
        public_key2 = crypto_utils.private_to_public_key(private_key2)

        private_key3 = crypto_utils.make_private_key(helpers.generate_bytearray(333))

        eccx1 = ECCx(raw_private_key=private_key1)
        eccx2 = ECCx(raw_private_key=private_key2)
        eccx3 = ECCx(raw_private_key=private_key3)

        key1 = eccx1.get_ecdh_key(public_key2)
        key2 = eccx2.get_ecdh_key(public_key1)
        key3 = eccx3.get_ecdh_key(public_key1)

        self.assertEqual(key1, key2)
        self.assertNotEqual(key1, key3)

    def test_is_valid_key(self):
        for i in range(10):
            eccx = self._get_test_eccx(i + 1)
            self.assertEqual(len(eccx.get_raw_public_key()), eth_constants.PUBLIC_KEY_LEN)

            self.assertTrue(eccx.is_valid_key(eccx.get_raw_public_key()))
            self.assertTrue(eccx.is_valid_key(eccx.get_raw_public_key(), eccx.get_raw_private_key()))

        invalid_eccx = self._get_test_eccx(111)
        invalid_pubkey = "\x00" * eth_constants.PUBLIC_KEY_LEN
        self.assertFalse(invalid_eccx.is_valid_key(invalid_pubkey))

    def _get_test_eccx(self, nonce):
        private_key = crypto_utils.make_private_key(helpers.generate_bytearray(nonce))
        return ECCx(raw_private_key=private_key)
