from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils import helpers

from bxcommon.utils.blockchain_utils.eth import crypto_utils, eth_common_constants
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher


class AbstractRLPxCipherTest(AbstractTestCase):
    """
    Base class for unit test that need to use RLPx Cipher
    """

    def setup_ciphers(self, private_key1=None, private_key2=None):
        if private_key1 is None:
            private_key1 = crypto_utils.make_private_key(helpers.generate_bytearray(111))
        if private_key2 is None:
            private_key2 = crypto_utils.make_private_key(helpers.generate_bytearray(111))

        public_key1 = crypto_utils.private_to_public_key(private_key1)
        public_key2 = crypto_utils.private_to_public_key(private_key2)

        cipher1 = RLPxCipher(True, private_key1, public_key2)
        cipher2 = RLPxCipher(False, private_key2, public_key1)

        self.assertFalse(cipher1.is_ready())
        self.assertFalse(cipher2.is_ready())

        auth_msg = cipher1.create_auth_message()
        self.assertEqual(len(auth_msg), eth_common_constants.AUTH_MSG_LEN)
        self.assertTrue(auth_msg)

        enc_auth_msg = cipher1.encrypt_auth_message(auth_msg)
        self.assertEqual(len(enc_auth_msg), eth_common_constants.ENC_AUTH_MSG_LEN)

        decrypted_auth_msg, size = cipher2.decrypt_auth_message(enc_auth_msg)
        self.assertEqual(len(decrypted_auth_msg), eth_common_constants.AUTH_MSG_LEN)
        self.assertEqual(size, eth_common_constants.ENC_AUTH_MSG_LEN)

        cipher2.parse_auth_message(decrypted_auth_msg)

        auth_ack_message = cipher2.create_auth_ack_message()
        enc_auth_ack_message = cipher2.encrypt_auth_ack_message(auth_ack_message)

        cipher1.decrypt_auth_ack_message(enc_auth_ack_message)

        cipher1.setup_cipher()
        cipher2.setup_cipher()

        self.assertTrue(cipher1.is_ready())
        self.assertTrue(cipher2.is_ready())

        return cipher1, cipher2
