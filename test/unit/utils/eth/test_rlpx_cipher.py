from bxgateway.testing.abstract_rlpx_cipher_test import AbstractRLPxCipherTest


class RLPxCipherTests(AbstractRLPxCipherTest):

    def test_rlpx_cipher_end_to_end(self):
        cipher1, cipher2 = self.setup_ciphers()

        self.assertTrue(cipher1)
        self.assertTrue(cipher2)

        dummy_text = b"Lorem ipsum"

        encoded_text = cipher1.aes_encode(dummy_text)
        decoded_text = cipher2.aes_decode(encoded_text)

        self.assertNotEquals(encoded_text, dummy_text)
        self.assertEqual(decoded_text, dummy_text)

        dummy_text2 = b"Lorem ipsum 2"

        encoded_text2 = cipher2.aes_encode(dummy_text2)
        decoded_text2 = cipher1.aes_decode(encoded_text2)

        self.assertNotEquals(encoded_text2, dummy_text2)
        self.assertEqual(decoded_text2, dummy_text2)

        mac_ingress1 = cipher1.mac_ingress()
        mac_egress2 = cipher2.mac_egress()

        self.assertEqual(mac_ingress1, mac_egress2)

        mac_egress1 = cipher1.mac_egress()
        mac_ingress2 = cipher2.mac_ingress()

        self.assertEqual(mac_egress1, mac_ingress2)
