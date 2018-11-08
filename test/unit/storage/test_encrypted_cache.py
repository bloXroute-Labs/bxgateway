import time

from mock import MagicMock

from bxcommon.exceptions import DecryptionError
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.utils.alarm import AlarmQueue
from bxcommon.utils.crypto import symmetric_decrypt, KEY_SIZE
from bxgateway.storage.encrypted_cache import EncryptedCache


class EncryptedCacheTest(AbstractTestCase):
    ALARM_QUEUE = AlarmQueue()

    def test_encrypt_and_store(self):
        payload = bytearray(i for i in xrange(100))
        sut = EncryptedCache(10, self.ALARM_QUEUE)
        ciphertext, block_hash = sut.encrypt_and_add_payload(payload)

        self.assertEquals(1, len(sut))
        cache_item = sut._cache.get(block_hash)

        self.assertEqual(ciphertext, cache_item.ciphertext)
        self.assertEquals(payload, symmetric_decrypt(cache_item.key, cache_item.ciphertext))

    def test_decrypt_and_get(self):
        payload = bytearray(i for i in xrange(100))
        sut1 = EncryptedCache(10, self.ALARM_QUEUE)
        ciphertext, block_hash = sut1.encrypt_and_add_payload(payload)
        key = sut1.get_encryption_key(block_hash)

        sut2 = EncryptedCache(10, self.ALARM_QUEUE)
        sut2.add_ciphertext(block_hash, ciphertext)
        decrypted = sut2.decrypt_and_get_payload(block_hash, key)

        self.assertEqual(payload, decrypted)

    def test_decrypt_ciphertext(self):
        payload = bytearray(i for i in xrange(100))
        sut1 = EncryptedCache(10, self.ALARM_QUEUE)
        ciphertext, block_hash = sut1.encrypt_and_add_payload(payload)
        key = sut1.get_encryption_key(block_hash)

        sut2 = EncryptedCache(10, self.ALARM_QUEUE)
        sut2.add_key(block_hash, key)
        decrypted = sut2.decrypt_ciphertext(block_hash, ciphertext)

        self.assertEqual(payload, decrypted)

    def test_cant_decrypt_incomplete_content(self):
        ciphertext = "foobar"
        hash_key = "baz"

        sut1 = EncryptedCache(10, self.ALARM_QUEUE)
        sut1.add_ciphertext(hash_key, ciphertext)

        with self.assertRaises(DecryptionError):
            sut1.decrypt_ciphertext(hash_key, ciphertext)

    def test_cant_decrypt_wrong_keys(self):
        ciphertext = "foobar" * 50  # ciphertext needs to be long enough to contain a nonce
        hash_key = "baz"
        bad_encryption_key = "q" * KEY_SIZE

        sut1 = EncryptedCache(10, self.ALARM_QUEUE)
        sut1.add_ciphertext(hash_key, ciphertext)
        sut1.add_key(hash_key, bad_encryption_key)

        with self.assertRaises(DecryptionError):
            sut1.decrypt_ciphertext(hash_key, ciphertext)

    def test_cache_cleanup(self):
        ciphertext = "foobar"
        hash_key = "baz"

        sut = EncryptedCache(10, self.ALARM_QUEUE)
        sut.add_ciphertext(hash_key, ciphertext)
        self.assertEqual(1, len(sut))

        time.time = MagicMock(return_value=time.time() + 20)
        self.ALARM_QUEUE.fire_alarms()
        self.assertEqual(0, len(sut))
