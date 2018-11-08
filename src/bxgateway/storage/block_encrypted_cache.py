from bxcommon.constants import BLOXROUTE_ENCRYPTION_CACHE_TIMEOUT_S
from bxgateway.storage.encrypted_cache import EncryptedCache


# TODO: these functions arent intended to be permanent, but pulling
# them out here for now for easier tests to convert these to different
# formats for symmetric encryption / hashing.
# We can probably implement some sort of EncryptionCacheProtocol that
# converts hash/payload types to avoid the copies on msg_hash (and so we
# can make ObjectHash the storage key), but that doesn't solve the copy
# of the payload, which is probably the mort significant part.
def message_hash_to_hash_key(msg_hash):
    return bytes(msg_hash.binary)


def message_blob_to_ciphertext(msg_blob):
    return msg_blob.tobytes()


class BlockEncryptedCache(EncryptedCache):
    def __init__(self, alarm_queue):
        super(BlockEncryptedCache, self).__init__(BLOXROUTE_ENCRYPTION_CACHE_TIMEOUT_S, alarm_queue)

    def decrypt_ciphertext(self, hash_key, ciphertext):
        """
        Attempts to decrypt from hash and blob from a BroadcastMessage
        :param hash_key: BroadcastMessage.msg_hash()
        :param ciphertext: BroadcastMessage.blob()
        :return decrypted block
        """
        return super(BlockEncryptedCache, self).decrypt_ciphertext(
            message_hash_to_hash_key(hash_key),
            message_blob_to_ciphertext(ciphertext)
        )

    def decrypt_and_get_payload(self, hash_key, encryption_key):
        """
        Attempts to decrypt from hash and key from a KeyMessage
        :param hash_key: KeyMessage.hash_key()
        :param encryption_key: KeyMessage.key()
        :return:  decrypted block
        """
        return super(BlockEncryptedCache, self).decrypt_and_get_payload(
            message_hash_to_hash_key(hash_key),
            message_blob_to_ciphertext(encryption_key)
        )

    def has_encryption_key_for_hash(self, hash_key):
        return super(BlockEncryptedCache, self).has_encryption_key_for_hash(
            message_hash_to_hash_key(hash_key),
        )

    def has_ciphertext_for_hash(self, hash_key):
        return super(BlockEncryptedCache, self).has_ciphertext_for_hash(
            message_hash_to_hash_key(hash_key),
        )

    def add_ciphertext(self, hash_key, ciphertext):
        return super(BlockEncryptedCache, self).add_ciphertext(
            message_hash_to_hash_key(hash_key),
            message_blob_to_ciphertext(ciphertext)
        )

    def add_key(self, hash_key, encryption_key):
        return super(BlockEncryptedCache, self).add_key(
            message_hash_to_hash_key(hash_key),
            message_blob_to_ciphertext(encryption_key)
        )
