import hashlib

import bitcoin
import pyelliptic

from bxgateway.eth_exceptions import DecryptionError
from bxcommon.utils.blockchain_utils.eth import crypto_utils, rlp_utils, eth_common_constants


class ECCx(pyelliptic.ECC):
    """
    Extensions for ECC class from pyelliptic library
    Modified to work with raw_pubkey format used in RLPx
    and binding default curve and cipher
    """

    def __init__(self, raw_public_key=None, raw_private_key=None):

        self._init_ecc(raw_public_key, raw_private_key)

    def get_raw_private_key(self):
        """
        Raw private key used by instance of ECCx
        :return: private key
        """

        if self.privkey:
            return rlp_utils.str_to_bytes(self.privkey)
        return self.privkey

    def get_raw_public_key(self):
        """
        Raw public ley user by instance of ECCx
        :return: public key
        """

        if self.pubkey_x and self.pubkey_y:
            return rlp_utils.str_to_bytes(self.pubkey_x + self.pubkey_y)
        return self.pubkey_x + self.pubkey_y

    def get_ecdh_key(self, raw_public_key):
        """
        Compute public key with the local private key
        and returns a 256bits shared ECDH (Elliptic-Curve Diffie-Hellman) key
        :param raw_public_key: other parties raw public key
        :return shared key
        """

        if len(raw_public_key) != eth_common_constants.PUBLIC_KEY_LEN:
            raise ValueError("Public key of len {0} is expected but was {1}"
                             .format(eth_common_constants.PUBLIC_KEY_LEN, len(raw_public_key)))

        _, pubkey_x, pubkey_y, _ = self._decode_pubkey(raw_public_key)
        key = self.raw_get_ecdh_key(pubkey_x, pubkey_y)
        assert len(key) == eth_common_constants.SHARED_KEY_LEN
        return key

    def is_valid_key(self, raw_public_key, raw_private_key=None):
        """
        Validates raw public key
        :param raw_public_key: raw public key
        :param raw_private_key: raw private key (optional)
        :return: True is valid, False otherwise
        """

        if len(raw_public_key) != eth_common_constants.PUBLIC_KEY_LEN:
            raise ValueError("Public key of len {0} is expected but was {1}"
                             .format(eth_common_constants.PUBLIC_KEY_LEN, len(raw_public_key)))

        try:
            assert len(raw_public_key) == eth_common_constants.PUBLIC_KEY_LEN
            pubkey_x = raw_public_key[:eth_common_constants.PUBLIC_KEY_X_Y_LEN]
            pubkey_y = raw_public_key[eth_common_constants.PUBLIC_KEY_X_Y_LEN:]
            failed = bool(self.raw_check_key(raw_private_key, pubkey_x, pubkey_y))
        except (AssertionError, Exception):
            failed = True
        return not failed

    def encrypt(cls, data, raw_public_key, shared_mac_data=""):
        """
        ECIES Encrypt, where P = recipient public key is:
        1) generate r = random value
        2) generate shared-secret = kdf( ecdhAgree(r, P) )
        3) generate R = rG [same op as generating a public key]
        4) send 0x04 || R || AsymmetricEncrypt(shared-secret, plaintext) || tag


        currently used by go:
        ECIES_AES128_SHA256 = &ECIESParams{
            Hash: sha256.New,
            hashAlgo: crypto.SHA256,
            Cipher: aes.NewCipher,
            BlockSize: aes.BlockSize,
            KeyLen: 16,
            }

        :param data: data to encrypt
        :param raw_public_key: public key used of encryption
        :param shared_mac_data: shared mac
        :return: encrypted data
        """

        if not data:
            raise ValueError("Data is required")

        if len(raw_public_key) != eth_common_constants.PUBLIC_KEY_LEN:
            raise ValueError("Public key of len {0} is expected but was {1}"
                             .format(eth_common_constants.PUBLIC_KEY_LEN, len(raw_public_key)))

        # 1) generate r = random value
        ephem = ECCx()

        # 2) generate shared-secret = kdf( ecdhAgree(r, P) )
        pubkey_x = raw_public_key[:eth_common_constants.PUBLIC_KEY_X_Y_LEN]
        pubkey_y = raw_public_key[eth_common_constants.PUBLIC_KEY_X_Y_LEN:]
        key_material = ephem.raw_get_ecdh_key(pubkey_x=pubkey_x, pubkey_y=pubkey_y)

        key = crypto_utils.ecies_kdf(key_material, eth_common_constants.SHARED_KEY_LEN)
        assert len(key) == eth_common_constants.SHARED_KEY_LEN
        key_enc, key_mac = key[:eth_common_constants.ENC_KEY_LEN], key[eth_common_constants.ENC_KEY_LEN:]

        key_mac = hashlib.sha256(key_mac).digest()

        # 3) generate R = rG [same op as generating a public key]
        ephem_pubkey = ephem.get_raw_public_key()

        # encrypt
        iv = pyelliptic.Cipher.gen_IV(eth_common_constants.ECIES_CIPHER_NAME)
        assert len(iv) == eth_common_constants.IV_LEN

        ctx = pyelliptic.Cipher(key_enc, iv, eth_common_constants.CIPHER_ENCRYPT_DO, eth_common_constants.ECIES_CIPHER_NAME)
        cipher_text = ctx.ciphering(data)
        assert len(cipher_text) == len(data)

        # 4) send 0x04 || R || AsymmetricEncrypt(shared-secret, plaintext) || tag
        msg = rlp_utils.ascii_chr(eth_common_constants.ECIES_HEADER) + ephem_pubkey + iv + cipher_text

        # the MAC of a message (called the tag) as per SEC 1, 3.5.
        tag = pyelliptic.hmac_sha256(key_mac, msg[eth_common_constants.ECIES_HEADER_LEN + eth_common_constants.PUBLIC_KEY_LEN:] +
                                     rlp_utils.str_to_bytes(shared_mac_data))
        assert len(tag) == eth_common_constants.MAC_LEN
        msg += tag

        assert len(msg) - eth_common_constants.ECIES_ENCRYPT_OVERHEAD_LENGTH == len(data)
        return msg

    def decrypt(self, data, shared_mac_data=b""):
        """
        Decrypt data with ECIES method using the local private key

        ECIES Decrypt (performed by recipient):
        1) generate shared-secret = kdf( ecdhAgree(myPrivKey, msg[1:65]) )
        2) verify tag
        3) decrypt

        ecdhAgree(r, recipientPublic) == ecdhAgree(recipientPrivate, R)
        [where R = r*G, and recipientPublic = recipientPrivate*G]

        :param data: data to decrypt
        :param shared_mac_data: shared mac
        :return: decrypted data
        """

        if not data:
            raise ValueError("Data is required")

        if data[:eth_common_constants.ECIES_HEADER_LEN] != eth_common_constants.ECIES_HEADER_BYTE:
            raise DecryptionError("Wrong ECIES header")

        #  1) generate shared-secret = kdf( ecdhAgree(myPrivKey, msg[1:65]) )
        shared = data[eth_common_constants.ECIES_HEADER_LEN:eth_common_constants.ECIES_HEADER_LEN + eth_common_constants.PUBLIC_KEY_LEN]

        shared_pubkey_x = shared[:eth_common_constants.PUBLIC_KEY_X_Y_LEN]
        shared_pubkey_y = shared[eth_common_constants.PUBLIC_KEY_X_Y_LEN:]

        key_material = self.raw_get_ecdh_key(pubkey_x=shared_pubkey_x, pubkey_y=shared_pubkey_y)
        assert len(key_material) == eth_common_constants.KEY_MATERIAL_LEN
        key = crypto_utils.ecies_kdf(key_material, eth_common_constants.SHARED_KEY_LEN)
        assert len(key) == eth_common_constants.SHARED_KEY_LEN
        key_enc, key_mac = key[:eth_common_constants.ENC_KEY_LEN], key[eth_common_constants.ENC_KEY_LEN:]

        key_mac = hashlib.sha256(key_mac).digest()
        assert len(key_mac) == eth_common_constants.MAC_LEN

        tag = data[-eth_common_constants.MAC_LEN:]
        assert len(tag) == eth_common_constants.MAC_LEN

        # 2) verify tag
        header_len = eth_common_constants.ECIES_HEADER_LEN + eth_common_constants.PUBLIC_KEY_LEN

        mac_data = data[header_len:- eth_common_constants.MAC_LEN] + \
                   shared_mac_data
        if not pyelliptic.equals(pyelliptic.hmac_sha256(key_mac, mac_data), tag):
            raise DecryptionError("Fail to verify data")

        # 3) decrypt
        block_size = pyelliptic.OpenSSL.get_cipher(eth_common_constants.ECIES_CIPHER_NAME).get_blocksize()

        iv = data[header_len:header_len + block_size]
        cipher_text = data[header_len + block_size:- eth_common_constants.MAC_LEN]
        assert eth_common_constants.ECIES_HEADER_LEN + len(shared) + len(iv) + len(cipher_text) + len(tag) == len(data)
        ctx = pyelliptic.Cipher(key_enc, iv, eth_common_constants.CIPHER_DECRYPT_DO, eth_common_constants.ECIES_CIPHER_NAME)
        return ctx.ciphering(cipher_text)

    def sign(self, data):
        """
        Calculates signature for data
        :param data: data to sign
        :return: signature
        """

        if not data:
            raise ValueError("Data is required")

        signature = crypto_utils.sign(data, self.get_raw_private_key())
        assert len(signature) == eth_common_constants.SIGNATURE_LEN
        return signature

    def verify(self, signature, message):
        """
        Verifies signature for message
        :param signature: signature
        :param message: message
        :return: True if signature is valid, False otherwise
        """

        if len(signature) != eth_common_constants.SIGNATURE_LEN:
            raise ValueError("Signature len of {0} is expected but was {1}"
                             .format(eth_common_constants.SIGNATURE_LEN, len(signature)))

        return crypto_utils.verify_signature(self.get_raw_public_key(), signature, message)

    def _init_ecc(self, raw_public_key, raw_private_key):
        if raw_private_key:
            assert not raw_public_key
            raw_public_key = crypto_utils.private_to_public_key(raw_private_key)
        if raw_public_key:
            assert len(raw_public_key) == eth_common_constants.PUBLIC_KEY_LEN
            _, pubkey_x, pubkey_y, _ = self._decode_pubkey(raw_public_key)
        else:
            pubkey_x, pubkey_y = None, None

        while True:
            pyelliptic.ECC.__init__(self, pubkey_x=pubkey_x, pubkey_y=pubkey_y,
                                    raw_privkey=raw_private_key, curve=eth_common_constants.ECIES_CURVE)

            # when raw_private_key is generated by pyelliptic it sometimes has 31 bytes so we try again!
            if self.get_raw_private_key() and len(self.get_raw_private_key()) != eth_common_constants.PRIVATE_KEY_LEN:
                continue
            try:
                if self.get_raw_private_key():
                    bitcoin.get_privkey_format(self.get_raw_private_key())  # failed for some keys
                valid_private_key = True
            except AssertionError:
                valid_private_key = False
            if len(self.get_raw_public_key()) == eth_common_constants.PUBLIC_KEY_LEN and valid_private_key:
                break
            elif raw_private_key or raw_public_key:
                raise Exception("invalid private or public key")

        assert len(self.get_raw_public_key()) == eth_common_constants.PUBLIC_KEY_LEN

    def _decode_pubkey(cls, raw_pubkey):
        pubkey_x = raw_pubkey[:eth_common_constants.PUBLIC_KEY_X_Y_LEN]
        pubkey_y = raw_pubkey[eth_common_constants.PUBLIC_KEY_X_Y_LEN:]
        return eth_common_constants.ECIES_CURVE, pubkey_x, pubkey_y, eth_common_constants.PUBLIC_KEY_LEN
