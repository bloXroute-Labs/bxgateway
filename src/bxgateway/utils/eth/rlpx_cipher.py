import os
import random
import struct
import sys
import pyelliptic
from Crypto.Cipher import AES
import blxr_rlp as rlp
from blxr_rlp import sedes

from bxutils import logging

from bxcommon.exceptions import ParseError

from bxgateway.eth_exceptions import InvalidKeyError, AuthenticationError, CipherNotInitializedError
from bxcommon.utils.blockchain_utils.eth import crypto_utils, rlp_utils, eth_common_utils, eth_common_constants
from bxgateway.utils.eth.eccx import ECCx
from bxgateway.utils.eth.frame import Frame

logger = logging.get_logger(__name__)


class RLPxCipher(object):

    def __init__(self, is_initiator, private_key, remote_public_key=None):

        self._ecc = ECCx(raw_private_key=private_key)

        if remote_public_key and not self._ecc.is_valid_key(remote_public_key):
            raise InvalidKeyError("Invalid remote pubkey")

        self._ephemeral_ecc = ECCx()

        self._is_initiator = is_initiator
        self._private_key = private_key
        self._remote_pubkey = remote_public_key

        self._is_eip8_auth = False
        self._remote_ephemeral_pubkey = None
        self._initiator_nonce = None
        self._responder_nonce = None
        self._auth_init = None
        self._auth_ack = None
        self._aes_secret = None
        self._token = None
        self._aes_enc = None
        self._aes_dec = None
        self._mac_enc = None
        self._egress_mac = None
        self._ingress_mac = None
        self._is_ready = False
        self._remote_version = 0

    def get_private_key(self):
        """
        Get private key of current ECCx object
        :return: private key
        """

        return self._private_key

    def is_ready(self):
        """
        Returns flag indicating if cipher is ready for encrypted messages exchange
        :return: True if cipher is ready, False otherwise
        """

        return self._is_ready

    def create_auth_message(self):
        """
        Generates authentication message bytes

        1. initiator generates ecdhe-random and nonce and creates auth
        2. initiator connects to remote and sends auth

        New:
        E(remote-pubk,
            S(ephemeral-privk, ecdh-shared-secret ^ nonce) ||
            H(ephemeral-pubk) || pubk || nonce || 0x0
        )
        Known:
        E(remote-pubk,
            S(ephemeral-privk, token ^ nonce) || H(ephemeral-pubk) || pubk || nonce || 0x1)

        :param remote_pubkey: public key of remote node
        :param nonce: nonce
        :return: authentication message bytes
        """

        assert self._is_initiator

        ecdh_shared_secret = self._ecc.get_ecdh_key(self._remote_pubkey)
        token = ecdh_shared_secret
        flag = 0x0
        self._initiator_nonce = eth_common_utils.keccak_hash(rlp_utils.int_to_big_endian(random.randint(0, sys.maxsize)))
        assert len(self._initiator_nonce) == eth_common_constants.SHA3_LEN_BYTES

        token_xor_nonce = crypto_utils.string_xor(token, self._initiator_nonce)
        assert len(token_xor_nonce) == eth_common_constants.SHA3_LEN_BYTES

        ephemeral_pubkey = self._ephemeral_ecc.get_raw_public_key()
        assert len(ephemeral_pubkey) == eth_common_constants.PUBLIC_KEY_LEN
        if not self._ecc.is_valid_key(ephemeral_pubkey):
            raise InvalidKeyError("Invalid ephemeral pubkey")

        # S(ephemeral-privk, ecdh-shared-secret ^ nonce)
        S = self._ephemeral_ecc.sign(token_xor_nonce)
        assert len(S) == eth_common_constants.SIGNATURE_LEN

        # S || H(ephemeral-pubk) || pubk || nonce || 0x0
        auth_message = S + eth_common_utils.keccak_hash(ephemeral_pubkey) + self._ecc.get_raw_public_key() + \
                       self._initiator_nonce + rlp_utils.ascii_chr(flag)
        return auth_message

    def encrypt_auth_message(self, auth_message):
        """
        Encrypts authentication message
        :param auth_message: message bytes
        :param remote_pubkey: remote public key
        :return: encrypted message
        """

        assert self._is_initiator

        self._auth_init = self._ecc.encrypt(auth_message, self._remote_pubkey)
        return self._auth_init

    def decrypt_auth_message(self, cipher_text):
        """
        Decrypts authentication message
        :param cipher_text: encrypted message
        :return: decrypted authentication message
        """

        assert not self._is_initiator

        if len(cipher_text) < eth_common_constants.ENC_AUTH_MSG_LEN:
            logger.debug("Minimal len of auth message is {0} but was {1}. Continue waiting for more bytes."
                         .format(eth_common_constants.ENC_AUTH_MSG_LEN, len(cipher_text)))
            return None, None

        try:
            if len(cipher_text) == eth_common_constants.ENC_AUTH_MSG_LEN:
                # Decrypt plain auth message
                size = eth_common_constants.ENC_AUTH_MSG_LEN
                message = self._ecc.decrypt(cipher_text[:size])
            else:
                # Decrypt EIP8 auth message
                size = struct.unpack(">H", cipher_text[:eth_common_constants.EIP8_AUTH_PREFIX_LEN])[0] + \
                       eth_common_constants.EIP8_AUTH_PREFIX_LEN

                if len(cipher_text) < size:
                    logger.debug("Expected auth message size is {0} but was {1}. Continue waiting more bytes."
                                 .format(size, len(cipher_text)))
                    return None, None

                message = self._ecc.decrypt(bytes(cipher_text[eth_common_constants.EIP8_AUTH_PREFIX_LEN:size]),
                                            shared_mac_data=bytes(cipher_text[:eth_common_constants.EIP8_AUTH_PREFIX_LEN]))
                self._is_eip8_auth = True
        except RuntimeError as e:
            raise AuthenticationError(e)

        self._auth_init = cipher_text[:size]
        return message, size

    def parse_auth_message(self, message):
        assert not self._is_initiator

        if self._is_eip8_auth:
            (signature, pubkey, nonce, version) = self.parse_eip8_auth_message(message)
        else:
            (signature, pubkey, nonce, version) = self.parse_plain_auth_message(message)

        token = self._ecc.get_ecdh_key(pubkey)
        remote_ephemeral_pubkey = crypto_utils.recover_public_key(crypto_utils.string_xor(token, nonce),
                                                                  signature)
        if not self._ecc.is_valid_key(remote_ephemeral_pubkey):
            raise InvalidKeyError("Invalid remote ephemeral pubkey")

        self._remote_ephemeral_pubkey = remote_ephemeral_pubkey
        self._initiator_nonce = nonce
        self._remote_pubkey = pubkey
        self._remote_version = eth_common_constants.P2P_PROTOCOL_VERSION

    def parse_plain_auth_message(self, message):
        if len(message) != eth_common_constants.AUTH_MSG_LEN:
            raise ValueError("Authentication message of len {0} is expected but was {1}"
                             .format(eth_common_constants.ENC_AUTH_MSG_LEN, len(message)))

        signature = message[:eth_common_constants.SIGNATURE_LEN]

        pubkey_start = eth_common_constants.SIGNATURE_LEN + eth_common_constants.MAC_LEN
        pubkey = message[pubkey_start:pubkey_start + eth_common_constants.PUBLIC_KEY_LEN]
        if not self._ecc.is_valid_key(pubkey):
            raise InvalidKeyError("Invalid initiator pubkey")

        nonce_start = eth_common_constants.SIGNATURE_LEN + eth_common_constants.MAC_LEN + eth_common_constants.PUBLIC_KEY_LEN
        nonce = message[nonce_start: nonce_start + eth_common_constants.AUTH_NONCE_LEN]
        known_flag = bool(rlp_utils.safe_ord(message[nonce_start + eth_common_constants.AUTH_NONCE_LEN:]))
        assert known_flag == 0

        return signature, pubkey, nonce, eth_common_constants.P2P_PROTOCOL_VERSION

    def parse_eip8_auth_message(self, message):
        eip8_auth_serializers = sedes.List(
            [
                sedes.Binary(min_length=eth_common_constants.SIGNATURE_LEN, max_length=eth_common_constants.SIGNATURE_LEN),  # sig
                sedes.Binary(min_length=eth_common_constants.PUBLIC_KEY_LEN, max_length=eth_common_constants.PUBLIC_KEY_LEN),
                # pubkey
                sedes.Binary(min_length=eth_common_constants.AUTH_NONCE_LEN, max_length=eth_common_constants.AUTH_NONCE_LEN),  # nonce
                sedes.BigEndianInt()  # version
            ], strict=False)

        values = rlp.decode(message, sedes=eip8_auth_serializers, strict=False)
        signature, pubkey, nonce, version = values

        return signature, pubkey, nonce, version

    def create_auth_ack_message(self):
        """
        authRecipient = E(remote-pubk, remote-ephemeral-pubk || nonce || 0x1) // token found
        authRecipient = E(remote-pubk, remote-ephemeral-pubk || nonce || 0x0) // token not found

        nonce, ephemeral_pubkey, version are local!
        """

        assert not self._is_initiator

        ephemeral_pubkey = self._ephemeral_ecc.get_raw_public_key()
        encoded_nonce = rlp.sedes.big_endian_int.serialize(random.randint(0, eth_common_constants.MAX_NONCE))
        self._responder_nonce = eth_common_utils.keccak_hash(encoded_nonce)

        if self._is_eip8_auth:
            msg = self.create_eip8_auth_ack_message(ephemeral_pubkey, self._responder_nonce,
                                                    eth_common_constants.P2P_PROTOCOL_VERSION)
            assert len(msg) > eth_common_constants.AUTH_ACK_MSG_LEN
        else:
            msg = self.create_plain_auth_ack_message(ephemeral_pubkey, self._responder_nonce,
                                                     eth_common_constants.P2P_PROTOCOL_VERSION)
            assert len(msg) == eth_common_constants.AUTH_ACK_MSG_LEN

        return msg

    def create_plain_auth_ack_message(self, ephemeral_pubkey, nonce, version):
        return ephemeral_pubkey + nonce + b"\x00"


    def create_eip8_auth_ack_message(self, ephemeral_pubkey, nonce, version):
        eip8_ack_serializers = sedes.List(
            [
                sedes.Binary(min_length=eth_common_constants.PUBLIC_KEY_LEN, max_length=eth_common_constants.PUBLIC_KEY_LEN),  # ephemeral pubkey
                sedes.Binary(min_length=eth_common_constants.AUTH_NONCE_LEN, max_length=eth_common_constants.AUTH_NONCE_LEN),  # nonce
                sedes.BigEndianInt()  # version
            ], strict=False)

        data = rlp.encode((ephemeral_pubkey, nonce, version), sedes=eip8_ack_serializers)
        pad = os.urandom(random.randint(eth_common_constants.EIP8_ACK_PAD_MIN, eth_common_constants.EIP8_ACK_PAD_MAX))
        return data + pad

    def encrypt_auth_ack_message(self, ack_message):
        assert not self._is_initiator

        if self._is_eip8_auth:
            prefix = struct.pack(">H", len(ack_message) + eth_common_constants.ECIES_ENCRYPT_OVERHEAD_LENGTH)
            enc_auth_ack = self._ecc.encrypt(ack_message, self._remote_pubkey, shared_mac_data=prefix)

            self._auth_ack = prefix + enc_auth_ack
        else:
            self._auth_ack = self._ecc.encrypt(ack_message, self._remote_pubkey)
            assert len(self._auth_ack) == eth_common_constants.ENC_AUTH_ACK_MSG_LEN

        return self._auth_ack

    def decrypt_auth_ack_message(self, cipher_text):
        """
        Decrypts authentication acknowledge message
        :param cipher_text: encrypted message
        :return: decrypted message
        """

        assert self._is_initiator

        if len(cipher_text) != eth_common_constants.ENC_AUTH_ACK_MSG_LEN:
            raise ValueError("Expected length of auth ack message is {0} but was provided {1}"
                             .format(eth_common_constants.ENC_AUTH_ACK_MSG_LEN, len(cipher_text)))

        (size, eph_pubkey, nonce) = self._decode_auth_ack_message(cipher_text)
        self._auth_ack = cipher_text[:size]
        self._remote_ephemeral_pubkey = eph_pubkey[:eth_common_constants.PUBLIC_KEY_LEN]
        self._responder_nonce = nonce
        self._remote_version = eth_common_constants.AUTH_MSG_VERSION

        if not self._ecc.is_valid_key(self._remote_ephemeral_pubkey):
            raise InvalidKeyError("Invalid remote ephemeral pubkey")

        return cipher_text[size:]

    def setup_cipher(self):
        """
        Sets up cipher parameters.
        Needs to be called after initial authentication handshake
        """

        assert self._responder_nonce
        assert self._initiator_nonce
        assert self._auth_init
        assert self._auth_ack
        assert self._remote_ephemeral_pubkey
        if not self._ecc.is_valid_key(self._remote_ephemeral_pubkey):
            raise InvalidKeyError("Invalid remote ephemeral pubkey")

        # derive base secrets from ephemeral key agreement
        # ecdhe-shared-secret = ecdh.agree(ephemeral-privkey, remote-ephemeral-pubk)
        ecdhe_shared_secret = self._ephemeral_ecc.get_ecdh_key(self._remote_ephemeral_pubkey)

        # shared-secret = sha3(ecdhe-shared-secret || sha3(nonce || initiator-nonce))
        shared_secret = eth_common_utils.keccak_hash(
            ecdhe_shared_secret + eth_common_utils.keccak_hash(self._responder_nonce + self._initiator_nonce))

        self.ecdhe_shared_secret = ecdhe_shared_secret  # used in tests
        self.shared_secret = shared_secret  # used in tests

        # token = sha3(shared-secret)
        self._token = eth_common_utils.keccak_hash(shared_secret)

        # aes-secret = sha3(ecdhe-shared-secret || shared-secret)
        self._aes_secret = eth_common_utils.keccak_hash(ecdhe_shared_secret + shared_secret)

        # mac-secret = sha3(ecdhe-shared-secret || aes-secret)
        self.mac_secret = eth_common_utils.keccak_hash(ecdhe_shared_secret + self._aes_secret)

        # setup sha3 instances for the MACs
        # egress-mac = sha3.update(mac-secret ^ recipient-nonce || auth-sent-init)
        mac1 = crypto_utils.get_sha3_calculator(
            crypto_utils.string_xor(self.mac_secret, self._responder_nonce) + self._auth_init)
        # ingress-mac = sha3.update(mac-secret ^ initiator-nonce || auth-recvd-ack)
        mac2 = crypto_utils.get_sha3_calculator(
            crypto_utils.string_xor(self.mac_secret, self._initiator_nonce) + self._auth_ack)

        if self._is_initiator:
            self._egress_mac, self._ingress_mac = mac1, mac2
        else:
            self._egress_mac, self._ingress_mac = mac2, mac1

        iv = "\x00" * eth_common_constants.IV_LEN
        self._aes_enc = pyelliptic.Cipher(self._aes_secret, iv, eth_common_constants.CIPHER_ENCRYPT_DO,
                                          ciphername=eth_common_constants.RLPX_CIPHER_NAME)
        self._aes_dec = pyelliptic.Cipher(self._aes_secret, iv, eth_common_constants.CIPHER_DECRYPT_DO,
                                          ciphername=eth_common_constants.RLPX_CIPHER_NAME)
        self._mac_enc = AES.new(self.mac_secret, AES.MODE_ECB).encrypt

        self._is_ready = True

    def encrypt_frame(self, frame):
        """
        Encrypts frame
        :param frame: frame data
        :return: encrypted frame
        """

        if not isinstance(frame, Frame):
            raise TypeError("frame must be of type Frame but was {0}".format(type(frame)))

        if not self._is_ready:
            raise CipherNotInitializedError(f"failed to encrypt frame {frame}, the cipher was never initialized!")

        header = frame.get_header()
        body = frame.get_body()

        # header
        header_ciphertext = self.aes_encode(header)

        # egress-mac.update(aes(mac-secret,egress-mac) ^ header-ciphertext).digest
        header_mac = self.mac_egress(
            crypto_utils.string_xor(self._mac_enc(self.mac_egress()[:eth_common_constants.FRAME_MAC_LEN]),
                                    header_ciphertext))[:eth_common_constants.FRAME_MAC_LEN]

        # frame
        frame_ciphertext = self.aes_encode(body)
        assert len(frame_ciphertext) == len(body)
        # egress-mac.update(aes(mac-secret,egress-mac) ^
        # left128(egress-mac.update(frame-ciphertext).digest))
        fmac_seed = self.mac_egress(frame_ciphertext)
        frame_mac = self.mac_egress(
            crypto_utils.string_xor(self._mac_enc(self.mac_egress()[:eth_common_constants.FRAME_MAC_LEN]),
                                    fmac_seed[:eth_common_constants.FRAME_MAC_LEN]))[:eth_common_constants.FRAME_MAC_LEN]

        return bytearray(header_ciphertext + header_mac + frame_ciphertext + frame_mac)

    def decrypt_frame_header(self, data):
        """
        Decrypts frame header
        :param data: frame header data
        :return: decrypted header data
        """

        if len(data) != eth_common_constants.FRAME_HDR_TOTAL_LEN:
            raise ValueError("Frame header len is expected to be {0} but was {1}"
                             .format(eth_common_constants.FRAME_HDR_TOTAL_LEN, len(data)))

        if not self._is_ready:
            raise CipherNotInitializedError("failed to decrypt frame header, the cipher was never initialized!")

        header_cipher_text = data[:eth_common_constants.FRAME_HDR_DATA_LEN]
        header_mac = data[eth_common_constants.FRAME_HDR_DATA_LEN:eth_common_constants.FRAME_HDR_TOTAL_LEN]

        # ingress-mac.update(aes(mac-secret,ingress-mac) ^ header-ciphertext).digest
        mac1 = self.mac_ingress()[:eth_common_constants.FRAME_MAC_LEN]
        mac_enc = self._mac_enc(mac1)
        mac_sxor = crypto_utils.string_xor(mac_enc, header_cipher_text)

        expected_header_mac = self.mac_ingress(mac_sxor)[:eth_common_constants.FRAME_MAC_LEN]

        # expected_header_mac = self.updateMAC(self.ingress_mac, header_ciphertext)
        if not expected_header_mac == header_mac:
            raise AuthenticationError("Invalid header mac")

        return self.aes_decode(header_cipher_text)

    def decrypt_frame_body(self, data, body_size):
        """
        Decrypts frame body
        :param data: frame data
        :param body_size: body size
        :return: decrypted frame body
        """

        if not self._is_ready:
            raise CipherNotInitializedError("failed to decrypt frame body, the cipher was never initialized!")


        # frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
        # frame relates to body w/o padding w/o mac

        read_size = crypto_utils.get_padded_len_16(body_size)

        if not len(data) >= read_size + eth_common_constants.FRAME_MAC_LEN:
            raise ParseError("Insufficient body length")

        frame_cipher_text = data[:read_size]
        frame_mac = data[read_size:read_size + eth_common_constants.FRAME_MAC_LEN]

        # ingres-mac.update(aes(mac-secret,ingres-mac) ^
        # left128(ingres-mac.update(frame-ciphertext).digest))
        frame_mac_seed = self.mac_ingress(frame_cipher_text)
        expected_frame_mac = self.mac_ingress(
            crypto_utils.string_xor(self._mac_enc(self.mac_ingress()[:eth_common_constants.FRAME_MAC_LEN]),
                                    frame_mac_seed[:eth_common_constants.FRAME_MAC_LEN]))[:eth_common_constants.FRAME_MAC_LEN]

        if not frame_mac == expected_frame_mac:
            raise AuthenticationError("Invalid frame mac")

        return self.aes_decode(frame_cipher_text)[:body_size]

    def aes_encode(self, data=""):
        if isinstance(data, bytearray):
            bytes_to_encode = bytes(data)
        elif isinstance(data, memoryview):
            bytes_to_encode = data.tobytes()
        else:
            bytes_to_encode = data

        return self._aes_enc.update(bytes_to_encode)

    def aes_decode(self, data=""):
        if isinstance(data, bytearray):
            bytes_to_decode = bytes(data)
        elif isinstance(data, memoryview):
            bytes_to_decode = data.tobytes()
        else:
            bytes_to_decode = data

        return self._aes_dec.update(bytes_to_decode)

    def mac_egress(self, data=b""):
        data = rlp_utils.str_to_bytes(data)
        self._egress_mac.update(data)
        return self._egress_mac.digest()

    def mac_ingress(self, data=b""):
        data = rlp_utils.str_to_bytes(data)
        self._ingress_mac.update(data)
        return self._ingress_mac.digest()

    def _decode_auth_ack_message(self, cipher_text):
        try:
            message = self._ecc.decrypt(cipher_text[:eth_common_constants.ENC_AUTH_ACK_MSG_LEN])
        except RuntimeError as e:
            raise AuthenticationError(e)
        eph_pubkey = message[:eth_common_constants.PUBLIC_KEY_LEN]
        nonce = message[eth_common_constants.PUBLIC_KEY_LEN:eth_common_constants.PUBLIC_KEY_LEN + eth_common_constants.AUTH_NONCE_LEN]
        known = rlp_utils.safe_ord(message[-1])
        assert known == 0
        return (eth_common_constants.ENC_AUTH_ACK_MSG_LEN, eph_pubkey, nonce)
