import hashlib
import os
import struct

import bitcoin
from Crypto.Hash import keccak
from coincurve import PrivateKey, PublicKey

from bxcommon.utils import convert
from bxcommon.utils.blockchain_utils.eth.eth_common_util import keccak_hash
from bxgateway import eth_constants
from bxgateway.utils.eth import rlp_utils


def sign(msg, private_key):
    """
    Signs message using private key
    """

    if not msg:
        raise ValueError("Message is required")

    if len(private_key) != eth_constants.PRIVATE_KEY_LEN:
        raise ValueError("Private key is expected of len {0} but was {1}"
                         .format(eth_constants.PRIVATE_KEY_LEN, len(private_key)))

    pk = PrivateKey(private_key)
    return pk.sign_recoverable(msg, hasher=None)


def get_sha3_calculator(input):
    """
    Returns object that can be used to calculate sha3 hash
    :param input: input bytes
    :return: object that can calculate sha3 256 hash
    """

    if input is None:
        raise ValueError("Input is required")

    return keccak.new(digest_bits=eth_constants.SHA3_LEN_BITS, update_after_digest=True, data=input)


def recover_public_key(message, signature):
    """
    Recovers public key from signed message
    :param message: message
    :param signature: signature
    :return: public key
    """

    if len(signature) != eth_constants.SIGNATURE_LEN:
        raise ValueError("Expected signature len of {0} but was {1}"
                         .format(eth_constants.SIGNATURE_LEN, len(signature)))

    pk = PublicKey.from_signature_and_message(signature, message, hasher=None)
    return pk.format(compressed=False)[1:]


def verify_signature(pubkey, signature, message):
    """
    Verifies signature
    :param pubkey: signing public key
    :param signature: message signature
    :param message: signature
    :return: returns True if signature is valid, False otherwise
    """

    if len(pubkey) != eth_constants.PUBLIC_KEY_LEN:
        raise ValueError("Pubkey is expected of len {0} but was {1}"
                         .format(eth_constants.PUBLIC_KEY_LEN, len(pubkey)))

    if len(signature) != eth_constants.SIGNATURE_LEN:
        raise ValueError("Signature is expected of len {0} but was {1}"
                         .format(eth_constants.PUBLIC_KEY_LEN, len(signature)))

    if not message:
        raise ValueError("Message is required")

    pk = PublicKey.from_signature_and_message(signature, message, hasher=None)
    return pk.format(compressed=False) == b"\04" + pubkey


def encode_signature(v, r, s):
    """
    Calculates byte representation of ECC signature from parameters
    :param v:
    :param r:
    :param s:
    :return: bytes of ECC signature
    """

    if not isinstance(v, int) or v not in (27, 28):
        raise ValueError("v is expected to be int or long in range (27, 28)")

    vb, rb, sb = rlp_utils.ascii_chr(v - 27), bitcoin.encode(r, 256), bitcoin.encode(s, 256)
    return _left_0_pad_32(rb) + _left_0_pad_32(sb) + vb


def decode_signature(sig):
    """
    Decodes coordinates from ECC signature bytes
    :param sig: signature bytes
    :return: ECC signature parameters
    """

    if not sig:
        raise ValueError("Signature is required")

    return rlp_utils.safe_ord(sig[64]) + 27, bitcoin.decode(sig[0:32], 256), bitcoin.decode(sig[32:64], 256)


def make_private_key(seed):
    """
    Generates ECC private using provided seed value
    :param seed: seed used to generate ECC private key
    :return: ECC private key
    """

    if not seed:
        raise ValueError("Seed is required")

    return keccak_hash(seed)


def generate_random_private_key_hex_str():
    """
    Generate hex string of random ECC private key
    :return: ECC private key hex string
    """

    # seed can be any random bytes of any length
    random_seed = os.urandom(100)
    private_key_bytes = make_private_key(random_seed)
    return convert.bytes_to_hex(private_key_bytes)


def private_to_public_key(raw_private_key):
    """
    Calculates public key for private key
    :param raw_private_key: ECC private key
    :return: public key
    """

    if len(raw_private_key) != eth_constants.PRIVATE_KEY_LEN:
        raise ValueError("Private key is expected of len {0} but was {1}"
                         .format(eth_constants.PRIVATE_KEY_LEN, len(raw_private_key)))

    raw_pubkey = bitcoin.encode_pubkey(bitcoin.privtopub(raw_private_key), "bin_electrum")
    assert len(raw_pubkey) == eth_constants.PUBLIC_KEY_LEN
    return raw_pubkey


def ecies_kdf(key_material, key_len):
    """
    interop w/go ecies implementation

    for sha3, blocksize is 136 bytes
    for sha256, blocksize is 64 bytes

    NIST SP 800-56a Concatenation Key Derivation Function (see section 5.8.1).

    :param key_material: key material
    :param key_len: key length
    :return: key
    """

    if len(key_material) != eth_constants.KEY_MATERIAL_LEN:
        raise ValueError("Key material is expected of len {0} but was {1}"
                         .format(eth_constants.KEY_MATERIAL_LEN, len(key_material)))

    if key_len <= 0:
        raise ValueError("Key len is expected to be positive but was {0}".format(key_len))

    s1 = b""
    key = b""
    hash_blocksize = eth_constants.BLOCK_HASH_LEN
    reps = ((key_len + 7) * 8) / (hash_blocksize * 8)
    counter = 0
    while counter <= reps:
        counter += 1
        ctx = hashlib.sha256()
        ctx.update(struct.pack(">I", counter))
        ctx.update(key_material)
        ctx.update(s1)
        key += ctx.digest()
    return key[:key_len]


def string_xor(s1, s2):
    """
    Calculates xor of two strings
    :param s1: string 1
    :param s2: string 2
    :return: xor of two strings
    """

    if len(s1) != len(s2):
        raise ValueError("String must have the same length")

    return b"".join(rlp_utils.ascii_chr(rlp_utils.safe_ord(a) ^ rlp_utils.safe_ord(b)) for a, b in zip(s1, s2))


def get_padded_len_16(x):
    """
    Length of bytes if padded to 16
    :param x: bytes
    :return: padded length
    """

    return x if x % eth_constants.MSG_PADDING == 0 else x + eth_constants.MSG_PADDING - (x % eth_constants.MSG_PADDING)


def right_0_pad_16(data):
    """
    Pads bytes with 0 on the right to length of 16
    :param data: bytes
    :return: padded bytes
    """

    if len(data) % eth_constants.MSG_PADDING:
        data += b"\x00" * (eth_constants.MSG_PADDING - len(data) % eth_constants.MSG_PADDING)
    return data


def _left_0_pad_32(x):
    """
    Pads bytes with 0 on the left to length of 32
    :param x: bytes
    :return: padded bytes
    """

    return b"\x00" * (32 - len(x)) + x
