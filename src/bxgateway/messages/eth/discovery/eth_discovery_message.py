from bxgateway.messages.eth.abstract_eth_message import AbstractEthMessage
from bxcommon.utils.blockchain_utils.eth import crypto_utils, rlp_utils, eth_common_constants
from bxcommon.utils.blockchain_utils.eth import eth_common_utils
from bxgateway.eth_exceptions import WrongMACError, InvalidSignatureError


class EthDiscoveryMessage(AbstractEthMessage):

    def __init__(self, msg_bytes, private_key=None, *args, **kwargs):
        super(EthDiscoveryMessage, self).__init__(msg_bytes, *args, **kwargs)

        self._private_key = private_key
        self._public_key = None

    def __repr__(self):
        return "EthDiscoveryMessage<type: {}>".format(self.__class__.__name__)

    def get_public_key(self):
        if not self._is_deserialized:
            self.deserialize()

        return self._public_key

    def serialize(self):
        cmd_id = rlp_utils.str_to_bytes(chr(self.msg_type))
        encoded_payload = self._serialize_rlp_payload()
        signed_data = eth_common_utils.keccak_hash(cmd_id + encoded_payload)
        signature = crypto_utils.sign(signed_data, self._private_key)

        assert len(signature) == eth_common_constants.SIGNATURE_LEN
        mdc = eth_common_utils.keccak_hash(signature + cmd_id + encoded_payload)

        assert len(mdc) == eth_common_constants.MDC_LEN
        msg_bytes = bytearray(mdc + signature + cmd_id + encoded_payload)

        self._set_raw_bytes(msg_bytes)

    def deserialize(self):
        if self._msg_bytes is None or len(self._msg_bytes) < eth_common_constants.MDC_LEN + eth_common_constants.SIGNATURE_LEN:
            raise ValueError("Message bytes empty or too short.")

        mdc = self._memory_view[:eth_common_constants.MDC_LEN].tobytes()
        if mdc != eth_common_utils.keccak_hash(self._memory_view[eth_common_constants.MDC_LEN:].tobytes()):
            raise WrongMACError("Message hash does not match MDC")

        mdc_sig_len = eth_common_constants.MDC_LEN + eth_common_constants.SIGNATURE_LEN
        signature = self._memory_view[eth_common_constants.MDC_LEN:mdc_sig_len].tobytes()

        self._msg_type = rlp_utils.safe_ord(self._memory_view[mdc_sig_len])

        encoded_data = self._memory_view[mdc_sig_len:].tobytes()
        signed_data = eth_common_utils.keccak_hash(encoded_data)
        remote_pubkey = crypto_utils.recover_public_key(signed_data, signature)

        self._public_key = remote_pubkey

        if not crypto_utils.verify_signature(remote_pubkey, signature, signed_data):
            raise InvalidSignatureError("Message signature does not match public key")

        encoded_payload = self._memory_view[mdc_sig_len + eth_common_constants.MSG_TYPE_LEN:].tobytes()

        self._deserialize_rlp_payload(encoded_payload)

        self._is_deserialized = True

    @classmethod
    def unpack(cls, buf):
        pass

    @classmethod
    def validate_payload(cls, buf, unpacked_args):
        pass
