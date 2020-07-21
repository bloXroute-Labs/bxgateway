from bxcommon.exceptions import ParseError
from bxcommon.utils.buffers.input_buffer import InputBuffer
from bxgateway.utils.eth import frame_utils
from bxcommon.utils.blockchain_utils.eth import rlp_utils, eth_common_constants
from bxgateway.utils.eth.rlpx_cipher import RLPxCipher


class FramedInputBuffer(object):
    """
    Input buffer for Ethereum framed messages
    """

    def __init__(self, rlpx_cipher):

        if not isinstance(rlpx_cipher, RLPxCipher):
            raise TypeError("Cipher is expected of type RLPxCipher but was {0}"
                            .format(type(rlpx_cipher)))

        self._rlpx_cipher = rlpx_cipher
        self._payload_buffer = InputBuffer()

        self._receiving_frame = False
        self._chunked_frames_in_progress = False
        self._chunked_frames_total_body_size = False
        self._chunked_frames_body_size_received = 0

        self._current_msg_type = None

        self._current_frame_size = None
        self._current_frame_body_size = None
        self._current_frame_enc_body_size = None
        self._current_frame_protocol_id = None
        self._current_frame_sequence_id = None

        self._full_message_received = False

    def peek_message(self, input_buffer):
        """
        Peeks message from input frame
        :param input_buffer: input buffer
        :return: tuple (flag if full message is received, message type)
        """

        if not isinstance(input_buffer, InputBuffer):
            raise ValueError("Expected type InputBuffer")

        if self._full_message_received:
            raise ValueError("Get full message before trying to peek another one")

        if not self._receiving_frame and input_buffer.length >= eth_common_constants.FRAME_HDR_TOTAL_LEN:
            enc_header_bytes = input_buffer.remove_bytes(eth_common_constants.FRAME_HDR_TOTAL_LEN)
            header_bytes = self._rlpx_cipher.decrypt_frame_header(enc_header_bytes)
            body_size, protocol_id, sequence_id, total_payload_len = frame_utils.parse_frame_header(header_bytes)

            self._current_frame_body_size = body_size
            self._current_frame_protocol_id = protocol_id
            self._current_frame_size = frame_utils.get_full_frame_size(body_size)
            self._current_frame_enc_body_size = self._current_frame_size - eth_common_constants.FRAME_HDR_TOTAL_LEN
            self._current_frame_sequence_id = sequence_id

            if sequence_id == 0 and total_payload_len is not None:
                self._chunked_frames_in_progress = True
                self._chunked_frames_total_body_size = total_payload_len

            self._receiving_frame = True

        if self._receiving_frame and input_buffer.length >= self._current_frame_enc_body_size:
            frame_enc_body_bytes = input_buffer.remove_bytes(self._current_frame_enc_body_size)

            body = self._rlpx_cipher.decrypt_frame_body(frame_enc_body_bytes, self._current_frame_body_size)

            msg_type_is_expected = not self._chunked_frames_in_progress or self._current_frame_sequence_id == 0
            payload, msg_type = frame_utils.parse_frame_body(body, msg_type_is_expected)

            if msg_type_is_expected:
                self._current_msg_type = msg_type

            self._payload_buffer.add_bytes(bytearray(rlp_utils.str_to_bytes(payload)))

            if not self._chunked_frames_in_progress:
                self._full_message_received = True
            else:
                self._chunked_frames_body_size_received += len(body)

                if self._chunked_frames_body_size_received > self._chunked_frames_total_body_size:
                    raise ParseError("Expected total body length for frame message is {0} but received {1}"
                                     .format(self._chunked_frames_total_body_size,
                                             self._chunked_frames_body_size_received))

                if self._chunked_frames_body_size_received == self._chunked_frames_total_body_size:
                    self._full_message_received = True

            self._receiving_frame = False

        return self._full_message_received, self._current_msg_type

    def get_full_message(self):
        """
        Returns full message from input buffer
        :return: full message
        """

        assert self._full_message_received

        message_length = self._payload_buffer.length
        message = self._payload_buffer.remove_bytes(message_length)
        msg_type = self._current_msg_type

        self._full_message_received = False

        self._receiving_frame = False
        self._chunked_frames_in_progress = False
        self._chunked_frames_total_body_size = False
        self._chunked_frames_body_size_received = 0

        self._current_msg_type = None
        self._current_frame_size = None
        self._current_frame_body_size = None
        self._current_frame_enc_body_size = None
        self._current_frame_protocol_id = None
        self._current_frame_sequence_id = None

        return message, msg_type
