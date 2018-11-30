from rlp import utils as rlp_utils

from bxcommon.test_utils import helpers
from bxcommon.utils.buffers.input_buffer import InputBuffer
from bxgateway import eth_constants
from bxgateway.testing.abstract_rlpx_cipher_test import AbstractRLPxCipherTest
from bxgateway.utils.eth import frame_utils
from bxgateway.utils.eth.framed_input_buffer import FramedInputBuffer


class FramedInputBufferTests(AbstractRLPxCipherTest):
    TEST_FRAME_SIZE = 8192

    def test_normal_frame(self):
        cipher1, cipher2 = self.setup_ciphers()

        dummy_msg_type = 1
        dummy_payload = helpers.generate_bytearray(123)
        dummy_protocol = 0

        frames = frame_utils.get_frames(dummy_msg_type, memoryview(dummy_payload), dummy_protocol,
                                        eth_constants.DEFAULT_FRAME_SIZE)

        input_buffer = InputBuffer()

        for frame in frames:
            encrypted_frame = cipher1.encrypt_frame(frame)
            encrypted_frame_bytes = bytearray(rlp_utils.str_to_bytes(encrypted_frame))
            input_buffer.add_bytes(encrypted_frame_bytes)

        # adding some dummy bytes but less than header size len
        dummy_bytes_len = eth_constants.FRAME_HDR_TOTAL_LEN / 2
        input_buffer.add_bytes(helpers.generate_bytearray(dummy_bytes_len))

        framed_input_buffer = FramedInputBuffer(cipher2)

        is_full, msg_type = framed_input_buffer.peek_message(input_buffer)
        self.assertTrue(is_full)
        self.assertEqual(msg_type, dummy_msg_type)

        message, full_msg_type = framed_input_buffer.get_full_message()
        self.assertEqual(message, dummy_payload)
        self.assertEqual(full_msg_type, dummy_msg_type)

        self.assertEqual(input_buffer.length, dummy_bytes_len)

    def test_chunked_frames(self):
        cipher1, cipher2 = self.setup_ciphers()

        dummy_msg_type = 10
        expected_frames_count = 10
        dummy_payload = helpers.generate_bytearray(self.TEST_FRAME_SIZE * (expected_frames_count - 1))
        dummy_protocol = 0

        frames = frame_utils.get_frames(dummy_msg_type, memoryview(dummy_payload), dummy_protocol,
                                        self.TEST_FRAME_SIZE)
        self.assertEqual(len(frames), expected_frames_count)

        frames_bytes = bytearray(0)

        for frame in frames:
            encrypted_frame = cipher1.encrypt_frame(frame)
            encrypted_frame_bytes = bytearray(rlp_utils.str_to_bytes(encrypted_frame))
            frames_bytes += encrypted_frame_bytes

        # adding some dummy bytes but less than header size len
        dummy_bytes_len = eth_constants.FRAME_HDR_TOTAL_LEN / 2
        frames_bytes += helpers.generate_bytearray(dummy_bytes_len)

        input_buffer = InputBuffer()
        framed_input_buffer = FramedInputBuffer(cipher2)

        read_start = 0
        read_size = eth_constants.FRAME_HDR_TOTAL_LEN
        is_full = False
        msg_type = None

        while not is_full and read_start < len(frames_bytes):
            input_buffer.add_bytes(frames_bytes[read_start:read_start + read_size])
            is_full, msg_type = framed_input_buffer.peek_message(input_buffer)
            read_start += read_size

        self.assertTrue(is_full)
        self.assertEqual(msg_type, dummy_msg_type)
