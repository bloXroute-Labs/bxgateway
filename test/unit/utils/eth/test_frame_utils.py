from bxcommon.test_utils import helpers
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxgateway.testing.abstract_rlpx_cipher_test import AbstractRLPxCipherTest
from bxgateway.utils.eth import frame_utils
from bxgateway.utils.eth.frame import Frame


class FrameUtilsTests(AbstractRLPxCipherTest):
    TEST_FRAME_SIZE = 8192

    def test_get_frames__normal_frame(self):
        msg_type = 1
        dummy_payload = memoryview(helpers.generate_bytearray(123))
        dummy_protocol_id = 0

        frames = frame_utils.get_frames(msg_type, dummy_payload, dummy_protocol_id,
                                        window_size=self.TEST_FRAME_SIZE)
        self.assertTrue(frames)
        self.assertEqual(len(frames), 1)

        frame = frames[0]

        self.assertTrue(frame)
        self.assertEqual(frame.get_msg_type(), msg_type)
        self.assertEqual(frame.get_payload(), dummy_payload)
        self.assertEqual(frame.get_protocol_id(), dummy_protocol_id)
        self.assertIsNone(frame.get_sequence_id())
        self.assertFalse(frame.is_chunked())

        self.assertTrue(frame.get_header())
        self.assertTrue(frame.get_body())

    def test_get_frames__chunked(self):
        msg_type = 1
        expected_frames_count = 4
        dummy_payload = memoryview(helpers.generate_bytearray(self.TEST_FRAME_SIZE) *
                                   (expected_frames_count - 1))
        dummy_protocol_id = 0

        frames = frame_utils.get_frames(msg_type, dummy_payload, dummy_protocol_id,
                                        window_size=self.TEST_FRAME_SIZE)
        self.assertTrue(frames)
        self.assertEqual(len(frames), expected_frames_count)

        all_payload = bytearray(0)
        expected_sequence_id = 0

        for frame in frames:

            self.assertTrue(frame.get_payload())
            self.assertEqual(frame.get_protocol_id(), dummy_protocol_id)
            self.assertEqual(frame.get_sequence_id(), expected_sequence_id)
            self.assertTrue(frame.is_chunked())

            # Verify that only the first of chunked frames has information about total payload length
            if expected_sequence_id == 0:
                self.assertEqual(frame.get_msg_type(), msg_type)

                expected_total_size = len(dummy_payload) + eth_common_constants.FRAME_MSG_TYPE_LEN
                self.assertEqual(frame.get_total_payload_size(), expected_total_size)
            else:
                self.assertIsNone(frame.get_msg_type())
                self.assertIsNone(frame.get_total_payload_size())

            self.assertTrue(frame.get_header())
            self.assertTrue(frame.get_body())

            all_payload += frame.get_payload()

            expected_sequence_id += 1

        self.assertEqual(all_payload, dummy_payload)

    def test_encrypt_decrypt_frame__normal_frame(self):
        cipher1, cipher2 = self.setup_ciphers()

        msg_type = 1
        dummy_payload = memoryview(helpers.generate_bytearray(123))
        dummy_protocol_id = 0

        frames = frame_utils.get_frames(msg_type, dummy_payload, dummy_protocol_id,
                                        window_size=self.TEST_FRAME_SIZE)
        self.assertTrue(frames)
        self.assertEqual(len(frames), 1)

        frame = frames[0]

        encrypted_frame = memoryview(cipher1.encrypt_frame(frame))
        self.assertTrue(encrypted_frame)

        decrypted_frame = self._decrypt_frame(encrypted_frame, cipher2)
        self._assert_frames_equal(decrypted_frame, frame)

    def test_encrypt_decrypt_frame__chunked_frame(self):
        cipher1, cipher2 = self.setup_ciphers()

        msg_type = 1
        expected_frames_count = 3
        dummy_payload = memoryview(helpers.generate_bytearray(self.TEST_FRAME_SIZE *
                                                              (expected_frames_count - 1)))
        dummy_protocol_id = 0

        frames = frame_utils.get_frames(msg_type, dummy_payload, dummy_protocol_id,
                                        window_size=self.TEST_FRAME_SIZE)
        self.assertTrue(frames)
        self.assertEqual(len(frames), expected_frames_count)

        for frame in frames:
            encrypted_frame = memoryview(cipher1.encrypt_frame(frame))
            self.assertTrue(encrypted_frame)

            decrypted_frame = self._decrypt_frame(encrypted_frame, cipher2)
            self._assert_frames_equal(decrypted_frame, frame)

    def _decrypt_frame(self, frame, cipher):
        encrypted_frame = memoryview(frame)
        self.assertTrue(encrypted_frame)

        decrypted_header = cipher.decrypt_frame_header(encrypted_frame[:eth_common_constants.FRAME_HDR_TOTAL_LEN].tobytes())
        self.assertEqual(len(decrypted_header), eth_common_constants.FRAME_HDR_DATA_LEN)

        body_size, protocol_id, sequence_id, total_payload_size = frame_utils.parse_frame_header(decrypted_header)

        decrypted_body = cipher.decrypt_frame_body(encrypted_frame[eth_common_constants.FRAME_HDR_TOTAL_LEN:].tobytes(),
                                                   body_size)

        payload, msg_type = frame_utils.parse_frame_body(decrypted_body, sequence_id is None or sequence_id == 0)

        return Frame(msg_type, memoryview(payload),
                     protocol_id=protocol_id,
                     sequence_id=sequence_id,
                     total_payload_size=total_payload_size,
                     is_chunked=sequence_id is not None)

    def _assert_frames_equal(self, firstFrame, secondFrame):
        assert isinstance(firstFrame, Frame)
        assert isinstance(secondFrame, Frame)

        self.assertEqual(firstFrame.get_msg_type(), secondFrame.get_msg_type())
        self.assertEqual(firstFrame.get_protocol_id(), secondFrame.get_protocol_id())
        self.assertEqual(firstFrame.get_sequence_id(), secondFrame.get_sequence_id())
        self.assertEqual(firstFrame.get_total_payload_size(), secondFrame.get_total_payload_size())
        self.assertEqual(firstFrame.is_chunked(), secondFrame.is_chunked())
        self.assertEqual(firstFrame.get_payload(), secondFrame.get_payload())
