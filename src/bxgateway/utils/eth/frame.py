import struct

import blxr_rlp as rlp
from bxcommon.utils.blockchain_utils.eth import crypto_utils, eth_common_constants


class Frame(object):
    """
    When sending a packet over RLPx, the packet will be framed.
    The frame provides information about the size of the packet and the packet's
    source protocol. There are three slightly different frames, depending on whether
    or not the frame is delivering a multi-frame packet. A multi-frame packet is a
    packet which is split (aka chunked) into multiple frames because it's size is
    larger than the protocol window size (pws; see Multiplexing). When a packet is
    chunked into multiple frames, there is an implicit difference between the first
    frame and all subsequent frames.
    Thus, the three frame types are
    normal (single frame), chunked-0 (first frame of a multi-frame packet),
    and chunked-n (subsequent frames of a multi-frame packet).


    Single-frame packet:
    header || header-mac || frame || mac

    Multi-frame packet:
    header || header-mac || frame-0 ||
    [ header || header-mac || frame-n || ... || ]
    header || header-mac || frame-last || mac
    """

    header_data_serializer = rlp.sedes.List([rlp.sedes.big_endian_int] * 3, strict=False)

    def __init__(self, msg_type, payload, protocol_id=0,
                 sequence_id=None, total_payload_size=None, is_chunked=False):

        self._payload = payload if isinstance(payload, memoryview) else memoryview(payload)

        self._msg_type = msg_type
        self._sequence_id = sequence_id
        self._protocol_id = protocol_id
        self._is_chunked = is_chunked
        self._total_payload_size = total_payload_size

    def get_body_size(self, padded=False):
        """
        Returns size of frame body
        frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
        frame relates to body w/o padding w/o mac
        """

        l = len(self.get_encoded_msg_type()) + len(self._payload)
        if padded:
            l = crypto_utils.get_padded_len_16(l)
        return l

    def get_frame_size(self):
        """
        Returns total frame size
        Frame = header16 || mac16 || dataN + [padding] || mac16

        :return: frame size
        """

        return eth_common_constants.FRAME_HDR_DATA_LEN + eth_common_constants.FRAME_MAC_LEN + \
               self.get_body_size(padded=True) + eth_common_constants.FRAME_MAC_LEN

    def get_payload(self):
        """
        Returns frame payload
        :return: frame payload
        """

        return self._payload.tobytes()

    def get_protocol_id(self):
        """
        Return protocol id for frame
        :return: protocol id
        """

        return self._protocol_id

    def get_sequence_id(self):
        """
        Returns sequence id of chunked frame
        :return: sequence id
        """

        return self._sequence_id

    def is_chunked(self):
        """
        Returns flag indicating if frame is chunked
        :return: flag
        """

        return self._is_chunked

    def get_payload_len(self):
        """
        Returns length of frame payload
        :return: length
        """

        return len(self._payload)

    def get_total_payload_size(self):
        """
        Returns total payload size of chunked frames
        :return: total payload size of chunked frames
        """

        return self._total_payload_size

    def set_payload(self, payload):
        """
        Sets frame payload
        :param payload: payload
        """

        self._payload = memoryview(payload)

    def get_header(self):
        """
        Returns frame header

        header: frame-size || header-data || padding
        frame-size: 3-byte integer size of frame, big endian encoded
        header-data:
            normal: rlp.list(protocol-type[, sequence-id])
            chunked-0: rlp.list(protocol-type, sequence-id, total-packet-size)
            chunked-n: rlp.list(protocol-type, sequence-id)
            normal, chunked-n: rlp.list(protocol-type[, sequence-id])
            values:
                protocol-type: < 2**16
                sequence-id: < 2**16 (this value is optional for normal frames)
                total-packet-size: < 2**32
        padding: zero-fill to 16-byte boundary
        :return: frame header bytes
        """

        header_parts = [self._protocol_id]

        if self._is_chunked and self._sequence_id == 0:
            header_parts.append(self._sequence_id)
            header_parts.append(self._total_payload_size)
        elif self._sequence_id is not None:  # normal, chunked_n
            header_parts.append(self._sequence_id)

        header_data = rlp.encode(header_parts, sedes=self.header_data_serializer)

        # write body_size to header
        # frame-size: 3-byte integer size of frame, big endian encoded (excludes padding)
        # frame relates to body w/o padding w/o mac
        body_size = self.get_body_size()
        assert body_size < eth_common_constants.FRAME_MAX_BODY_SIZE
        header = struct.pack(">I", body_size)[1:] + header_data
        header = crypto_utils.right_0_pad_16(header)  # padding
        assert len(header) == eth_common_constants.FRAME_HDR_DATA_LEN
        return header

    def get_body(self):
        """
        Returns frame body
        frame:
            normal: rlp(packet-type) [|| rlp(packet-data)] || padding
            chunked-0: rlp(packet-type) || rlp(packet-data...)
            chunked-n: rlp(...packet-data) || padding
        padding: zero-fill to 16-byte boundary (only necessary for last frame)
        :return: frame body bytes
        """

        encoded_msg_type = self.get_encoded_msg_type()  # packet-type
        assert isinstance(self._payload, memoryview)
        return crypto_utils.right_0_pad_16(encoded_msg_type + self._payload.tobytes())

    def get_msg_type(self):
        """
        Returns message type
        :return: message type
        """

        return self._msg_type

    def get_encoded_msg_type(self):
        """
        Returns RLP-encoded message type
        :return: bytes of RLP-encoded message type
        """

        if self._sequence_id and self._sequence_id > 0:
            return b""

        return rlp.encode(self._msg_type, sedes=rlp.sedes.big_endian_int)  # unsigned byte
