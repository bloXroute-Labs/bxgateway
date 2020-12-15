import struct

import blxr_rlp as rlp
from bxcommon.exceptions import ParseError
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxcommon.utils.blockchain_utils.eth.crypto_utils import get_padded_len_16
from bxgateway.utils.eth.frame import Frame


def get_frames(msg_type, payload_bytes, protocol_id=eth_common_constants.DEFAULT_FRAME_PROTOCOL_ID,
               window_size=eth_common_constants.DEFAULT_FRAME_SIZE):
    """
    Parses frames from message bytes

    :param msg_type: type of message
    :param payload_bytes: frames payload bytes
    :param protocol_id: protocol id
    :param window_size: frame window size
    :return: list of frames
    """

    payload_mem_view = memoryview(payload_bytes)

    max_frame_payload_size = get_max_frame_payload_size(window_size)

    if len(payload_bytes) <= max_frame_payload_size:
        return [Frame(msg_type, payload_bytes, protocol_id)]

    frames = []
    total_payload_size = eth_common_constants.FRAME_MSG_TYPE_LEN + len(payload_bytes)
    sequence_id = 0

    while (True):
        start_index = sequence_id * max_frame_payload_size
        end_index = (sequence_id + 1) * max_frame_payload_size

        payload = payload_mem_view[start_index:end_index]
        frame = Frame(msg_type if sequence_id == 0 else None,
                      payload,
                      protocol_id,
                      sequence_id=sequence_id,
                      total_payload_size=(total_payload_size if sequence_id == 0 else None),
                      is_chunked=True)

        frames.append(frame)

        if end_index >= len(payload_mem_view):
            break

        sequence_id += 1

    return frames


def get_max_frame_payload_size(window_size):
    """
    Returns max frame payload size for provided window size
    :param window_size: frame window size
    :return: max payload size
    """

    return window_size - eth_common_constants.FRAME_HDR_DATA_LEN - \
           2 * eth_common_constants.FRAME_MAC_LEN - \
           eth_common_constants.FRAME_MSG_TYPE_LEN


def get_full_frame_size(body_size):
    """
    Returns size of full frame for provided frame body size
    :param body_size: frame body size
    :return: size of full frame
    """

    return eth_common_constants.FRAME_HDR_TOTAL_LEN + \
           get_padded_len_16(body_size) + \
           eth_common_constants.FRAME_MAC_LEN


def parse_frame_header(header_bytes):
    """
    Parses frame header
    :param header_bytes: frame header bytes
    :return: tuple (body size, protocol id, sequence id, total payload size)
    """

    sequence_id = None
    total_payload_size = None

    body_size = struct.unpack(">I", b"\x00" + header_bytes[:3])[0]

    try:
        header_data = rlp.decode(header_bytes[3:], sedes=Frame.header_data_serializer, strict=False)
    except rlp.RLPException:
        raise ParseError("Invalid rlp data in frame header")

    protocol_id = header_data[0]

    if len(header_data) > 1:
        sequence_id = header_data[1]

    if len(header_data) == 3:
        total_payload_size = header_data[2]

    return body_size, protocol_id, sequence_id, total_payload_size


def parse_frame_body(body_bytes, has_msg_type):
    """
    Parses frame body

    :param body_bytes: frame body bytes
    :param has_msg_type: message type
    :return: tuple (payload, message type)
    """

    msg_type = None

    if has_msg_type:
        item, end = rlp.codec.consume_item(body_bytes, 0)
        msg_type = rlp.sedes.big_endian_int.deserialize(item)
        payload = body_bytes[end:]
    else:
        payload = body_bytes

    return payload, msg_type
