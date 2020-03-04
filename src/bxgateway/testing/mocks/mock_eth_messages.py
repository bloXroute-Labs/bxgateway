from typing import Optional

from bxcommon.test_utils import helpers
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import eth_constants
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import \
    NewBlockEthProtocolMessage
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.serializers.transient_block_body import \
    TransientBlockBody


def get_dummy_transaction(nonce):
    # create transaction object with dummy values multiplied by nonce to be able generate txs with different values
    return Transaction(
        nonce,
        2 * nonce,
        3 * nonce,
        helpers.generate_bytes(eth_constants.ADDRESS_LEN),
        4 * nonce,
        helpers.generate_bytes(15 * nonce),
        5 * nonce,
        6 * nonce,
        7 * nonce)


def get_dummy_block_header(nonce, timestamp=None, block_number=None, prev_block_hash: Optional[Sha256Hash] = None):
    if timestamp is None:
        timestamp = 5 * nonce
    if block_number is None:
        block_number = 2 * nonce
    if prev_block_hash is None:
        prev_block_hash = helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN)
    else:
        prev_block_hash = prev_block_hash.binary
    # create BlockHeader object with dummy values multiplied by nonce to be able generate txs with different value
    return BlockHeader(
        prev_block_hash,
        helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
        helpers.generate_bytes(eth_constants.ADDRESS_LEN),
        helpers.generate_bytes(eth_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytes(eth_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytes(eth_constants.MERKLE_ROOT_LEN),
        100 * nonce,
        nonce,
        block_number,
        3 * nonce,
        4 * nonce,
        timestamp,
        helpers.generate_bytes(100 * nonce),
        helpers.generate_bytes(eth_constants.BLOCK_HASH_LEN),
        helpers.generate_bytes(nonce)
    )


def get_dummy_transient_block_body(nonce):
    return TransientBlockBody(
        [
            get_dummy_transaction(nonce),
            get_dummy_transaction(2 * nonce),
            get_dummy_transaction(3 * nonce)
        ],
        [
            get_dummy_block_header(nonce),
            get_dummy_block_header(2 * nonce)
        ])


def get_dummy_block(nonce, header=None):
    if header is None:
        header = get_dummy_block_header(nonce)

    return Block(
        header,
        [
            get_dummy_transaction(1),
            get_dummy_transaction(2),
            get_dummy_transaction(3)
        ],
        [
            get_dummy_block_header(1),
            get_dummy_block_header(2)
        ]
    )


def new_block_eth_protocol_message(
        nonce: int,
        block_number: Optional[int] = None,
        prev_block_hash: Optional[Sha256Hash] = None
) -> NewBlockEthProtocolMessage:
    header = get_dummy_block_header(nonce, block_number=block_number, prev_block_hash=prev_block_hash)
    block = get_dummy_block(nonce, header)
    return NewBlockEthProtocolMessage(None, block, nonce * 5 + 10)
