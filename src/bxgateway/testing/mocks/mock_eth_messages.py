from bxcommon.test_utils import helpers
from bxgateway import eth_constants
from bxgateway.messages.eth.serializers.block import Block
from bxgateway.messages.eth.serializers.block_header import BlockHeader
from bxgateway.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.serializers.transient_block_body import TransientBlockBody


def get_dummy_transaction(nonce):
    # create transaction object with dummy values multiplied by nonce to be able generate txs with different values
    return Transaction(
        nonce,
        2 * nonce,
        3 * nonce,
        helpers.generate_bytearray(eth_constants.ADDRESS_LEN),
        4 * nonce,
        helpers.generate_bytearray(15 * nonce),
        5 * nonce,
        6 * nonce,
        7 * nonce)


def get_dummy_block_header(nonce, timestamp=None):
    if timestamp is None:
        timestamp = 5 * nonce

    # create BlockHeader object with dummy values multiplied by nonce to be able generate txs with different value
    return BlockHeader(
        helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
        helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
        helpers.generate_bytearray(eth_constants.ADDRESS_LEN),
        helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytearray(eth_constants.MERKLE_ROOT_LEN),
        100 * nonce,
        nonce,
        2 * nonce,
        3 * nonce,
        4 * nonce,
        timestamp,
        helpers.generate_bytearray(100 * nonce),
        helpers.generate_bytearray(eth_constants.BLOCK_HASH_LEN),
        helpers.generate_bytearray(nonce)
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

