from typing import Optional

from bxcommon.messages.bloxroute.tx_message import TxMessage
from bxcommon.messages.eth.serializers.block_header import BlockHeader
from bxcommon.test_utils import helpers
from bxcommon.utils import convert
from bxcommon.utils.object_hash import Sha256Hash
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxcommon.feed.eth.eth_raw_transaction import EthRawTransaction
from bxcommon.feed.new_transaction_feed import FeedSource
from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter
from bxgateway.messages.eth.protocol.new_block_eth_protocol_message import \
    NewBlockEthProtocolMessage
from bxgateway.messages.eth.protocol.transactions_eth_protocol_message import \
    TransactionsEthProtocolMessage
from bxcommon.messages.eth.serializers.block import Block
from bxcommon.messages.eth.serializers.transaction import Transaction
from bxgateway.messages.eth.serializers.transient_block_body import \
    TransientBlockBody


def generate_eth_tx_message() -> TxMessage:
    transaction = get_dummy_transaction(1)
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[0][0]
    return tx_message


def generate_eth_raw_transaction(source: FeedSource = FeedSource.BDN_SOCKET) -> EthRawTransaction:
    tx_message = generate_eth_tx_message()
    return EthRawTransaction(
        tx_message.tx_hash(), tx_message.tx_val(), source
    )


def generate_eth_raw_transaction_with_to_address(
        source: FeedSource = FeedSource.BDN_SOCKET,
        to_address: str = helpers.generate_bytes(eth_common_constants.ADDRESS_LEN)) -> EthRawTransaction:
    transaction = get_dummy_transaction(1, to_address_str=to_address)
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[0][0]
    return EthRawTransaction(
        tx_message.tx_hash(), tx_message.tx_val(), source
    )


def get_dummy_transaction(
    nonce: int, gas_price: Optional[int] = None, v: int = 27, to_address_str: Optional[str] = None
) -> Transaction:
    if gas_price is None:
        gas_price = 2 * nonce
    if to_address_str is None:
        to_address = helpers.generate_bytes(eth_common_constants.ADDRESS_LEN)
    else:
        to_address = convert.hex_to_bytes(to_address_str)
    # create transaction object with dummy values multiplied by nonce to be able generate txs with different values
    return Transaction(
        nonce,
        gas_price,
        3 * nonce,
        to_address,
        4 * nonce,
        helpers.generate_bytes(15 * nonce),
        v,
        6 * nonce,
        7 * nonce)


def get_dummy_block_header(
    nonce, timestamp=None, block_number=None, prev_block_hash: Optional[Sha256Hash] = None
) -> BlockHeader:
    if timestamp is None:
        timestamp = 5 * nonce
    if block_number is None:
        block_number = 2 * nonce
    if prev_block_hash is None:
        prev_block_hash = helpers.generate_bytes(eth_common_constants.BLOCK_HASH_LEN)
    else:
        prev_block_hash = prev_block_hash.binary
    # create BlockHeader object with dummy values multiplied by nonce to be able generate txs with different value
    return BlockHeader(
        prev_block_hash,
        helpers.generate_bytes(eth_common_constants.BLOCK_HASH_LEN),
        helpers.generate_bytes(eth_common_constants.ADDRESS_LEN),
        helpers.generate_bytes(eth_common_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytes(eth_common_constants.MERKLE_ROOT_LEN),
        helpers.generate_bytes(eth_common_constants.MERKLE_ROOT_LEN),
        100 * nonce,
        nonce,
        block_number,
        3 * nonce,
        4 * nonce,
        timestamp,
        helpers.generate_bytes(100 * nonce),
        helpers.generate_bytes(eth_common_constants.BLOCK_HASH_LEN),
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

# reference
# https://etherscan.io/tx/0x00278cf7120dbbbee72eb7bdaaa2eac8ec41ef931c30fd6d218bdad1b2b2324e

EIP_155_TRANSACTION_HASH = "00278cf7120dbbbee72eb7bdaaa2eac8ec41ef931c30fd6d218bdad1b2b2324e"
EIP_155_TRANSACTION_GAS_PRICE = 53_000_000_000
EIP_155_TRANSACTION_BYTES = bytearray(
    b"\xf9\x01X\xf9\x01U\x82\x01&\x85\x0cW\x0b\xd2\x00\x83\x02\x88Q\x94z%\rV0\xb4\xcfS\x979\xdf"
    b",]\xac\xb4\xc6Y\xf2H\x8d\x89\x01\x15\x8eF\t\x13\xd0\x00\x00\xb8\xe4\x7f\xf3j\xb5\x00\x00\x00"
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf6:\xd7\xb1"
    b"pFm\xe7\xd8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\x00b)a\xe7\xf7k^W=\xf4J\xfd\xebq'I\xbb\xee9\x8d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00^\xe2jy\x00\x00\x00\x00"
    b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xc0*\xaa9\xb2#\xfe\x8d"
    b"\n\x0e\\O'\xea\xd9\x08<ul\xc2\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00k\x17Tt\xe8\x90"
    b"\x94\xc4M\xa9\x8b\x95N\xed\xea\xc4\x95'\x1d\x0f%\xa0\xcf^w\x17\x04*S\xb7a\xda\xd2K\x9ehs\xf2"
    b"\xdak\xb3\x81\xab\x0b\xec\x1a\x1b\xa7\xe1[\xc9$\xb0\xb2\xa0Zmb\x7f\x03E\xf8J\xc6\xfb\xf7\x08"
    b"\xd3\n=\xda\xc8\xe1!\x05\xd4C\n\xb6v\x8d\x81\xa7\xa8\xdbq\x91"
)
EIP_155_TRANSACTIONS_MESSAGE = TransactionsEthProtocolMessage(
    EIP_155_TRANSACTION_BYTES
)


NOT_EIP_155_TRANSACTION_HASH = "f4996a6631c6c685b6c34a30d129917b8d502988feb1d7e930035e207e9761dc"
NOT_EIP_155_TRANSACTION_BYTES = bytearray(
    b"\xf8\xae\xf8\xac\x83\x01\x88\x8c\x85\x077\xbev\x00\x82\xea`\x94\xa1\xb1\x9b\xcdP\xa2K\xe0\xcb9\x9c\x1e\xc0\xf7\xcaTk\x94\xa2\xb0\x80\xb8D\xa9\x05\x9c\xbb\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f0\"q\xd0\xdf\xbdf\xc5\xf7\x8a2\x16.\xeb\xf0\xa8\xb3Q.\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00uI\x1c\xca\x15\x13<\x00\x1b\xa0\x19\x04f\xba\x93\x0f|\xd8\xf4\xf0\xe0\xfcVK\xfbu\xcb+U\xd0\x80\x7f\xe0\xa7C\xe5\x86\xc2\'WQ\x8e\xa0k\xb1\x1a\x08\x99FI>J\xa7U\x9dW\x8d\xa3\x98b\xd8\xd1D\xf2\xb3\xc4\x9a\xfe\x9f\xa3Y\x1b\xab\x05-"
)
NOT_EIP_155_TRANSACTIONS_MESSAGE = TransactionsEthProtocolMessage(
    NOT_EIP_155_TRANSACTION_BYTES
)
