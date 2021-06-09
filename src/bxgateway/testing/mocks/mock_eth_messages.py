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
from bxcommon.messages.eth.serializers.transaction import Transaction, LegacyTransaction, \
    AccessListTransaction
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
        tx_message.tx_hash(), tx_message.tx_val(), source, local_region=True
    )


def generate_eth_raw_transaction_with_to_address(
        source: FeedSource = FeedSource.BDN_SOCKET,
        to_address: str = helpers.generate_bytes(eth_common_constants.ADDRESS_LEN)) -> EthRawTransaction:
    transaction = get_dummy_transaction(1, to_address_str=to_address)
    transactions_eth_message = TransactionsEthProtocolMessage(None, [transaction])
    tx_message = EthNormalMessageConverter().tx_to_bx_txs(transactions_eth_message, 5)[0][0]
    return EthRawTransaction(
        tx_message.tx_hash(), tx_message.tx_val(), source, local_region=True
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
    return LegacyTransaction(
        nonce,
        gas_price,
        3 * nonce,
        to_address,
        4 * nonce,
        helpers.generate_bytes(15 * nonce),
        v,
        6 * nonce,
        7 * nonce
    )


def get_dummy_access_list_transaction(
    nonce: int, gas_price: Optional[int] = None, v: int = 27, to_address_str: Optional[str] = None
) -> Transaction:
    if gas_price is None:
        gas_price = 2 * nonce
    if to_address_str is None:
        to_address = helpers.generate_bytes(eth_common_constants.ADDRESS_LEN)
    else:
        to_address = convert.hex_to_bytes(to_address_str)
    return AccessListTransaction(
        8 * nonce,
        nonce,
        gas_price,
        3 * nonce,
        to_address,
        4 * nonce,
        helpers.generate_bytes(15 * nonce),
        [],
        v,
        6 * nonce,
        7 * nonce
    )


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
        nonce.to_bytes(eth_common_constants.BLOCK_NONCE_LEN, byteorder="big")
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
    header = get_dummy_block_header(nonce, block_number=block_number,
                                    prev_block_hash=prev_block_hash)
    block = get_dummy_block(nonce, header)
    return NewBlockEthProtocolMessage(None, block, nonce * 5 + 10)
