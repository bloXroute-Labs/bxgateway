from argparse import Namespace
from typing import cast

from bxcommon import constants
from bxcommon.connections.abstract_connection import AbstractConnection
from bxcommon.models.node_type import NodeType
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.test_utils.helpers import COOKIE_FILE_PATH, get_common_opts, \
    BTC_COMPACT_BLOCK_DECOMPRESS_MIN_TX_COUNT
from bxgateway.connections.abstract_gateway_blockchain_connection import AbstractGatewayBlockchainConnection
from bxgateway.connections.abstract_gateway_node import AbstractGatewayNode
from bxgateway import argument_parsers
from bxgateway.gateway_opts import GatewayOpts


# pylint: disable=unused-argument,too-many-branches
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo


def get_gateway_opts(
    port,
    node_id=None,
    external_ip=constants.LOCALHOST,
    blockchain_address=None,
    test_mode=None,
    peer_gateways=None,
    peer_relays=None,
    peer_transaction_relays=None,
    protocol_version=1,
    bloxroute_version="bloxroute 1.5",
    pub_key=None,
    include_default_ont_args=False,
    blockchain_network_num=constants.DEFAULT_NETWORK_NUM,
    min_peer_gateways=0,
    remote_blockchain_ip=None,
    remote_blockchain_port=None,
    connect_to_remote_blockchain=False,
    blockchain_peers=None,
    blockchain_peers_file=None,
    is_internal_gateway=False,
    is_gateway_miner=False,
    enable_buffered_send=False,
    encrypt_blocks=True,
    cookie_file_path=COOKIE_FILE_PATH,
    blockchain_block_hold_timeout_s=30,
    blockchain_block_recovery_timeout_s=30,
    stay_alive_duration=30 * 60,
    source_version="v1.1.1.1",
    initial_liveliness_check=30,
    block_interval=600,
    non_ssl_port: int = 9001,
    has_fully_updated_tx_service: bool = False,
    max_block_interval_s: float = 10,
    default_tx_flag: TransactionFlag = TransactionFlag.NO_FLAGS,
    log_level_overrides=None,
    enable_network_content_logs=False,
    account_model=None,
    ipc=False,
    ipc_file="bxgateway.ipc",
    ws=False,
    ws_host=constants.LOCALHOST,
    ws_port=28333,
    request_remote_transaction_streaming: bool = False,
    enable_block_compression: bool = True,
    filter_txs_factor: float = 0,
    blockchain_protocol: str = "Ethereum",
    should_restart_on_high_memory: bool = False,
    account_id: str = constants.DECODED_EMPTY_ACCOUNT_ID,
    **kwargs,
) -> GatewayOpts:
    if node_id is None:
        node_id = "Gateway at {0}".format(port)
    if peer_gateways is None:
        peer_gateways = set()
    if peer_relays is None:
        peer_relays = set()
    if peer_transaction_relays is None:
        peer_transaction_relays = []
    if blockchain_address is None:
        blockchain_address = ("127.0.0.1", 7000)  # not real, just a placeholder
    if test_mode is None:
        test_mode = []
    if log_level_overrides is None:
        log_level_overrides = {}
    if remote_blockchain_ip is not None and remote_blockchain_port is not None:
        remote_blockchain_peer = (remote_blockchain_ip, remote_blockchain_port)
    else:
        remote_blockchain_peer = None
    # below is the same as `class ParseBlockchainPeers(argparse.Action)`
    if blockchain_peers is not None:
        blockchain_peers_set = set()
        blockchain_protocol = blockchain_protocol
        for peer in blockchain_peers.split(","):
            blockchain_peer = argument_parsers.parse_peer(blockchain_protocol, peer) # from bxgateway import argument_parsers,
            blockchain_peers_set.add(blockchain_peer)
        blockchain_peers = blockchain_peers_set

    partial_apply_args = locals().copy()
    for kwarg, arg in partial_apply_args["kwargs"].items():
        partial_apply_args[kwarg] = arg

    partial_apply_args["outbound_peers"] = set(peer_gateways).union(peer_relays)

    opts = Namespace()
    common_opts = get_common_opts(**partial_apply_args)
    opts.__dict__.update(common_opts.__dict__)

    opts.__dict__.update(
        {
            "bloxroute_version": bloxroute_version,
            "blockchain_ip": blockchain_address[0],
            "blockchain_port": blockchain_address[1],
            "blockchain_protocol": blockchain_protocol,
            "blockchain_network": "Mainnet",
            "blockchain_version": 12345,
            "test_mode": test_mode,
            "peer_gateways": peer_gateways,
            "peer_relays": peer_relays,
            "peer_transaction_relays": peer_transaction_relays,
            "protocol_version": protocol_version,
            "min_peer_gateways": min_peer_gateways,
            "remote_blockchain_ip": remote_blockchain_ip,
            "remote_blockchain_port": remote_blockchain_port,
            "remote_blockchain_peer": remote_blockchain_peer,
            "connect_to_remote_blockchain": connect_to_remote_blockchain,
            "blockchain_peers": blockchain_peers,
            "blockchain_peers_file": blockchain_peers_file,
            "is_internal_gateway": is_internal_gateway,
            "is_gateway_miner": is_gateway_miner,
            "encrypt_blocks": encrypt_blocks,
            "enable_buffered_send": enable_buffered_send,
            "compact_block": True,
            "compact_block_min_tx_count": BTC_COMPACT_BLOCK_DECOMPRESS_MIN_TX_COUNT,
            "tune_send_buffer_size": False,
            "dump_short_id_mapping_compression": False,
            "max_block_interval_s": max_block_interval_s,
            "cookie_file_path": cookie_file_path,
            "config_update_interval": 60,
            "blockchain_message_ttl": 10,
            "remote_blockchain_message_ttl": 10,
            "stay_alive_duration": stay_alive_duration,
            "initial_liveliness_check": initial_liveliness_check,
            "has_fully_updated_tx_service": has_fully_updated_tx_service,
            "source_version": source_version,
            "require_blockchain_connection": True,
            "non_ssl_port": non_ssl_port,
            "default_tx_flag": default_tx_flag,
            "should_update_source_version": False,
            "enable_network_content_logs": False,
            "enable_node_cache": True,
            "dump_short_id_mapping_compression_path": "",
            "ws": ws,
            "ws_host": constants.LOCALHOST,
            "ws_port": 28333,
            "account_id": account_id,
            "ipc": False,
            "ipc_file": "bxgateway.ipc",
            "request_remote_transaction_streaming": request_remote_transaction_streaming,
            "process_node_txs_in_extension": True,
            "enable_eth_extensions": True,   # TODO remove,
            "request_recovery": True,
            "enable_block_compression": enable_block_compression,
            "filter_txs_factor": filter_txs_factor,
            "min_peer_relays_count": None,
            "should_restart_on_high_memory": should_restart_on_high_memory,
        }
    )

    # bitcoin
    opts.__dict__.update(
        {
            "blockchain_net_magic": 12345,
            "blockchain_version": 23456,
            "blockchain_nonce": 0,
            "blockchain_services": 1,
        }
    )
    # ethereum
    opts.__dict__.update(
        {
            "private_key": "294549f8629f0eeb2b8e01aca491f701f5386a9662403b485c4efe7d447dfba3",
            "node_public_key": pub_key,
            "remote_public_key": pub_key,
            "network_id": 1,
            "chain_difficulty": 4194304,
            "genesis_hash": "1e8ff5fd9d06ab673db775cf5c72a6b2d63171cd26fe1e6a8b9d2d696049c781",
            "no_discovery": True,
            "enode": "enode://294549f8629f0eeb2b8e01aca491f701f5386a9662403b485c4efe7d447dfba3@127.0.0.1:8000",
            "eth_ws_uri": None
        }
    )
    # ontology
    opts.__dict__.update(
        {
            "blockchain_net_magic": 12345,
            "blockchain_version": 23456,
            "is_consensus": True,
            "sync_port": 10001,
            "http_info_port": 10002,
            "consensus_port": 10003,
            "cap": bytes(32),
            "blockchain_nonce": 0,
            "relay": True,
            "soft_version": "myversion",
            "blockchain_services": 1,
        }
    )
    for key, val in kwargs.items():
        opts.__dict__[key] = val

    gateway_opts = GatewayOpts.from_opts(opts)
    if account_model:
        gateway_opts.set_account_options(account_model)

    # some attributes are usually set by the node runner
    gateway_opts.__dict__.update({
        "node_type": NodeType.EXTERNAL_GATEWAY,
        "outbound_peers": common_opts.outbound_peers,
        "sid_expire_time": common_opts.sid_expire_time,
        "blockchain_networks": common_opts.blockchain_networks,
        "blockchain_network_num": common_opts.blockchain_network_num,
        "split_relays": common_opts.split_relays,
        "should_update_source_version": False,
        "blockchain_block_interval": block_interval,
        "blockchain_ignore_block_interval_count": 3,
        "blockchain_block_recovery_timeout_s": blockchain_block_recovery_timeout_s,
        "blockchain_block_hold_timeout_s": blockchain_block_hold_timeout_s,
    })
    return gateway_opts


def add_blockchain_peer(node: AbstractGatewayNode, connection: AbstractConnection):
    node_conn = cast(AbstractGatewayBlockchainConnection, connection)
    node.blockchain_peers.add(BlockchainPeerInfo(node_conn.peer_ip, node_conn.peer_port))
    block_queuing_service = node.build_block_queuing_service(node_conn)
    node.block_queuing_service_manager.add_block_queuing_service(node_conn, block_queuing_service)
