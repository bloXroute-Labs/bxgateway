import os
import sys

from argparse import Namespace
from dataclasses import dataclass
from typing import Optional, Set, Dict

from bxcommon import constants
from bxcommon.common_opts import CommonOpts
from bxcommon.rpc import rpc_constants
from bxcommon.models.transaction_flag import TransactionFlag
from bxcommon.utils import ip_resolver, node_cache
from bxcommon.utils.blockchain_utils.eth import eth_common_constants
from bxcommon.utils.convert import hex_to_bytes
from bxcommon.models.bdn_account_model_base import BdnAccountModelBase
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.blockchain_peer_info import BlockchainPeerInfo
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxgateway import argument_parsers
from bxgateway import gateway_constants
from bxgateway import log_messages
from bxgateway.utils.eth.eccx import ECCx
from bxutils import logging


logger = logging.get_logger(__name__)


@dataclass
class GatewayOpts(CommonOpts):
    blockchain_port: int
    blockchain_protocol: Optional[str]
    blockchain_network: Optional[str]
    blockchain_networks: Dict[int, BlockchainNetworkModel]
    blockchain_block_recovery_timeout_s: int
    blockchain_block_hold_timeout_s: int
    blockchain_block_interval: int
    blockchain_ignore_block_interval_count: int
    blockchain_ip: str
    peer_gateways: Set[OutboundPeerModel]
    peer_transaction_relays: Set[OutboundPeerModel]
    remote_blockchain_peer: OutboundPeerModel
    min_peer_gateways: int
    remote_blockchain_ip: str
    remote_blockchain_port: int
    connect_to_remote_blockchain: bool
    encrypt_blocks: bool
    peer_relays: Set[OutboundPeerModel]
    test_mode: str
    blockchain_version: int
    blockchain_nonce: int
    blockchain_net_magic: int
    blockchain_services: int
    enable_node_cache: bool
    node_public_key: str
    enode: str
    private_key: str
    network_id: int
    genesis_hash: str
    chain_difficulty: str
    no_discovery: bool
    remote_public_key: str
    blockchain_peers: Set[BlockchainPeerInfo]
    blockchain_peers_file: str
    compact_block: bool
    compact_block_min_tx_count: int
    dump_short_id_mapping_compression: bool
    dump_short_id_mapping_compression_path: str
    tune_send_buffer_size: bool
    max_block_interval_s: int
    cookie_file_path: str
    blockchain_message_ttl: int
    remote_blockchain_message_ttl: int
    stay_alive_duration: int
    initial_liveliness_check: int
    config_update_interval: int
    require_blockchain_connection: bool
    default_tx_flag: TransactionFlag
    should_update_source_version: bool
    account_model: Optional[BdnAccountModelBase]
    process_node_txs_in_extension: bool
    enable_eth_extensions: bool     # TODO remove
    request_recovery: bool
    enable_block_compression: bool
    filter_txs_factor: float
    min_peer_relays_count: int
    should_restart_on_high_memory: bool

    # IPC
    ipc: bool
    ipc_file: str

    # Ontology specific
    http_info_port: int
    consensus_port: int
    relay: bool
    is_consensus: bool

    # transaction feed
    ws: bool
    ws_host: str
    ws_port: int
    eth_ws_uri: Optional[str]
    request_remote_transaction_streaming: bool

    # ENV
    is_docker: bool

    @classmethod
    def opts_defaults(cls, opts) -> Namespace:
        opts = super().opts_defaults(opts)

        if "blockchain_networks" not in opts:
            # node_cache dependencies should be untangled
            #  parameter to call `node_cache.read` but got `Namespace`
            cache_file_info = node_cache.read(opts)
            if cache_file_info is not None:
                opts.blockchain_networks = cache_file_info.blockchain_networks

        opts.outbound_peers = set(opts.peer_gateways).union(opts.peer_relays)

        if opts.connect_to_remote_blockchain and opts.remote_blockchain_ip and opts.remote_blockchain_port:
            opts.remote_blockchain_peer = OutboundPeerModel(opts.remote_blockchain_ip, opts.remote_blockchain_port)
        else:
            opts.remote_blockchain_peer = None

        opts.account_model = None

        opts.is_docker = os.path.exists("/.dockerenv")

        # Request streaming from BDN if ws server is turned on
        opts.request_remote_transaction_streaming = opts.ws

        # set by node runner
        opts.blockchain_block_interval = 0
        opts.blockchain_ignore_block_interval_count = 0
        opts.blockchain_block_recovery_timeout_s = 0
        opts.blockchain_block_hold_timeout_s = 0
        opts.enable_network_content_logs = False
        opts.should_update_source_version = False

        # set after initialization
        opts.peer_transaction_relays = []

        if opts.blockchain_protocol:
            opts.blockchain_protocol = opts.blockchain_protocol.lower()
        else:
            opts.blockchain_protocol = None

        if not opts.blockchain_peers:
            opts.blockchain_peers = set()

        if opts.blockchain_peers_file:
            cls.read_blockchain_peers_from_file(opts)

        if not opts.cookie_file_path:
            opts.cookie_file_path = gateway_constants.COOKIE_FILE_PATH_TEMPLATE.format(
                "{}_{}".format(get_sdn_hostname(opts.sdn_url), opts.external_port)
            )

        opts.min_peer_relays_count = 1
        return opts

    @classmethod
    def read_blockchain_peers_from_file(cls, opts) -> None:
        try:
            with open(opts.blockchain_peers_file, "r", encoding=constants.DEFAULT_TEXT_ENCODING) as peers_file:
                blockchain_peers_list = peers_file.readlines()
                blockchain_peers_list = [peer.strip() for peer in blockchain_peers_list]
                blockchain_peers_list = [peer for peer in blockchain_peers_list if peer]
                for peer in blockchain_peers_list:
                    opts.blockchain_peers.add(argument_parsers.parse_peer(opts.blockchain_protocol, peer))
        except FileNotFoundError:
            logger.fatal(log_messages.BLOCKCHAIN_PEERS_FILE_NOT_FOUND, opts.blockchain_peers_file, exc_info=False)
            sys.exit(1)

    def __post_init__(self):
        if self.filter_txs_factor < 0:
            logger.fatal("--filter_txs_factor cannot be below 0.")
            sys.exit(1)

    def validate_eth_opts(self) -> None:
        if not self.blockchain_ip and not self.blockchain_peers:
            logger.fatal(log_messages.ETH_MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS, exc_info=False)
            sys.exit(1)

        if self.blockchain_ip:
            if self.node_public_key is None:
                logger.fatal(log_messages.ETH_MISSING_NODE_PUBLIC_KEY, exc_info=False)
                sys.exit(1)
            validate_pub_key(self.node_public_key)

        if self.blockchain_peers:
            for blockchain_peer in self.blockchain_peers:
                if blockchain_peer.ip is None:
                    logger.fatal(log_messages.ETH_MISSING_BLOCKCHAIN_IP_FROM_BLOCKCHAIN_PEERS, exc_info=False)
                    sys.exit(1)
                if blockchain_peer.node_public_key is None:
                    logger.fatal(log_messages.ETH_MISSING_NODE_PUBLIC_KEY_FROM_BLOCKCHAIN_PEERS, exc_info=False)
                    sys.exit(1)
                validate_pub_key(blockchain_peer.node_public_key)

        if self.remote_blockchain_peer is not None:
            if self.remote_public_key is None:
                logger.fatal(log_messages.ETH_MISSING_REMOTE_NODE_PUBLIC_KEY, exc_info=False)
                sys.exit(1)
            validate_pub_key(self.remote_public_key)

    def set_account_options(self, account_model: BdnAccountModelBase) -> None:
        super().set_account_options(account_model)
        self.account_model = account_model

        blockchain_protocol = account_model.blockchain_protocol
        blockchain_network = account_model.blockchain_network
        if blockchain_protocol is not None:
            blockchain_protocol = blockchain_protocol.lower()
            if self.blockchain_protocol:
                if self.blockchain_protocol != blockchain_protocol:
                    logger.fatal(log_messages.BLOCKCHAIN_PROTOCOL_AND_ACCOUNT_MISMATCH, exc_info=False)
                    sys.exit(1)
            else:
                self.blockchain_protocol = blockchain_protocol
        if blockchain_network is not None:
            if self.blockchain_network:
                assert self.blockchain_network == blockchain_network
            else:
                self.blockchain_network = blockchain_network

    def validate_network_opts(self) -> None:
        if self.blockchain_network is None:
            self.blockchain_network = "mainnet"

        blockchain_protocol = self.blockchain_protocol

        if blockchain_protocol is None:
            logger.fatal(log_messages.MISSING_BLOCKCHAIN_PROTOCOL)
            sys.exit(1)

        if blockchain_protocol == BlockchainProtocol.ETHEREUM:
            self.validate_eth_opts()

        if not self.blockchain_ip and not self.blockchain_peers:
            logger.fatal(log_messages.MISSING_BLOCKCHAIN_IP_AND_BLOCKCHAIN_PEERS, exc_info=False)
            sys.exit(1)

        if self.blockchain_ip:
            self.blockchain_ip = validate_blockchain_ip(self.blockchain_ip, self.is_docker)
        if self.blockchain_peers:
            for blockchain_peer in self.blockchain_peers:
                blockchain_peer.ip = validate_blockchain_ip(blockchain_peer.ip, self.is_docker)
        if self.rpc_host == rpc_constants.DEFAULT_RPC_HOST and self.is_docker:
            docker_host = get_docker_host()
            if docker_host:
                self.rpc_host = docker_host
            else:
                self.rpc_host = rpc_constants.DEFAULT_RPC_HOST


def get_sdn_hostname(sdn_url: str) -> str:
    new_sdn_url = sdn_url
    if "://" in sdn_url:
        new_sdn_url = sdn_url.split("://")[1]

    return new_sdn_url


def get_docker_host() -> Optional[str]:
    with open('/etc/hosts', "r") as f:
        host_list = f.readlines()
        for host in host_list:
            if "172" in host:
                end_idx = 0
                for char in host:
                    if not char.isnumeric() and char != '.':
                        return host[:end_idx]
                    end_idx += 1


def validate_pub_key(key) -> None:
    if key.startswith("0x"):
        key = key[2:]
    if len(key) != 2 * eth_common_constants.PUBLIC_KEY_LEN:
        logger.fatal(log_messages.INVALID_PUBLIC_KEY_LENGTH,
                     len(key), exc_info=False)
        sys.exit(1)
    eccx_obj = ECCx()
    if not eccx_obj.is_valid_key(hex_to_bytes(key)):
        logger.fatal(log_messages.INVALID_PUBLIC_KEY, exc_info=False)
        sys.exit(1)


def validate_blockchain_ip(blockchain_ip, is_docker=False) -> str:
    if blockchain_ip is None:
        logger.fatal(log_messages.MISSING_BLOCKCHAIN_IP_FROM_BLOCKCHAIN_PEERS, exc_info=False)
        sys.exit(1)

    if blockchain_ip == gateway_constants.LOCALHOST and is_docker:
        logger.warning(log_messages.INVALID_BLOCKCHAIN_IP, exc_info=False)
    try:
        return ip_resolver.blocking_resolve_ip(blockchain_ip)
    except EnvironmentError:
        logger.fatal(log_messages.BLOCKCHAIN_IP_RESOLVE_ERROR, blockchain_ip)
        sys.exit(1)

