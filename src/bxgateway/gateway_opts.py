from dataclasses import dataclass
from bxcommon.utils.cli import CommonOpts
from bxcommon.utils import ip_resolver
from typing import Union, List
from bxcommon.models.blockchain_network_model import BlockchainNetworkModel
from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.models.outbound_peer_model import OutboundPeerModel
from bxcommon.models.quota_type_model import QuotaType
from bxcommon.utils.convert import hex_to_bytes
from argparse import Namespace
from bxcommon.utils import node_cache
from bxgateway import gateway_constants
from bxgateway import eth_constants
from bxgateway.utils.eth.eccx import ECCx
from bxutils import logging
import os

logger = logging.get_logger(__name__)


@dataclass()
class GatewayOpts(CommonOpts):
    blockchain_port: int
    blockchain_protocol: str
    blockchain_network: Union[str, List[BlockchainNetworkModel]]
    blockchain_ip: str
    peer_gateways: List[OutboundPeerModel]
    min_peer_gateways: int
    remote_blockchain_ip: str
    remote_blockchain_port: int
    connect_to_remote_blockchain: bool
    encrypt_blocks: bool
    peer_relays: List[OutboundPeerModel]
    test_mode: str
    sync_tx_service: bool
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
    compact_block: bool
    compact_block_min_tx_count: int
    dump_short_id_mapping_compression: bool
    dump_short_id_mapping_compression_path: str
    tune_send_buffer_size: bool
    max_block_interval: int
    cookie_file_path: str
    blockchain_message_ttl: int
    remote_blockchain_message_ttl: int
    stay_alive_duration: int
    initial_liveliness_check: int
    config_update_interval: int
    require_blockchain_connection: bool
    rpc_port: int
    rpc_host: str
    rpc_user: str
    rpc_password: str
    default_tx_quota_type: QuotaType
    # Ontology specific
    http_info_port: int
    consensus_port: int
    relay: bool
    is_consensus: bool

    # ENV
    is_docker: bool

    def __init__(self, opts: Namespace):

        super().__init__(opts)

        if not opts.blockchain_network:
            cache_file_info = node_cache.read(opts)
            if cache_file_info is not None:
                self.blockchain_network = cache_file_info.blockchain_network
            else:
                self.blockchain_network = "mainnet"
        else:
            self.blockchain_network = opts.blockchain_network
        self.outbound_peers = opts.peer_gateways + opts.peer_relays

        if opts.connect_to_remote_blockchain and opts.remote_blockchain_ip and opts.remote_blockchain_port:
            self.remote_blockchain_peer = OutboundPeerModel(opts.remote_blockchain_ip, opts.remote_blockchain_port)
        else:
            self.remote_blockchain_peer = None

        self.blockchain_port = opts.blockchain_port
        self.blockchain_protocol = opts.blockchain_protocol.lower()
        self.blockchain_ip = opts.blockchain_ip
        self.peer_gateways = opts.peer_gateways
        self.min_peer_gateways = opts.min_peer_gateways
        self.remote_blockchain_ip = opts.remote_blockchain_ip
        self.remote_blockchain_port = opts.remote_blockchain_port
        self.connect_to_remote_blockchain = opts.connect_to_remote_blockchain
        self.encrypt_blocks = opts.encrypt_blocks
        self.peer_relays = opts.peer_relays
        self.test_mode = opts.test_mode
        self.sync_tx_service = opts.sync_tx_service
        self.blockchain_version = opts.blockchain_version
        self.blockchain_nonce = opts.blockchain_nonce
        self.blockchain_net_magic = opts.blockchain_net_magic
        self.blockchain_services = opts.blockchain_services
        self.enable_node_cache = opts.enable_node_cache
        self.node_public_key = opts.node_public_key
        self.enode = opts.enode
        self.private_key = opts.private_key
        self.network_id = opts.network_id
        self.genesis_hash = opts.genesis_hash
        self.chain_difficulty = opts.chain_difficulty
        self.no_discovery = opts.no_discovery
        self.remote_public_key = opts.remote_public_key
        self.compact_block = opts.compact_block
        self.compact_block_min_tx_count = opts.compact_block_min_tx_count
        self.dump_short_id_mapping_compression = opts.dump_short_id_mapping_compression
        self.dump_short_id_mapping_compression_path = opts.dump_short_id_mapping_compression_path
        self.tune_send_buffer_size = opts.tune_send_buffer_size
        self.max_block_interval = opts.max_block_interval
        self.cookie_file_path = opts.cookie_file_path
        self.blockchain_message_ttl = opts.blockchain_message_ttl
        self.remote_blockchain_message_ttl = opts.remote_blockchain_message_ttl
        self.stay_alive_duration = opts.stay_alive_duration
        self.initial_liveliness_check = opts.initial_liveliness_check
        self.config_update_interval = opts.config_update_interval
        self.require_blockchain_connection = opts.require_blockchain_connection
        self.rpc_port = opts.rpc_port
        self.rpc_host = opts.rpc_host
        self.rpc_user = opts.rpc_user
        self.rpc_password = opts.rpc_password
        self.default_tx_quota_type = opts.default_tx_quota_type
        # Ontology specific
        self.http_info_port = opts.http_info_port
        self.consensus_port = opts.consensus_port
        self.relay = opts.relay
        self.is_consensus = opts.is_consensus

        self.is_docker = os.path.exists("/.dockerenv")

        # do rest of validation
        if self.blockchain_protocol == BlockchainProtocol.ETHEREUM.value:
            self.validate_eth_opts()
        if not self.cookie_file_path:
            self.cookie_file_path = gateway_constants.COOKIE_FILE_PATH_TEMPLATE.format(
                "{}_{}".format(get_sdn_hostname(opts.sdn_url), opts.external_ip))
        self.validate_blockchain_ip()

    def validate_eth_opts(self):
        if self.enode is not None and isinstance(self.enode, str):
            self.parse_enode()
        if self.blockchain_ip is None:
            logger.fatal("Either --blockchain-ip or --enode arguments are required.", exc_info=False)
            exit(1)
        if self.node_public_key is None:
            logger.fatal("--node-public-key argument is required but not specified.", exc_info=False)
            exit(1)
        validate_pub_key(self.node_public_key)

        if self.remote_blockchain_peer is not None:
            if self.remote_public_key is None:
                logger.fatal(
                    "--remote-public-key of the blockchain node must be included with command-line specified remote "
                    "blockchain peer. Use --remote-public-key",
                    exc_info=False)
                exit(1)
            validate_pub_key(self.remote_public_key)

    def parse_enode(self):
        # Make sure enode is at least as long as the public key
        if len(self.enode) < 2 * eth_constants.PUBLIC_KEY_LEN:
            logger.fatal("Invalid enode. "
                         "Invalid enode length: {}", len(self.enode), exc_info=False)
            exit(1)
        try:
            enode_and_pub_key, ip_and_port = self.enode.split("@")
            if enode_and_pub_key.startswith("enode://"):
                pub_key = enode_and_pub_key[8:]
            else:
                pub_key = enode_and_pub_key
            ip, port_and_disc = ip_and_port.split(":")
            port = port_and_disc.split("?")[0]
        except ValueError:
            logger.fatal("Invalid enode: {}", self.enode, exc_info=False)
            exit(1)
        else:
            # Node public key gets validated in validate_eth_opts
            self.node_public_key = pub_key
            # blockchain IP gets validated in __init__()
            self.blockchain_ip = ip
            # Port validation
            if not port.isnumeric():
                logger.fatal("Invalid port: {}", port, exc_info=False)
                exit(1)
            self.blockchain_port = int(port)

    def validate_blockchain_ip(self):
        if self.blockchain_ip is None:
            logger.fatal("--blockchain-ip is required but not specified.", exc_info=False)
            exit(1)
        if self.blockchain_ip == gateway_constants.LOCALHOST and self.is_docker:
            logger.fatal("The specified blockchain IP is localhost, which is not compatible with a dockerized "
                         "gateway. Did you mean 172.17.0.X?", exc_info=False)
            exit(1)
        try:
            self.blockchain_ip = ip_resolver.blocking_resolve_ip(self.blockchain_ip)
        except EnvironmentError:
            logger.fatal("Blockchain IP could not be resolved, exiting. Blockchain IP: {}", self.blockchain_ip)
            exit(1)


def get_sdn_hostname(sdn_url: str) -> str:
    new_sdn_url = sdn_url
    if "://" in sdn_url:
        new_sdn_url = sdn_url.split("://")[1]

    return new_sdn_url


def validate_pub_key(key):
    if key.startswith("0x"):
        key = key[2:]
    if len(key) != 2 * eth_constants.PUBLIC_KEY_LEN:
        logger.fatal("Public key must be the 128 digit key associated with the blockchain enode. "
                     "Invalid key length: {}", len(key), exc_info=False)
        exit(1)
    eccx_obj = ECCx()
    if not eccx_obj.is_valid_key(hex_to_bytes(key)):
        logger.fatal("Public key must be constructed from a valid private key.", exc_info=False)
        exit(1)

