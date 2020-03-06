import json
import os
import platform
import sys
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import List, Optional

from bxcommon.connections.connection_pool import ConnectionPool
from bxcommon.connections.connection_type import ConnectionType
from bxcommon.constants import OS_VERSION
from bxcommon.utils import config
from bxcommon.utils import model_loader
from bxgateway.utils.logging.status.analysis import Analysis
from bxgateway.utils.logging.status.diagnostics import Diagnostics
from bxgateway.utils.logging.status.environment import Environment
from bxgateway.utils.logging.status.extension_modules_state import ExtensionModulesState
from bxgateway.utils.logging.status.gateway_status import GatewayStatus
from bxgateway.utils.logging.status.installation_type import InstallationType
from bxgateway.utils.logging.status.network import Network
from bxgateway.utils.logging.status.summary import Summary
from bxutils import logging
from bxutils.encoding.json_encoder import EnhancedJSONEncoder
from bxgateway.utils.logging.status import summary as summary_status

logger = logging.get_logger(__name__)

STATUS_FILE_NAME = "gateway_status.log"
CONN_TYPES = {ConnectionType.RELAY_BLOCK, ConnectionType.RELAY_TRANSACTION, ConnectionType.BLOCKCHAIN_NODE,
              ConnectionType.REMOTE_BLOCKCHAIN_NODE}


def initialize(use_ext: bool, src_ver: str, ip_address: str, continent: str, country: str,
               update_required: bool, account_id: Optional[str]) -> Diagnostics:
    current_time = _get_current_time()
    summary = Summary(gateway_status=GatewayStatus.OFFLINE, ip_address=ip_address, continent=continent, country=country,
                      update_required=update_required,
                      account_info=summary_status.gateway_status_get_account_info(account_id))
    environment = Environment(_get_installation_type(), OS_VERSION, platform.python_version(), sys.executable)
    network = Network([], [], [], [])
    analysis = Analysis(current_time, _get_startup_param(), src_ver, _check_extensions_validity(use_ext, src_ver),
                        environment, network, _get_installed_python_modules())
    diagnostics = Diagnostics(summary, analysis)

    _save_status_to_file(diagnostics)
    return diagnostics


def get_diagnostics(use_ext: bool, src_ver: str, ip_address: str, continent: str, country: str,
                    update_required: bool, account_id: Optional[str]) -> Diagnostics:
    return _load_status_from_file(use_ext, src_ver, ip_address, continent, country, update_required, account_id)


def update(conn_pool: ConnectionPool, use_ext: bool, src_ver: str, ip_address: str, continent: str, country: str,
           update_required: bool, account_id: Optional[str]) -> Diagnostics:
    path = config.get_data_file(STATUS_FILE_NAME)
    if not os.path.exists(path):
        initialize(use_ext, src_ver, ip_address, continent, country, update_required, account_id)
    diagnostics = _load_status_from_file(use_ext, src_ver, ip_address, continent, country, update_required, account_id)
    analysis = diagnostics.analysis
    network = analysis.network

    network.clear()

    for conn_type in CONN_TYPES:
        for conn in conn_pool.get_by_connection_type(conn_type):
            network.add_connection(conn.CONNECTION_TYPE, conn.peer_desc, conn.file_no, conn.peer_id)

    summary = network.get_summary(ip_address, continent, country, update_required, account_id)
    diagnostics = Diagnostics(summary, analysis)

    _save_status_to_file(diagnostics)
    return diagnostics


def update_alarm_callback(conn_pool: ConnectionPool, use_ext: bool, src_ver: str, ip_address: str, continent: str,
                          country: str, update_required: bool, account_id: Optional[str]) -> None:
    update(conn_pool, use_ext, src_ver, ip_address, continent, country, update_required, account_id)


def _check_extensions_validity(use_ext: bool, src_ver: str) -> ExtensionModulesState:
    if use_ext:
        try:
            import task_pool_executor as tpe  # pyre-ignore
        except ImportError:
            return ExtensionModulesState.UNAVAILABLE
        extensions_version = tpe.__version__
        if src_ver == extensions_version:
            return ExtensionModulesState.OK
        else:
            return ExtensionModulesState.INVALID_VERSION
    return ExtensionModulesState.UNAVAILABLE


def _get_current_time() -> str:
    return "UTC " + str(datetime.utcnow())


def _get_installation_type() -> InstallationType:
    if os.path.exists("/.dockerenv"):
        return InstallationType.DOCKER
    else:
        return InstallationType.PYPI


def _get_installed_python_modules() -> List[str]:
    installed_packages = []
    for name, module in sorted(sys.modules.items()):
        if hasattr(module, "__version__"):
            installed_packages.append(f"{name}=={module.__version__}")  # pyre-ignore
        else:
            installed_packages.append(name)
    return installed_packages


def _get_startup_param() -> str:
    return " ".join(sys.argv[1:])


def _load_status_from_file(use_ext: bool, src_ver: str, ip_address: str, continent: str, country: str,
                           update_required: bool, account_id: Optional[str]) -> Diagnostics:
    path = config.get_data_file(STATUS_FILE_NAME)
    with open(path, "r", encoding="utf-8") as json_file:
        status_file = json_file.read()
    try:
        model_dict = json.loads(status_file)
        diagnostics = model_loader.load_model(Diagnostics, model_dict)
    except JSONDecodeError:
        logger.warning("The status file may have been modified. This message can be safely ignored when "
                       "running multiple nodes on the same machine. "
                       f"For more information, please check the status file: {path}")
        diagnostics = initialize(use_ext, src_ver, ip_address, continent, country, update_required, account_id)
    return diagnostics


def _save_status_to_file(diagnostics: Diagnostics) -> None:
    path = config.get_data_file(STATUS_FILE_NAME)
    with open(path, "w", encoding="utf-8") as outfile:
        json.dump(diagnostics, outfile, cls=EnhancedJSONEncoder, indent=2)
