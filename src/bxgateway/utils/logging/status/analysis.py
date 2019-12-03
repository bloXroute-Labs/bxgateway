from dataclasses import dataclass
from typing import List

from bxgateway.utils.logging.status.environment import Environment
from bxgateway.utils.logging.status.extension_modules_state import ExtensionModulesState
from bxgateway.utils.logging.status.network import Network


@dataclass
class Analysis:
    time_started: str
    startup_parameters: str
    gateway_version: str
    extensions_check: ExtensionModulesState
    environment: Environment
    network: Network
    installed_python_packages: List[str]
