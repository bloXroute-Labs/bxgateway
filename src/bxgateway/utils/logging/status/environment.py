from dataclasses import dataclass
from typing import Optional

from bxgateway.utils.logging.status.installation_type import InstallationType


@dataclass
class Environment:
    installation_type: Optional[InstallationType] = None
    platform: Optional[str] = None
    python_version: Optional[str] = None
    python_path: Optional[str] = None
