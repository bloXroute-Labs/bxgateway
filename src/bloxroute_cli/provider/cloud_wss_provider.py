import os
import ssl
import sys
import urllib.request
from ssl import Purpose
from typing import Optional
from os import path
from pathlib import Path

import websockets

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.models.node_type import NodeType
from bxcommon import constants
from bxutils import constants as utils_constants

CLOUD_WEBSOCKETS_URL = "wss://eth.feed.blxrbdn.com:28333"
CA_CERT_URL = "https://certificates.blxrbdn.com/ca/ca_cert.pem"
DEFAULT_NODE_NAME = NodeType.EXTERNAL_GATEWAY.name.lower()


def _get_default_data_path():
    home_path = str(Path.home())
    if sys.platform == constants.PLATFORM_MAC:
        return path.join(home_path, "Library", "bloXroute")
    else:
        return path.join(home_path, "bloxroute")


class CloudWssProvider(WsProvider):
    def __init__(
        self,
        *,
        ssl_dir: Optional[str] = None,
        ssl_certificate_location: Optional[str] = None,
        ssl_key_location: Optional[str] = None,
        ca_file: Optional[str] = None,
        ca_url: str = CA_CERT_URL,
        ws_uri: str = CLOUD_WEBSOCKETS_URL,
        node_type: str = DEFAULT_NODE_NAME
    ):
        if ssl_dir is None:
            ssl_dir = os.path.join(
                _get_default_data_path(),
                utils_constants.SSL_FOLDER,
                node_type,
                "registration_only"
            )
        if ssl_certificate_location is None:
            ssl_certificate_location = os.path.join(
                ssl_dir, f"{node_type}_cert.pem"
            )
        if ssl_key_location is None:
            ssl_key_location = os.path.join(
                ssl_dir, f"{node_type}_key.pem"
            )
        if ca_file is None:
            with urllib.request.urlopen(ca_url) as cert_file:
                contents = cert_file.read().decode("utf-8")
                context = ssl.create_default_context(
                    Purpose.SERVER_AUTH,
                    cadata=contents,
                )
        else:
            context = ssl.create_default_context(
                Purpose.SERVER_AUTH,
                cafile=ca_file,
            )

        super().__init__(ws_uri)
        context.load_cert_chain(
            ssl_certificate_location, ssl_key_location
        )
        context.check_hostname = False
        self.ssl_context = context

    async def connect_websocket(self) -> websockets.WebSocketClientProtocol:
        return await websockets.connect(self.uri, ssl=self.ssl_context)
