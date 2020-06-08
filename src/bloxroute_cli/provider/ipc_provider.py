import asyncio
import websockets

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.utils import config


class IpcProvider(WsProvider):
    """
    Provider that connects to bxgateway's websocket RPC endpoint.
    """

    def __init__(self, ipc_file: str):
        super().__init__(ipc_file)
        self.ipc_path: str = config.get_data_file(ipc_file)

    async def initialize(self) -> None:
        self.ws = await websockets.unix_connect(self.ipc_path)
        self.listener = asyncio.create_task(self.receive())
        self.running = True
