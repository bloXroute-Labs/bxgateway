import asyncio

import websockets

from bxcommon import constants
from bxcommon.rpc.json_rpc_request import JsonRpcRequest
from bxcommon.rpc.rpc_request_type import RpcRequestType

"""
Barebones ws-client implementation. 
Intended as a reference for further work on BX-1675.
https://bloxroute.atlassian.net/browse/BX-1675
"""


async def main():
    ws_uri = f"ws://{constants.LOCALHOST}:28333"
    async with websockets.connect(ws_uri) as ws:
        await ws.send(
            JsonRpcRequest(
                "2", RpcRequestType.SUBSCRIBE, ["unconfirmedTxs", {}]
            ).to_jsons()
        )
        print(await ws.recv())

        while True:
            print(await ws.recv())


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    run_main()
