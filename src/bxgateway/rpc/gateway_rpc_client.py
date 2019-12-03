import asyncio
import uuid
import json
import sys
import shlex
import os
from asyncio import StreamReader, StreamReaderProtocol, StreamWriter, CancelledError
from asyncio.streams import FlowControlMixin
from argparse import ArgumentParser, Namespace
from typing import Optional, Union, Dict, List, Any

from aiohttp import ClientSession, ClientResponse, ClientConnectorError

from bxcommon.utils import config

from bxgateway.rpc.blxr_transaction_rpc_request import BlxrTransactionRpcRequest
from bxgateway.rpc.gateway_status_rpc_request import GatewayStatusRpcRequest
from bxgateway.rpc.rpc_request_handler import RPCRequestHandler
from bxgateway.rpc.rpc_request_type import RpcRequestType
from bxgateway.utils.gateway_start_args import GatewayStartArgs


class GatewayRpcClient:

    def __init__(self, rpc_host: str, rpc_port: int):
        self._session = ClientSession()
        self._rpc_url = f"http://{rpc_host}:{rpc_port}/"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def make_request(
            self,
            method: RpcRequestType,
            request_id: Optional[str] = None,
            request_params: Union[Dict[str, Any], List[Any], None] = None
    ) -> ClientResponse:
        if request_id is None:
            request_id = str(uuid.uuid4())
        json_data = {
            "method": method.name.lower(),
            "id": request_id,
            "params": request_params
        }
        return await self._session.post(
            self._rpc_url,
            data=json.dumps(json_data),
            headers={RPCRequestHandler.CONTENT_TYPE: RPCRequestHandler.PLAIN}
        )

    async def close(self) -> None:
        await self._session.close()


def merge_params(opts: Namespace, unrecognized_params: List[str]) -> Namespace:
    merged_opts = Namespace()
    merged_opts.__dict__ = opts.__dict__.copy()
    if merged_opts.request_params is None and unrecognized_params:
        if merged_opts.method == RpcRequestType.BLXR_TX:
            merged_opts.request_params = {BlxrTransactionRpcRequest.TRANSACTION: unrecognized_params[0]}
        elif merged_opts.method == RpcRequestType.GATEWAY_STATUS:
            merged_opts.request_params = {GatewayStatusRpcRequest.DETAILS_LEVEL: unrecognized_params[0]}
    return merged_opts


async def handle_command(opts: Namespace, client: GatewayRpcClient, stdout_writer: StreamWriter) -> None:
    if opts.debug:
        stdout_writer.write(f"executing with opts: {opts}\n".encode("utf-8"))
    response: ClientResponse = await client.make_request(opts.method, opts.request_id, opts.request_params)
    response_json = await response.json(content_type=RPCRequestHandler.PLAIN)
    stdout_writer.write(json.dumps(response_json, indent=4, sort_keys=True).encode("utf-8"))
    stdout_writer.write(b"\n")
    await stdout_writer.drain()
    await asyncio.sleep(0)


async def run_cli(
        rpc_host: str,
        rpc_port: int,
        arg_parser: ArgumentParser,
        stdin_reader: StreamReader,
        stdout_writer: StreamWriter
) -> None:
    async with GatewayRpcClient(rpc_host, rpc_port) as client:
        while True:
            stdout_writer.write(b">> ")
            await stdout_writer.drain()
            line = (await stdin_reader.readline()).decode("utf-8")
            if line == "\n":
                await asyncio.sleep(0)
                continue
            if "exit" in line.lower():
                break
            shell_args = shlex.split(line)
            if "-h" in shell_args or "--help" in shell_args or "help" in shell_args:
                arg_parser.print_help()
                continue
            try:
                opts, params = arg_parser.parse_known_args(shlex.split(line))
            except KeyError as unrecognized_method:
                stdout_writer.write(f"unrecognized_method {unrecognized_method} entered, ignoring!\n".encode("utf-8"))
                await stdout_writer.drain()
                continue

            opts = merge_params(opts, params)
            await handle_command(opts, client, stdout_writer)


def add_run_arguments(arg_parser: ArgumentParser) -> None:
    arg_parser.add_argument(
        "method",
        help="The BDN RPC method (valid values: {}).".format(
            [method.lower() for method in RpcRequestType.__members__.keys()]
        ),
        type=lambda t: RpcRequestType[t.upper()],
        default=RpcRequestType.GATEWAY_STATUS
    )
    arg_parser.add_argument(
        "--request-id",
        help="The BDN RPC request unique identifier (default: {}).".format(None),
        type=str,
        default=None
    )
    arg_parser.add_argument(
        "--request-params",
        help="The BDN RPC request params (default: {}).".format(None),
        type=json.loads,
        default=None
    )


def add_base_arguments(arg_parser: ArgumentParser) -> None:
    default_rpc_host = config.get_env_default(GatewayStartArgs.GATEWAY_RPC_HOST)
    arg_parser.add_argument(
        "--rpc-host",
        help="The BDN RPC host (default: {}).".format(default_rpc_host),
        type=str,
        default=default_rpc_host
    )
    default_rpc_port = config.get_env_default(GatewayStartArgs.GATEWAY_RPC_PORT)
    arg_parser.add_argument(
        "--rpc-port",
        help="The BDN RPC port (default: {}).".format(default_rpc_port),
        type=int,
        default=default_rpc_port
    )
    arg_parser.add_argument(
        "--interactive-shell",
        help="Run the BDN RPC client in CLI mode (default: False)",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--debug",
        help="Run the BDN RPC client in debug mode (default: False)",
        action="store_true",
        default=False
    )


async def main():
    arg_parser = ArgumentParser(prog="BDN RPC client")
    cli_parser = ArgumentParser(prog="BDN RPC client")
    add_base_arguments(arg_parser)
    add_base_arguments(cli_parser)
    add_run_arguments(cli_parser)
    args = sys.argv[1:]
    if "-h" in args or "--help" in args:
        cli_parser.print_help()
        arg_parser.exit()

    opts, params = arg_parser.parse_known_args()
    stdin_reader = StreamReader()
    cli_loop = asyncio.get_event_loop()
    await cli_loop.connect_read_pipe(
        lambda: StreamReaderProtocol(stdin_reader),
        sys.stdin
    )
    transport, protocol = await cli_loop.connect_write_pipe(
        lambda: FlowControlMixin(), os.fdopen(sys.stdout.fileno(), "wb")
    )
    stdout_writer = StreamWriter(transport, protocol, None, cli_loop)
    try:
        if opts.interactive_shell:
            await run_cli(opts.rpc_host, opts.rpc_port, cli_parser, stdin_reader, stdout_writer)
        else:
            opts, params = cli_parser.parse_known_args()
            opts = merge_params(opts, params)
            async with GatewayRpcClient(opts.rpc_host, opts.rpc_port) as client:
                await handle_command(opts, client, stdout_writer)
    except (ClientConnectorError, TimeoutError, CancelledError) as e:
        print(f"Connection to RPC server is broken: {e}, exiting!", file=sys.stderr)
        if opts.debug:
            raise
    finally:
        stdout_writer.close()


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    run_main()
