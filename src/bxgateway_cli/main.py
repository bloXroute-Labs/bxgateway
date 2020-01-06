import asyncio
import uuid
import json
import sys
import shlex
import os
from enum import Enum
from asyncio import StreamReader, StreamReaderProtocol, StreamWriter, CancelledError
from asyncio.streams import FlowControlMixin
from argparse import ArgumentParser, Namespace
from json import JSONDecodeError
from typing import Optional, Union, Dict, List, Any

from aiohttp import ClientSession, ClientResponse, ClientConnectorError

from bxgateway.rpc import rpc_constants
from bxgateway.rpc.rpc_request_type import RpcRequestType

DEFAULT_RPC_PORT = 28332
DEFAULT_RPC_HOST = "127.0.0.1"


class CLICommand(Enum):
    EXIT = 0
    HELP = 1


class ArgParserFile:

    def __init__(self, writer: StreamWriter):
        self._writer = writer

    def write(self, message: str) -> int:
        lines = message.split("\n")
        for line in lines:
            self._writer.write(line.encode("utf-8"))
            self._writer.write(b"\n")
        return len(message)


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
            headers={rpc_constants.CONTENT_TYPE_HEADER_KEY: rpc_constants.PLAIN_HEADER_TYPE}
        )

    async def get_server_help(self) -> ClientResponse:
        return await self._session.get(self._rpc_url)

    async def close(self) -> None:
        await self._session.close()


def merge_params(opts: Namespace, unrecognized_params: List[str]) -> Namespace:
    merged_opts = Namespace()
    merged_opts.__dict__ = opts.__dict__.copy()
    if merged_opts.request_params is None and unrecognized_params:
        if merged_opts.command == RpcRequestType.BLXR_TX:
            merged_opts.request_params = {rpc_constants.TRANSACTION_PARAMS_KEY: unrecognized_params[0]}
        elif merged_opts.command == RpcRequestType.GATEWAY_STATUS:
            merged_opts.request_params = {rpc_constants.DETAILS_LEVEL_PARAMS_KEY: unrecognized_params[0]}
    return merged_opts


async def format_response(response: ClientResponse, content_type=rpc_constants.JSON_HEADER_TYPE) -> str:
    try:
        response_json = await response.json(content_type=content_type)
        return json.dumps(response_json, indent=4, sort_keys=True)
    except JSONDecodeError:
        return await response.text()


async def handle_command(opts: Namespace, client: GatewayRpcClient, stdout_writer: StreamWriter) -> None:
    if opts.debug:
        stdout_writer.write(f"executing with opts: {opts}\n".encode("utf-8"))
    response_text: Optional[str] = None
    if isinstance(opts.command, RpcRequestType):
        response: ClientResponse = await client.make_request(
            opts.command, opts.request_id, opts.request_params
        )
        response_text = await format_response(response, rpc_constants.PLAIN_HEADER_TYPE)
    elif opts.command == CLICommand.HELP:
        try:
            response: ClientResponse = await client.get_server_help()
            response_text = f"Server Help:\n\n{await format_response(response)}"
        except ClientConnectorError:
            pass
    if response_text is not None:
        stdout_writer.write(response_text.encode("utf-8"))
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
                arg_parser.print_help(file=ArgParserFile(stdout_writer))  # pyre-ignore
            try:
                opts, params = arg_parser.parse_known_args(shlex.split(line))
            except KeyError as unrecognized_command:
                stdout_writer.write(f"unrecognized command {unrecognized_command} entered, ignoring!\n".encode("utf-8"))
                await stdout_writer.drain()
                continue

            opts = merge_params(opts, params)
            await handle_command(opts, client, stdout_writer)


def get_command(command: str) -> Union[CLICommand, RpcRequestType]:
    command = command.upper()
    if command in CLICommand.__members__:
        return CLICommand[command]
    else:
        return RpcRequestType[command]


def add_run_arguments(arg_parser: ArgumentParser) -> None:
    arg_parser.add_argument(
        "command",
        help="The Gateway CLI command (valid values: {}).".format(
            [command.lower() for command in CLICommand.__members__.keys()] +
            [method.lower() for method in RpcRequestType.__members__.keys()]
        ),
        type=get_command
    )
    arg_parser.add_argument(
        "--request-id",
        help="The Gateway RPC request unique identifier (default: {}).".format(None),
        type=str,
        default=None
    )
    arg_parser.add_argument(
        "--request-params",
        help="The Gateway RPC request params (default: {}).".format(None),
        type=json.loads,
        default=None
    )


def add_base_arguments(arg_parser: ArgumentParser) -> None:
    arg_parser.add_argument(
        "--rpc-host",
        help="The Gateway RPC host (default: {}).".format(DEFAULT_RPC_HOST),
        type=str,
        default=DEFAULT_RPC_HOST
    )
    arg_parser.add_argument(
        "--rpc-port",
        help="The Gateway RPC port (default: {}).".format(DEFAULT_RPC_PORT),
        type=int,
        default=DEFAULT_RPC_PORT
    )
    arg_parser.add_argument(
        "--interactive-shell",
        help="Run the Gateway RPC client in interactive Shell mode (default: False)",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--debug",
        help="Run the Gateway RPC client in debug mode (default: False)",
        action="store_true",
        default=False
    )


async def main():
    arg_parser = ArgumentParser(prog="Gateway CLI")
    cli_parser = ArgumentParser(prog="Gateway CLI")
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
        stdout_writer.write(f"Connection to RPC server is broken: {e}, exiting!\n".encode("utf-8"))
        await stdout_writer.drain()
        if opts.debug:
            raise
    finally:
        stdout_writer.close()


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    run_main()
