import asyncio
import base64
import uuid
import json
import sys
import shlex
import os
from enum import Enum
from asyncio import StreamReader, StreamReaderProtocol, StreamWriter, CancelledError
from asyncio.streams import FlowControlMixin
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from json import JSONDecodeError
from typing import Optional, Union, Dict, List, Any
from aiohttp import ClientSession, ClientResponse, ClientConnectorError, ContentTypeError, ClientConnectionError

from bxcommon.models.blockchain_protocol import BlockchainProtocol
from bxcommon.models.url_scheme import UrlScheme
from bxcommon.rpc import rpc_constants
from bxcommon.rpc.rpc_request_type import RpcRequestType


COMMANDS_HELP = [
    "{:<18} exit the CLI.".format("exit"),
    "{:<18} print detailed help.".format("help"),
    "{:<18} send transaction to the bloXroute BDN.".format("blxr_tx"),
    "{:<18} get the status of the bloXroute Gateway.".format("gateway_status"),
    "{:<18} get the memory stats of the bloXroute Gateway.".format("memory"),
    "{:<18} shutdown the Gateway server.".format("stop"),
    "{:<18} get the bloXroute Gateway connected peers info.".format("peers"),
    "{:<18} get BDN performance stats.".format("bdn_performance")
]


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

    def __init__(self, rpc_host: str, rpc_port: int, rpc_user: str, rpc_password: str, url_scheme: str):
        self._session = ClientSession()
        self._rpc_url = rpc_constants.PUBLIC_API_URL.format(rpc_user) if url_scheme.lower() == UrlScheme.HTTPS.value \
            else f"http://{rpc_host}:{rpc_port}/"
        self._encoded_auth = base64.b64encode(f"{rpc_user}:{rpc_password}".encode("utf-8")).decode("utf-8")

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
            headers={
                rpc_constants.CONTENT_TYPE_HEADER_KEY: rpc_constants.PLAIN_HEADER_TYPE,
                rpc_constants.AUTHORIZATION_HEADER_KEY: self._encoded_auth
            }
        )

    async def get_server_help(self) -> ClientResponse:
        return await self._session.get(
            self._rpc_url,
            headers={
                rpc_constants.CONTENT_TYPE_HEADER_KEY: rpc_constants.PLAIN_HEADER_TYPE,
                rpc_constants.AUTHORIZATION_HEADER_KEY: self._encoded_auth
            }
        )

    async def close(self) -> None:
        await self._session.close()


def merge_params(opts: Namespace, unrecognized_params: List[str]) -> Namespace:
    merged_opts = Namespace()
    merged_opts.__dict__ = opts.__dict__.copy()
    if merged_opts.request_params is None and unrecognized_params:
        if merged_opts.command == RpcRequestType.BLXR_TX:
            transaction_payload = unrecognized_params[0]
            synchronous = str(True)
            if len(unrecognized_params) > 1:
                synchronous = unrecognized_params[1]
            merged_opts.request_params = {
                rpc_constants.TRANSACTION_PARAMS_KEY: transaction_payload,
                rpc_constants.SYNCHRONOUS_PARAMS_KEY: synchronous,
                rpc_constants.ACCOUNT_ID_PARAMS_KEY: opts.rpc_user,
                rpc_constants.BLOCKCHAIN_PROTOCOL_PARAMS_KEY: opts.blockchain_protocol,
                rpc_constants.BLOCKCHAIN_NETWORK_PARAMS_KEY: opts.blockchain_network
            }
        elif merged_opts.command == RpcRequestType.GATEWAY_STATUS:
            merged_opts.request_params = {rpc_constants.DETAILS_LEVEL_PARAMS_KEY: unrecognized_params[0]}
    return merged_opts


async def format_response(response: ClientResponse, content_type=rpc_constants.JSON_HEADER_TYPE) -> str:
    try:
        response_json = await response.json(content_type=content_type)
        result = response_json.get("result", response_json)
        if not result:
            result = response_json.get("error", "Unknown error")
        return json.dumps(result, indent=4)
    except (JSONDecodeError, ContentTypeError):
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
            response_text = "Server Help:\n\n{}".format(await format_response(response))
        except (ClientConnectorError, ClientConnectionError):
            pass
    if response_text is not None:
        stdout_writer.write(response_text.encode("utf-8"))
        stdout_writer.write(b"\n")
        await stdout_writer.drain()
    await asyncio.sleep(0)


async def parse_and_handle_command(
        arg_parser: ArgumentParser,
        stdout_writer: StreamWriter,
        client: GatewayRpcClient,
        shell_args: Optional[List[str]] = None
) -> None:
    try:
        opts, params = arg_parser.parse_known_args(shell_args)
    except KeyError as unrecognized_command:
        stdout_writer.write(
            f"unrecognized command {str(unrecognized_command).lower()} entered, ignoring!\n".encode("utf-8")
        )
        await stdout_writer.drain()
    else:
        opts = merge_params(opts, params)
        await handle_command(opts, client, stdout_writer)


async def run_cli(
        rpc_host: str,
        rpc_port: int,
        rpc_user: str,
        rpc_password: str,
        url_scheme: str,
        arg_parser: ArgumentParser,
        stdin_reader: StreamReader,
        stdout_writer: StreamWriter
) -> None:
    async with GatewayRpcClient(
            rpc_host, rpc_port, rpc_user, rpc_password, url_scheme,
    ) as client:
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
            if len(shell_args) == 0:
                shell_args.append("--interactive-shell")
            if "-h" in shell_args or "--help" in shell_args or "help" in shell_args:
                arg_parser.print_help(file=ArgParserFile(stdout_writer))  # pyre-ignore
            await parse_and_handle_command(arg_parser, stdout_writer, client, shell_args)


def get_command(command: str) -> Union[CLICommand, RpcRequestType]:
    command = command.upper()
    if command in CLICommand.__members__:
        return CLICommand[command]
    else:
        return RpcRequestType[command]


def get_command_help() -> str:
    commands = [command.lower() for command in CLICommand.__members__.keys()] + \
        [command.lower() for command in RpcRequestType.__members__.keys()]
    return f"The CLI command (valid values: {commands})."


def get_description() -> str:
    desc = "Commands:\n"
    for command_help in COMMANDS_HELP:
        desc = f"{desc}\n{command_help}"
    return desc


def add_run_arguments(arg_parser: ArgumentParser) -> None:
    arg_parser.add_argument(
        "command",
        help=get_command_help(),
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
    arg_parser.add_argument(
        "--blockchain-protocol",
        help="Blockchain protocol. e.g Ethereum, BitcoinCash. (default: Ethereum)",
        type=str,
        default=BlockchainProtocol.ETHEREUM.name
    )
    arg_parser.add_argument(
        "--blockchain-network",
        help="Blockchain network. e.g Mainnet, Ropsten, Rinkeby, and Testnet. (default: Mainnet)",
        type=str,
        default=rpc_constants.MAINNET_NETWORK_NAME
    )


def add_base_arguments(arg_parser: ArgumentParser) -> None:
    arg_parser.add_argument(
        "--rpc-host",
        help="The Gateway RPC host (default: {}).".format(rpc_constants.DEFAULT_RPC_HOST),
        type=str,
        default=rpc_constants.DEFAULT_RPC_HOST
    )
    arg_parser.add_argument(
        "--rpc-port",
        help="The Gateway RPC port (default: {}).".format(rpc_constants.DEFAULT_RPC_PORT),
        type=int,
        default=rpc_constants.DEFAULT_RPC_PORT
    )
    arg_parser.add_argument(
        "--rpc-user",
        help=f"The Gateway RPC server username, account ID. Contact support@bloxroute.com for assistance. "
             f"(default: {rpc_constants.DEFAULT_RPC_USER})",
        type=str,
        default=rpc_constants.DEFAULT_RPC_USER
    )
    arg_parser.add_argument(
        "--rpc-password",
        help=f"The Gateway RPC server password, secret key. (default: {rpc_constants.DEFAULT_RPC_PASSWORD})",
        type=str,
        default=rpc_constants.DEFAULT_RPC_PASSWORD
    )
    arg_parser.add_argument(
        "--url-scheme",
        help=f"URL scheme in RPC request. Use bloXroute public API as RPC server if https, "
             f"or use Gateway if http. (default: http)",
        type=str,
        default=UrlScheme.HTTP.value
    )
    arg_parser.add_argument(
        "--interactive-shell",
        help="Run the Gateway RPC client in interactive Shell mode (default: False)",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--debug",
        help="Run the CLI in debug mode (default: False)",
        action="store_true",
        default=False
    )


async def main():
    arg_parser = ArgumentParser(
        prog="bloXroute CLI", epilog=get_description(), formatter_class=RawDescriptionHelpFormatter
    )
    cli_parser = ArgumentParser(
        prog="bloXroute CLI", epilog=get_description(), formatter_class=RawDescriptionHelpFormatter
    )
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
    if opts.url_scheme.lower() == UrlScheme.HTTPS.value and (not opts.rpc_user or not opts.rpc_password):
        stdout_writer.write("The use of https scheme and bloXroute public API requires the following two arguments: "
                            "--rpc-user, --rpc-password.\n".encode("utf-8"))
        exit(1)
    try:

        if opts.interactive_shell or len(params) == 0:
            await run_cli(
                opts.rpc_host, opts.rpc_port, opts.rpc_user, opts.rpc_password, opts.url_scheme,
                cli_parser, stdin_reader, stdout_writer
            )
        else:
            if "help" in sys.argv:
                cli_parser.print_help(file=ArgParserFile(stdout_writer))
            async with GatewayRpcClient(
                    opts.rpc_host, opts.rpc_port, opts.rpc_user, opts.rpc_password, opts.url_scheme,
            ) as client:
                await parse_and_handle_command(cli_parser, stdout_writer, client)
    except (ClientConnectorError, ClientConnectionError, TimeoutError, CancelledError) as e:
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
