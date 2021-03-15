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

"""
Valid import path: bxcommon.models, bxcommon.rpc
"""
from bxcommon.models.blockchain_protocol import BlockchainProtocol
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
    "{:<18} get BDN performance stats.".format("bdn_performance"),
    "{:<18} get quota usage status.".format("quota_usage"),
    "{:<18} dump transaction service to file.".format("tx_service"),
    "{:<18} get status of transaction.".format("tx_status")
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


class RpcClient:

    def __init__(
        self,
        rpc_host: str,
        rpc_port: int,
        rpc_user: str,
        rpc_password: str,
        account_id: str,
        secret_hash: str,
        cloud_api_url: str,
        target_is_cloud_api: bool,
        interactive: bool = False
    ):
        self._session = ClientSession()
        self.target_is_cloud_api = target_is_cloud_api
        self.interactive = interactive
        if target_is_cloud_api:
            self._rpc_url = cloud_api_url
            self._encoded_auth = base64.b64encode(f"{account_id}:{secret_hash}".encode("utf-8")).decode("utf-8")
        else:
            self._rpc_url = f"http://{rpc_host}:{rpc_port}/"
            self._encoded_auth = base64.b64encode(f"{rpc_user}:{rpc_password}".encode("utf-8")).decode("utf-8")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def make_request(
        self,
        method: RpcRequestType,
        request_id: Optional[str] = None,
        request_params: Union[Dict[str, Any], None] = None
    ) -> Optional[ClientResponse]:
        if request_id is None:
            request_id = str(uuid.uuid4())
        json_data = {
            "method": method.name.lower(),
            "id": request_id,
            "params": request_params
        }
        synchronous = True
        if request_params:
            synchronous = request_params.get(rpc_constants.SYNCHRONOUS_PARAMS_KEY, True)

        if not synchronous and self.interactive:
            asyncio.create_task(
                self._session.post(
                    self._rpc_url,
                    data=json.dumps(json_data),
                    headers={
                        rpc_constants.CONTENT_TYPE_HEADER_KEY: rpc_constants.PLAIN_HEADER_TYPE,
                        rpc_constants.AUTHORIZATION_HEADER_KEY: self._encoded_auth
                    }
                )
            )
            return None

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
    if merged_opts.command == RpcRequestType.BLXR_TX:
        if merged_opts.request_params is None and unrecognized_params:
            transaction_payload = unrecognized_params[0]
            merged_opts.request_params = {
                rpc_constants.TRANSACTION_PARAMS_KEY: transaction_payload,
                rpc_constants.SYNCHRONOUS_PARAMS_KEY: True,
                rpc_constants.STATUS_TRACK_PARAMS_KEY: rpc_constants.STATUS_TRACK_PARAMS_KEY in unrecognized_params,
                rpc_constants.NONCE_MONITORING_PARAMS_KEY: rpc_constants.NONCE_MONITORING_PARAMS_KEY in unrecognized_params,
                rpc_constants.ACCOUNT_ID_PARAMS_KEY: opts.account_id if opts.cloud_api else opts.rpc_user,
                rpc_constants.BLOCKCHAIN_PROTOCOL_PARAMS_KEY: opts.blockchain_protocol,
                rpc_constants.BLOCKCHAIN_NETWORK_PARAMS_KEY: opts.blockchain_network
            }
    elif merged_opts.command == RpcRequestType.GATEWAY_STATUS:
        if merged_opts.request_params is None and unrecognized_params:
            merged_opts.request_params = {rpc_constants.DETAILS_LEVEL_PARAMS_KEY: unrecognized_params[0]}
    elif merged_opts.command == RpcRequestType.QUOTA_USAGE:
        if merged_opts.request_params is None:
            merged_opts.request_params = {
                rpc_constants.ACCOUNT_ID_PARAMS_KEY: opts.account_id if opts.cloud_api else opts.rpc_user
            }
    elif merged_opts.command == RpcRequestType.TX_SERVICE:
        if merged_opts.request_params is None and unrecognized_params:
            merged_opts.request_params = {rpc_constants.TX_SERVICE_FILE_NAME_PARAMS_KEY: unrecognized_params[0]}
    elif merged_opts.command == RpcRequestType.ADD_BLOCKCHAIN_PEER or \
            merged_opts.command == RpcRequestType.REMOVE_BLOCKCHAIN_PEER:
        if merged_opts.request_params is None and unrecognized_params:
            merged_opts.request_params = {rpc_constants.BLOCKCHAIN_PEER_PARAMS_KEY: unrecognized_params[0]}

    return merged_opts


async def format_response(response: ClientResponse) -> str:
    try:
        response_json_str = await response.text()
        response_json_dict = json.loads(json.loads(response_json_str))
        result = response_json_dict.get("result", None)
        if not result:
            result = response_json_dict.get("error", "Unknown error")
        return json.dumps(result, indent=4)
    except (JSONDecodeError, ContentTypeError, TypeError):
        return await response.text()


async def handle_command(opts: Namespace, client: RpcClient, stdout_writer: StreamWriter) -> None:
    if opts.debug:
        stdout_writer.write(f"executing with opts: {opts}\n".encode("utf-8"))
    response_text: Optional[str] = None
    if isinstance(opts.command, RpcRequestType):
        response: Optional[ClientResponse] = await client.make_request(
            opts.command, opts.request_id, opts.request_params
        )
        if response:
            response_text = await format_response(response)
        else:
            response_text = "{'message': 'Sent async request. Not waiting for response'}"
    elif opts.command == CLICommand.HELP:
        try:
            response: ClientResponse = await client.get_server_help()
            response_text = "Server Help:\n\n{}".format(await format_response(response))
        except (ClientConnectorError, ClientConnectionError):
            pass
    if response_text is not None:
        stdout_writer.write(response_text.encode("utf-8").decode("unicode_escape").encode("utf-8"))
        stdout_writer.write(b"\n")
        await stdout_writer.drain()
    await asyncio.sleep(0)


async def parse_and_handle_command(
    arg_parser: ArgumentParser,
    stdout_writer: StreamWriter,
    client: RpcClient,
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
    account_id: str,
    secret_hash: str,
    cloud_api_url: str,
    target_is_cloud_api: bool,
    arg_parser: ArgumentParser,
    stdin_reader: StreamReader,
    stdout_writer: StreamWriter

) -> None:

    interactive = True
    async with RpcClient(
        rpc_host, rpc_port, rpc_user, rpc_password, account_id, secret_hash, cloud_api_url, target_is_cloud_api,
        interactive
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
        "--cloud-api",
        help=f"Should the bloxroute-cli send the request to bloXroute Cloud API",
        action="store_true"
    )
    arg_parser.add_argument(
        "--cloud-api-url",
        help=f"bloXroute's cloud-api DNS name (default: {rpc_constants.CLOUD_API_URL})",
        type=str,
        default=rpc_constants.CLOUD_API_URL
    )
    arg_parser.add_argument(
        "--account-id",
        help=f"The account's ID. Contact support@bloxroute.com for assistance. ",
        type=str
    )
    arg_parser.add_argument(
        "--secret-hash",
        help=f"The account's secret key. Contact support@bloxroute.com for assistance. ",
        type=str,
    )
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
        help=f"The Gateway's RPC server username. Contact support@bloxroute.com for assistance."
             f"(default: {rpc_constants.DEFAULT_RPC_USER})",
        type=str,
        default=rpc_constants.DEFAULT_RPC_USER
    )
    arg_parser.add_argument(
        "--rpc-password",
        help=f"The Gateway's RPC server password. (default: {rpc_constants.DEFAULT_RPC_PASSWORD})",
        type=str,
        default=rpc_constants.DEFAULT_RPC_PASSWORD
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


def validate_args(opts: Namespace, stdout_writer: StreamWriter) -> bool:
    if ("account_id" in opts and opts.account_id is not None) and \
            ("secret_hash" in opts and opts.secret_hash is not None):
        opts.cloud_api = True
        sys.argv.append("--cloud-api")
        return True

    elif ("cloud_api" in opts and opts.cloud_api is True):
        if ("cloud_api_url" not in opts or ("cloud_api_url" in opts and opts.cloud_api_url is None)) or \
           ("account_id" not in opts or ("account_id" in opts and opts.account_id is None)) or \
           ("secret_hash" not in opts or ("secret_hash" in opts and opts.secret_hash is None)):
            stdout_writer.write("The use of bloXroute Cloud API requires the following two arguments: "
                                "--account-id, --secret-hash.\n".encode("utf-8"))
            return False

    elif ("rpc_host" not in opts or ("rpc_host" in opts and opts.rpc_host is None)) or \
         ("rpc_port" not in opts or ("rpc_port" in opts and opts.rpc_port is None)) or \
         ("rpc_user" not in opts or ("rpc_user" in opts and opts.rpc_user is None)) or \
         ("rpc_password" not in opts or ("rpc_password" in opts and opts.rpc_password is None)):
            stdout_writer.write("The use of bloXroute Gateway requires the following two arguments: "
                                "--rpc-user, --rpc-password.\n".encode("utf-8"))
            return False

    return True


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
    if not validate_args(opts, stdout_writer):
        exit(1)

    try:

        if opts.interactive_shell or len(params) == 0:
            await run_cli(
                opts.rpc_host, opts.rpc_port, opts.rpc_user, opts.rpc_password,
                opts.account_id, opts.secret_hash, opts.cloud_api_url, opts.cloud_api,
                cli_parser, stdin_reader, stdout_writer
            )
        else:
            if "help" in sys.argv:
                cli_parser.print_help(file=ArgParserFile(stdout_writer))
            async with RpcClient(
                opts.rpc_host, opts.rpc_port, opts.rpc_user, opts.rpc_password,
                opts.account_id, opts.secret_hash, opts.cloud_api_url, opts.cloud_api
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
