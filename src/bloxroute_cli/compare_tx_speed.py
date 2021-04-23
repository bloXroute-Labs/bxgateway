import argparse
import datetime
import json
import requests
import sys
import time
from collections import defaultdict
from enum import Enum
from typing import Optional

from eth_account import Account
from eth_account.datastructures import SignedTransaction
from threading import Thread
from web3 import Web3


def main() -> None:

    alchemy_endpoint: Optional[str] = None
    infura_endpoint: Optional[str] = None
    eth_tx_receipt_endpoint: Optional[str] = None

    default_web3 = Web3(Web3.HTTPProvider())
    gas_limit = 22000
    parser = get_argument_parser()
    args = parser.parse_args()
    sender_private_key = args.sender_private_key
    receiver_address = default_web3.toChecksumAddress(args.receiver_address)  # pyre-ignore
    blxr_endpoint = args.blxr_endpoint
    blxr_auth_header = args.blxr_auth_header
    num_tx_groups = args.num_tx_groups
    gas_price_wei = args.gas_price * int(1e9)
    delay = args.delay

    # Check Alchemy and Infura API keys
    alchemy_api_key = args.alchemy_api_key
    if alchemy_api_key:
        alchemy_endpoint = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_api_key
        eth_tx_receipt_endpoint = alchemy_endpoint
        default_web3 = Web3(Web3.HTTPProvider(alchemy_endpoint))
        if not default_web3.isConnected():
            print(f"Alchemy endpoint {alchemy_endpoint} is not connected. Please check your API key and try again.")
            sys.exit(0)
    infura_api_key = args.infura_api_key
    if infura_api_key:
        infura_endpoint = "https://mainnet.infura.io/v3/" + infura_api_key
        eth_tx_receipt_endpoint = infura_endpoint
        default_web3 = Web3(Web3.HTTPProvider(infura_endpoint))
        if not default_web3.isConnected():
            print(f"Infura endpoint {infura_endpoint} is not connected. Please check your API key and try again.")
            sys.exit(0)

    if not (alchemy_api_key or infura_api_key):
        print("Please provide at least one API key with --alchemy-api-key or --infura-api-key.")
        sys.exit(0)

    # Check bloXroute authorization header
    blxr_quota_usage_request_json = {"method": "quota_usage"}
    result = requests.post(
        blxr_endpoint,
        json=blxr_quota_usage_request_json,
        headers={
            "Content-Type": "text/plain",
            "Authorization": blxr_auth_header
        }
    )
    result_json = result.json().encode("utf-8").decode("unicode_escape")
    result_json = json.loads(result_json)
    if "result" not in result_json or "quota_limit" not in result_json["result"]:
        print(f"Failed to check bloXroute account status with result {result_json} returned. Please provide correct "
              f"bloXroute authorization header with argument --blxr-auth-header and try again.")
        sys.exit(0)

    # Check sender's wallet balance
    sender_account = Account.from_key(sender_private_key)  # pyre-ignore
    sender_address = sender_account.address

    # pyre-ignore[29]: `web3.method.Method` is not a function.
    nonce = default_web3.eth.getTransactionCount(sender_address)
    # pyre-ignore[29]: `web3.method.Method` is not a function.
    sender_balance = default_web3.eth.getBalance(sender_address)
    sender_balance_in_eth = default_web3.fromWei(sender_balance, "ether")  # pyre-ignore

    sender_expense = num_tx_groups * gas_price_wei * gas_limit
    if sender_balance < sender_expense:
        projected_ether = default_web3.fromWei(sender_expense, "ether")  # pyre-ignore
        print(f"Sender {sender_address} does not have enough balance for {num_tx_groups} groups of transactions. "
              f"Sender's balance is {sender_balance_in_eth} ETH, "
              f"while at least {projected_ether} ETH is required")
        sys.exit(0)

    print(f"Initial check completed. Sleeping {delay} sec.")
    time.sleep(delay)

    # Generate and send num_tx_groups transactions
    group_num_to_tx_hash = {}
    for i in range(1, num_tx_groups + 1):

        provider_to_tx_hash = {}
        threads = []
        print(f"Sending tx group {i}.")

        # Alchemy transaction
        if alchemy_api_key:
            tx_alchemy = {
                "to": receiver_address,
                "value": 0,
                "gas": gas_limit,
                "gasPrice": gas_price_wei,
                "nonce": nonce,
                "chainId": 1,
                "data": "0x11"
            }
            signed_tx_alchemy = default_web3.eth.account.sign_transaction(tx_alchemy, sender_private_key)
            provider_to_tx_hash[Provider.ALCHEMY] = signed_tx_alchemy.hash.hex()
            thread_alchemy = Thread(target=send_tx_eth, args=([signed_tx_alchemy, alchemy_endpoint]))
            threads.append(thread_alchemy)

        # Infura transaction
        if infura_api_key:
            tx_infura = {
                "to": receiver_address,
                "value": 0,
                "gas": gas_limit,
                "gasPrice": gas_price_wei,
                "nonce": nonce,
                "chainId": 1,
                "data": "0x22"
            }
            signed_tx_infura = default_web3.eth.account.sign_transaction(tx_infura, sender_private_key)
            provider_to_tx_hash[Provider.INFURA] = signed_tx_infura.hash.hex()
            thread_infura = Thread(target=send_tx_eth, args=([signed_tx_infura, infura_endpoint]))
            threads.append(thread_infura)

        # bloXroute Cloud-API transaction
        tx_blxr = {
            "to": receiver_address,
            "value": 0,
            "gas": gas_limit,
            "gasPrice": gas_price_wei,
            "nonce": nonce,
            "chainId": 1,
            "data": "0x33"
        }
        signed_tx_blxr = default_web3.eth.account.sign_transaction(tx_blxr, sender_private_key)
        provider_to_tx_hash[Provider.BLOXROUTE] = signed_tx_blxr.hash.hex()
        thread_blxr = Thread(target=send_tx_blxr, args=([signed_tx_blxr, blxr_endpoint, blxr_auth_header]))
        threads.append(thread_blxr)

        # Start the threads in parallel
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        nonce += 1
        group_num_to_tx_hash[i] = provider_to_tx_hash
        # Add a delay to all the groups except for the last group
        if i < num_tx_groups:
            print(f"Sleeping {delay} sec.")
            time.sleep(delay)

    print("Sleeping 1 min before checking transaction status.")
    time.sleep(60)

    provider_to_tx_mined = defaultdict(int)
    mined_tx_nums = set()
    sleep_left_minute = 4
    while len(mined_tx_nums) < num_tx_groups and sleep_left_minute > 0:
        for group_num in group_num_to_tx_hash:

            # Continue for confirmed transactions
            if group_num in mined_tx_nums:
                continue

            # Check transactions sent by different providers and find the confirmed one
            provider_to_tx_hash = group_num_to_tx_hash[group_num]
            for provider in provider_to_tx_hash:
                tx_hash = provider_to_tx_hash[provider]
                get_tx_receipt_request_json = {
                    "jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": [tx_hash], "id": 1
                }
                result = requests.post(
                    eth_tx_receipt_endpoint,
                    json=get_tx_receipt_request_json,
                    headers={"Content-Type": "application/json"}
                )
                result_json = result.json()
                if "result" not in result_json:
                    print(f"Failed to check transaction status of tx {tx_hash} in group {group_num}. "
                          f"Skipping current group.")
                    break
                if result_json["result"] is not None:
                    provider_to_tx_mined[provider] += 1
                    mined_tx_nums.add(group_num)
                    break

        # When there is any pending transaction, maximum sleep time is 4 min
        if len(mined_tx_nums) < num_tx_groups:
            print(f"{num_tx_groups - len(mined_tx_nums)} transactions are pending. "
                  f"Sleeping 1 min before checking status again.")
            time.sleep(60)
            sleep_left_minute -= 1

    print("---------------------------------------------------------------------------------------------------------")
    print("Summary:")
    print(f"Sent {num_tx_groups} groups of transactions to bloXroute Cloud-API and other providers, "
          f"{len(mined_tx_nums)} of them have been confirmed: ")
    if alchemy_api_key:
        print(f"Number of Alchemy transactions mined: {provider_to_tx_mined[Provider.ALCHEMY]}")
    if infura_api_key:
        print(f"Number of Infura transactions mined: {provider_to_tx_mined[Provider.INFURA]}")
    print(f"Number of bloXrotue transactions mined: {provider_to_tx_mined[Provider.BLOXROUTE]}")


class Provider(Enum):
    ALCHEMY = "Alchemy"
    BLOXROUTE = "bloXrotue"
    INFURA = "Infura"


# Send transaction to bloXroute Cloud-API
def send_tx_blxr(tx: SignedTransaction, endpoint: str, auth_header: str) -> None:

    tx_params = {
        "transaction": tx.rawTransaction.hex()[2:],
        "synchronous": "True"
    }
    send_tx_request_json = {
        "method": "blxr_tx",
        "params": tx_params
    }

    print(f"{datetime.datetime.utcnow()} - Sending transaction to bloXroute Cloud-API. TX Hash: {tx.hash.hex()}")
    result = requests.post(
        endpoint,
        json=send_tx_request_json,
        headers={
            "Content-Type": "text/plain",
            "Authorization": auth_header
        }
    )
    result_json = result.json().encode("utf-8").decode("unicode_escape")
    result_json = json.loads(result_json)

    if "result" not in result_json or "tx_hash" not in result_json["result"]:
        print(f"{datetime.datetime.utcnow()} - Failed to send raw transaction {tx.rawTransaction.hex()} to "
              f"bloXroute Cloud-API, with result {result_json} returned. ")


# Send transaction to other service providers (Alchemy, Infura)
def send_tx_eth(tx: SignedTransaction, endpoint: str) -> None:

    params = [tx.rawTransaction.hex()]
    send_tx_request_json = {
        "jsonrpc": "2.0",
        "method": "eth_sendRawTransaction",
        "params": params,
        "id": 1
    }

    print(f"{datetime.datetime.utcnow()} - Sending transaction to {endpoint}. TX Hash: {tx.hash.hex()}")
    result = requests.post(endpoint, json=send_tx_request_json, headers={"Content-Type": "application/json"})
    result_json = result.json()

    if "result" not in result_json:
        print(f"{datetime.datetime.utcnow()} - Failed to use endpoint {endpoint} to "
              f"send raw tx {tx.rawTransaction.hex()}, with result {result_json} returned. ")


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--infura-api-key", type=str,
                        help="Infura API key / project ID. "
                             "Sample Infura endpoint: https://mainnet.infura.io/v3/<api_key>.")
    parser.add_argument("--alchemy-api-key", type=str,
                        help="Alchemy API key. "
                             "Sample Alchemy endpoint: https://eth-mainnet.alchemyapi.io/v2/<api_key>.")
    parser.add_argument("--blxr-endpoint", type=str, default="http://api.blxrbdn.com:443",
                        help="bloXroute endpoint. Use http://api.blxrbdn.com:443 for Cloud-API.")
    parser.add_argument("--blxr-auth-header", type=str, required=True,
                        help="bloXroute authorization header. Use base64 encoded value of "
                             "account_id:secret_hash for Cloud-API. For more information, see "
                             "https://bloxroute.com/docs/bloxroute-documentation/cloud-api/overview/")
    parser.add_argument("--sender-private-key", type=str, required=True,
                        help="Sender's private key, which starts with 0x.")
    parser.add_argument("--receiver-address", type=str, required=True,
                        help="Receiver's address, which starts with 0x.")
    parser.add_argument("--num-tx-groups", type=int, default=1,
                        help="Number of groups of transactions to submit.")
    parser.add_argument("--gas-price", type=int, required=True,
                        help="Transaction gas price in Gwei.")
    parser.add_argument("--delay", type=int, default=30,
                        help="Time (sec) to sleep between two consecutive groups.")
    return parser


if __name__ == "__main__":
    main()
