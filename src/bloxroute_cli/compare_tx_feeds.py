import argparse
import asyncio
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Set, List, Any, cast

from typing import IO

from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.rpc.external.eth_ws_subscriber import EthWsSubscriber


class HashEntry:
    tx_hash: str
    gateway_time_received: Optional[float] = None
    eth_node_time_received: Optional[float] = None

    def __init__(
        self, tx_hash: str, gateway_time_received: Optional[float] = None, node_time_received: Optional[float] = None
    ) -> None:
        self.tx_hash = tx_hash
        self.gateway_time_received = gateway_time_received
        self.eth_node_time_received = node_time_received

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, HashEntry)
            and other.tx_hash == self.tx_hash
        )


ws_provider: WsProvider
eth_ws_provider: EthWsSubscriber
time_to_begin_comparison = 0.0
time_to_end_comparison = 0.0
all_hashes_file: Optional[IO[Any]] = None
missing_hashes_file: Optional[IO[Any]] = None
trail_new_hashes: Set[str] = set()
lead_new_hashes: Set[str] = set()
low_fee_hashes: Set[str] = set()
high_delta_hashes: Set[str] = set()
seen_hashes: Dict[str, HashEntry] = {}
num_intervals_passed = 0
num_intervals: Optional[int] = None


async def main() -> None:
    global seen_hashes
    global trail_new_hashes
    global low_fee_hashes
    global time_to_begin_comparison
    global time_to_end_comparison
    global num_intervals_passed
    global num_intervals

    parser = get_argument_parser()
    args = parser.parse_args()

    if (args.min_gas_price or args.addresses) and args.exclude_tx_contents:
        print("ERROR: If filtering by minimum gas price or addresses, exclude-tx-contents must be False.")
        sys.exit()

    addresses = None if not args.addresses else args.addresses.lower().split(",")
    min_gas = None if not args.min_gas_price else int(args.min_gas_price * 10e8)

    global all_hashes_file
    if "ALL" in args.dump:
        all_hashes_file = open("all_hashes.csv", "w")
        f = all_hashes_file
        assert f is not None
        f.write("tx-hash,blxr Time,eth Time\n")

    global missing_hashes_file
    if "MISSING" in args.dump:
        missing_hashes_file = open("missing_hashes.txt", "w")

    time_to_begin_comparison = time.time() + args.lead_time
    time_to_end_comparison = time_to_begin_comparison + args.interval
    num_intervals = args.num_intervals

    if args.use_cloud_api:
        asyncio.create_task(process_new_txs_cloud_api(
            args.cloud_api_ws_uri,
            args.auth_header,
            args.feed_name,
            args.exclude_tx_contents,
            args.exclude_duplicates,
            args.exclude_from_blockchain,
            min_gas,
            addresses
        ))
    else:
        asyncio.create_task(process_new_txs_gateway(
            args.gateway,
            args.feed_name,
            args.exclude_tx_contents,
            args.exclude_duplicates,
            args.exclude_from_blockchain,
            min_gas,
            addresses
        ))
    asyncio.create_task(process_new_txs_eth(
        args.eth,
        args.exclude_tx_contents,
        min_gas,
        addresses
    ))

    await asyncio.sleep(args.lead_time)
    while True:
        await asyncio.sleep(args.interval)
        trail_new_hashes.clear()
        await asyncio.sleep(args.trail_time)
        print(
            f"-----------------------------------------------------\n"
            f"Interval: {args.interval} seconds. "
            f"End time: {datetime.now().isoformat(sep=' ', timespec='seconds')}. "
            f"Minimum gas price: {args.min_gas_price} "
            f"{stats(seen_hashes, args.ignore_delta, args.verbose)}"
        )
        seen_hashes.clear()
        lead_new_hashes.clear()
        time_to_end_comparison = time.time() + args.interval

        num_intervals_passed += 1
        if num_intervals_passed == args.num_intervals:
            print(f"{num_intervals_passed} of {num_intervals} intervals complete. Exiting.")
            sys.exit(0)


async def process_new_txs_bdn(
    feed_name: str, exclude_tx_contents: bool, exclude_duplicates: bool, exclude_from_blockchain: bool,
    min_gas: Optional[int], addresses: List[str]
) -> None:
    options: Dict[str, Any] = {
        "duplicates": not exclude_duplicates,
        "include_from_blockchain": not exclude_from_blockchain
    }
    if exclude_tx_contents:
        options["include"] = ["tx_hash"]
    subscription_id = await ws_provider.subscribe(feed_name, options)

    while True:
        next_notification = await ws_provider.get_next_subscription_notification_by_id(subscription_id)
        time_received = time.time()
        transaction_hash = next_notification.notification["txHash"]

        if time_received < time_to_begin_comparison:
            lead_new_hashes.add(transaction_hash)
            continue

        if not exclude_tx_contents:
            tx_contents = next_notification.notification["txContents"]
            if addresses and tx_contents["from"] not in addresses and tx_contents["to"] not in addresses:
                continue
            if min_gas and int(tx_contents["gasPrice"], 16) < min_gas:
                low_fee_hashes.add(transaction_hash)
                continue

        if transaction_hash in seen_hashes:
            hash_entry = seen_hashes[transaction_hash]
            if hash_entry.gateway_time_received is None:
                hash_entry.gateway_time_received = time_received
        elif time_received < time_to_end_comparison and transaction_hash not in trail_new_hashes and transaction_hash not in lead_new_hashes:
            seen_hashes[transaction_hash] = HashEntry(transaction_hash, gateway_time_received=time.time())
        else:
            trail_new_hashes.add(transaction_hash)


async def process_new_txs_cloud_api(
    ws_uri: str, auth_header: str, feed_name: str, exclude_tx_contents: bool, exclude_duplicates: bool,
    exclude_from_blockchain: bool, min_gas: Optional[int], addresses: List[str]
) -> None:
    print(f"Initiating connection to: {ws_uri}")
    async with WsProvider(
        uri=ws_uri,
        headers={"Authorization": auth_header}
    ) as ws:
        print(f"websockets endpoint: {ws_uri} established")
        global ws_provider
        ws_provider = cast(WsProvider, ws)

        await process_new_txs_bdn(feed_name, exclude_tx_contents, exclude_duplicates, exclude_from_blockchain, min_gas, addresses)


async def process_new_txs_gateway(
    gateway_url: str, feed_name: str, exclude_tx_contents: bool, exclude_duplicates: bool, exclude_from_blockchain: bool,
    min_gas: Optional[int], addresses: List[str]
) -> None:
    print(f"Initiating connection to: {gateway_url}")
    async with WsProvider(gateway_url) as ws:
        print(f"websockets endpoint: {gateway_url} established")
        global ws_provider
        ws_provider = cast(WsProvider, ws)

        await process_new_txs_bdn(feed_name, exclude_tx_contents, exclude_duplicates, exclude_from_blockchain, min_gas, addresses)


async def process_new_txs_eth(
    eth_url: str, exclude_tx_contents: bool, min_gas: Optional[int], addresses: List[str]
) -> None:
    print(f"Initiating connection to: {eth_url}")
    async with EthWsSubscriber(eth_url) as eth_ws:
        print(f"websockets endpoint: {eth_url} established")

        global eth_ws_provider
        eth_ws_provider = cast(EthWsSubscriber, eth_ws)
        subscription_id = await eth_ws.subscribe("newPendingTransactions")

        while True:
            next_notification = await eth_ws.get_next_subscription_notification_by_id(subscription_id)
            time_received = time.time()
            transaction_hash = next_notification.notification

            if time_received < time_to_begin_comparison:
                lead_new_hashes.add(transaction_hash)
                continue

            if not exclude_tx_contents:
                asyncio.create_task(fetch_transaction_contents(transaction_hash, min_gas, addresses))
            elif transaction_hash in seen_hashes:
                hash_entry = seen_hashes[transaction_hash]
                if hash_entry.eth_node_time_received is None:
                    hash_entry.eth_node_time_received = time_received
            elif time_received < time_to_end_comparison and transaction_hash not in trail_new_hashes and transaction_hash not in lead_new_hashes:
                seen_hashes[transaction_hash] = HashEntry(transaction_hash, node_time_received=time_received)
            else:
                trail_new_hashes.add(transaction_hash)


async def fetch_transaction_contents(tx_hash: str, min_gas: Optional[int], addresses: List[str]) -> None:
    try:
        response = await eth_ws_provider.call_rpc(
            "eth_getTransactionByHash",
            [tx_hash]
        )
        tx_contents = response.result
        time_received = time.time()
        if tx_contents is None:
            return

        if addresses and tx_contents["from"] not in addresses and tx_contents["to"] not in addresses:
            return
        if min_gas and int(tx_contents["gasPrice"], 16) < min_gas:
            low_fee_hashes.add(tx_hash)
            return

        if tx_hash in seen_hashes:
            hash_entry = seen_hashes[tx_hash]
            if hash_entry.eth_node_time_received is None:
                hash_entry.eth_node_time_received = time_received
        elif tx_contents is not None and time_received < time_to_end_comparison and tx_hash not in trail_new_hashes and tx_hash not in lead_new_hashes:
            seen_hashes[tx_hash] = HashEntry(tx_hash, node_time_received=time.time())
        elif tx_contents is not None:
            trail_new_hashes.add(tx_hash)
    except Exception:
        print(f"Attempt to fetch transaction contents for {tx_hash} failed. Abandoning.")
        pass


def stats(seen_hashes: Dict[str, HashEntry], ignore_delta: int, verbose: bool) -> str:
    tx_seen_by_both_feeds_gateway_first = 0
    tx_seen_by_both_feeds_eth_node_first = 0
    tx_received_by_gateway_first_total_delta = 0
    tx_received_by_eth_node_first_total_delta = 0
    new_tx_from_gateway_feed_first = 0
    new_tx_from_eth_node_feed_first = 0
    total_tx_from_gateway = 0
    total_tx_from_eth_node = 0

    for tx_hash, hash_entry in seen_hashes.items():
        if hash_entry.gateway_time_received is None:
            eth_node_time_received = hash_entry.eth_node_time_received
            assert isinstance(eth_node_time_received, float)

            if missing_hashes_file:
                f = missing_hashes_file
                assert f is not None
                f.write(f"{tx_hash}\n")
            if all_hashes_file:
                f = all_hashes_file
                assert f is not None
                f.write(f"{tx_hash}, 0, {datetime.fromtimestamp(eth_node_time_received)}\n")
            new_tx_from_eth_node_feed_first += 1
            total_tx_from_eth_node += 1
            continue
        if hash_entry.eth_node_time_received is None:
            gateway_time_received = hash_entry.gateway_time_received
            assert isinstance(gateway_time_received, float)

            if all_hashes_file:
                f = all_hashes_file
                assert f is not None
                f.write(f"{tx_hash}, {datetime.fromtimestamp(gateway_time_received)}, 0\n")
            new_tx_from_gateway_feed_first += 1
            total_tx_from_gateway += 1
            continue

        eth_node_time_received = hash_entry.eth_node_time_received
        gateway_time_received = hash_entry.gateway_time_received
        assert isinstance(eth_node_time_received, float)
        assert isinstance(gateway_time_received, float)

        total_tx_from_gateway += 1
        total_tx_from_eth_node += 1

        if abs(gateway_time_received - eth_node_time_received) > ignore_delta:
            high_delta_hashes.add(hash_entry.tx_hash)
            continue

        if all_hashes_file:
            f = all_hashes_file
            assert f is not None
            f.write(
                f"{tx_hash}, "
                f"{datetime.fromtimestamp(gateway_time_received)}, "
                f"{datetime.fromtimestamp(eth_node_time_received)}\n"
            )

        if gateway_time_received < eth_node_time_received:
            new_tx_from_gateway_feed_first += 1
            tx_seen_by_both_feeds_gateway_first += 1
            tx_received_by_gateway_first_total_delta += eth_node_time_received - gateway_time_received
        elif eth_node_time_received < gateway_time_received:
            new_tx_from_eth_node_feed_first += 1
            tx_seen_by_both_feeds_eth_node_first += 1
            tx_received_by_eth_node_first_total_delta += gateway_time_received - eth_node_time_received

    new_tx_seen_by_both_feeds = tx_seen_by_both_feeds_gateway_first + tx_seen_by_both_feeds_eth_node_first
    tx_received_by_gw_first_avg_delta = 0 if tx_seen_by_both_feeds_gateway_first == 0 \
        else round((tx_received_by_gateway_first_total_delta / float(tx_seen_by_both_feeds_gateway_first) * 1000), 1)
    tx_received_by_eth_node_first_avg_delta = 0 if tx_seen_by_both_feeds_eth_node_first == 0 \
        else round((tx_received_by_eth_node_first_total_delta / float(tx_seen_by_both_feeds_eth_node_first) * 1000), 1)
    percentage_tx_seen_first_by_gateway = 0 if new_tx_seen_by_both_feeds == 0 \
        else int((tx_seen_by_both_feeds_gateway_first / new_tx_seen_by_both_feeds) * 100)

    results_str = (
        f"\nAnalysis of Transactions received on both feeds:\n"
        f"Number of transactions: {new_tx_seen_by_both_feeds}\n"
        f"Number of transactions received from Gateway first: {tx_seen_by_both_feeds_gateway_first}\n"
        f"Number of transactions received from Ethereum node first: {tx_seen_by_both_feeds_eth_node_first}\n"
        f"Percentage of transactions seen first from gateway: {percentage_tx_seen_first_by_gateway}%\n"
        f"Average time difference for transactions received first from gateway (ms): {tx_received_by_gw_first_avg_delta}\n"
        f"Average time difference for transactions received first from Ethereum node (ms): {tx_received_by_eth_node_first_avg_delta}\n"

        f"\nTotal Transactions summary:\n"
        f"Total tx from gateway: {total_tx_from_gateway}\n"
        f"Total tx from eth node: {total_tx_from_eth_node}\n"
        f"Number of low fee tx ignored: {len(low_fee_hashes)}\n"
    )

    verbose_results_str = (
        f"Number of high delta tx ignored: {len(high_delta_hashes)}\n"
        f"Number of new transactions received first from gateway: {new_tx_from_gateway_feed_first}\n"
        f"Number of new transactions received first from node: {new_tx_from_eth_node_feed_first}\n"
        f"Total number of transactions seen: {new_tx_from_eth_node_feed_first + new_tx_from_gateway_feed_first}\n"
    )

    if verbose:
        results_str = results_str + verbose_results_str
    return results_str


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=str, default="ws://127.0.0.1:28333")
    parser.add_argument("--eth", type=str, default="ws://127.0.0.1:8546")
    parser.add_argument("--feed-name", type=str, default="newTxs", choices=["newTxs", "pendingTxs", "transactionStatus"])
    parser.add_argument("--min-gas-price", type=float, default=None, help="Gas price in gigawei")
    parser.add_argument("--addresses", type=str, default=None, help="Comma separated list of Ethereum addresses")
    parser.add_argument("--exclude-tx-contents", action="store_true")
    parser.add_argument("--interval", type=int, default=600, help="Length of feed sample interval in seconds")
    parser.add_argument("--num-intervals", type=int, default=6)
    parser.add_argument("--lead-time", type=int, default=60, help="Seconds to wait before starting to compare feeds")
    parser.add_argument("--trail-time", type=int, default=60, help="Seconds to wait after interval to receive tx on both feeds")
    parser.add_argument("--dump", type=str, default="", choices=["ALL", "MISSING", "ALL,MISSING"])
    parser.add_argument("--exclude-duplicates", action="store_true", default=True, help="For pendingTxs only")
    parser.add_argument("--ignore-delta", type=int, default=5, help="Ignore tx with delta above this amount (seconds)")
    parser.add_argument("--use-cloud-api", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--exclude-from-blockchain", action="store_true")
    parser.add_argument("--cloud-api-ws-uri", type=str, default="wss://api.blxrbdn.com/ws")
    parser.add_argument("--auth-header", type=str, help="Authorization header created with account id and password")
    return parser


def run_main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print(f"{num_intervals_passed} of {num_intervals} intervals complete. Exiting.")
        sys.exit(0)
