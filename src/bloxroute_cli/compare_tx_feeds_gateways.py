import argparse
import asyncio
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Set, List, Any, cast

from typing import IO

from bloxroute_cli.provider.ws_provider import WsProvider

class HashEntry:
    tx_hash: str
    gateway1_time_received: Optional[float] = None
    gateway2_time_received: Optional[float] = None

    def __init__(
        self, tx_hash: str, gateway1_time_received: Optional[float] = None, gateway2_time_received: Optional[float] = None
    ) -> None:
        self.tx_hash = tx_hash
        self.gateway1_time_received = gateway1_time_received
        self.gateway2_time_received = gateway2_time_received

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, HashEntry)
            and other.tx_hash == self.tx_hash
        )


ws_provider: WsProvider
light_gw_ws_provider: WsProvider
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
        f.write("tx-hash,blxr1 Time,blxr2 Time\n")

    global missing_hashes_file
    if "MISSING" in args.dump:
        missing_hashes_file = open("missing_hashes.txt", "w")

    time_to_begin_comparison = time.time() + args.lead_time
    time_to_end_comparison = time_to_begin_comparison + args.interval
    num_intervals = args.num_intervals

    asyncio.create_task(process_new_txs_gateway(
        args.gateway1,
        args.auth_header1,
        args.feed_name,
        args.exclude_tx_contents,
        args.exclude_duplicates,
        args.exclude_from_blockchain,
        min_gas,
        addresses,
        args.use_light_gateway1
    ))
    asyncio.create_task(process_new_txs_gateway(
        args.gateway2,
        args.auth_header2,
        args.feed_name,
        args.exclude_tx_contents,
        args.exclude_duplicates,
        args.exclude_from_blockchain,
        min_gas,
        addresses,
        args.use_light_gateway2
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
    min_gas: Optional[int], addresses: List[str], use_light_gateway: bool = False
) -> None:
    options: Dict[str, Any] = {}
    if not use_light_gateway:
        options = {
            "duplicates": not exclude_duplicates,
            "include_from_blockchain": not exclude_from_blockchain
        }

    if exclude_tx_contents:
        options["include"] = ["tx_hash"]
    else:
        options["include"] = ["tx_hash", "tx_contents"]
    if use_light_gateway:
        subscription_id = await light_gw_ws_provider.subscribe(feed_name, options)
    else:
        subscription_id = await ws_provider.subscribe(feed_name, options)

    while True:
        if use_light_gateway:
            next_notification = await light_gw_ws_provider.get_next_subscription_notification_by_id(subscription_id)
        else:
            next_notification = await ws_provider.get_next_subscription_notification_by_id(subscription_id)
        time_received = time.time()
        transaction_hash = next_notification.notification["txHash"]

        if time_received < time_to_begin_comparison:
            lead_new_hashes.add(transaction_hash)
            continue

        if not exclude_tx_contents:
            tx_contents = next_notification.notification["txContents"]
            if (
                addresses
                and "to" in next_notification.notification["txContents"]
                and tx_contents["to"] not in addresses
            ):
                continue
            if min_gas and int(tx_contents["gasPrice"], 16) < min_gas:
                low_fee_hashes.add(transaction_hash)
                continue

        if transaction_hash in seen_hashes:
            hash_entry = seen_hashes[transaction_hash]
            if not use_light_gateway:
                if hash_entry.gateway1_time_received is None:
                    hash_entry.gateway1_time_received = time_received
            else:
                if hash_entry.gateway2_time_received is None:
                    hash_entry.gateway2_time_received = time_received
        elif time_received < time_to_end_comparison and transaction_hash not in trail_new_hashes and transaction_hash not in lead_new_hashes:
            if not use_light_gateway:
                seen_hashes[transaction_hash] = HashEntry(transaction_hash, gateway1_time_received=time.time())
            else:
                seen_hashes[transaction_hash] = HashEntry(transaction_hash, gateway2_time_received=time.time())
        else:
            trail_new_hashes.add(transaction_hash)


async def process_new_txs_gateway(
    gateway_url: str, auth_header: str, feed_name: str, exclude_tx_contents: bool, exclude_duplicates: bool, exclude_from_blockchain: bool,
    min_gas: Optional[int], addresses: List[str], use_light_gateway: bool
) -> None:
    print(f"Initiating connection to: {gateway_url}")
    headers = {"Authorization": auth_header}
    async with WsProvider(gateway_url, headers=headers) as ws:
        print(f"websockets endpoint: {gateway_url} established")
        global ws_provider
        global light_gw_ws_provider
        if use_light_gateway:
            light_gw_ws_provider = cast(WsProvider, ws)
        else:
            ws_provider = cast(WsProvider, ws)

        await process_new_txs_bdn(feed_name, exclude_tx_contents, exclude_duplicates, exclude_from_blockchain, min_gas, addresses, use_light_gateway)


def stats(seen_hashes: Dict[str, HashEntry], ignore_delta: int, verbose: bool) -> str:
    tx_seen_by_both_feeds_gateway1_first = 0
    tx_seen_by_both_feeds_gateway2_first = 0
    tx_received_by_gateway1_first_total_delta = 0
    tx_received_by_gateway2_first_total_delta = 0
    new_tx_from_gateway1_feed_first = 0
    new_tx_from_gateway2_feed_first = 0
    total_tx_from_gateway1 = 0
    total_tx_from_gateway2 = 0

    for tx_hash, hash_entry in seen_hashes.items():
        if hash_entry.gateway1_time_received is None:
            gateway2_time_received = hash_entry.gateway2_time_received
            assert isinstance(gateway2_time_received, float)

            if missing_hashes_file:
                f = missing_hashes_file
                assert f is not None
                f.write(f"{tx_hash}\n")
            if all_hashes_file:
                f = all_hashes_file
                assert f is not None
                f.write(f"{tx_hash}, 0, {datetime.fromtimestamp(gateway2_time_received)}\n")
            new_tx_from_gateway2_feed_first += 1
            total_tx_from_gateway2 += 1
            continue
        if hash_entry.gateway2_time_received is None:
            gateway1_time_received = hash_entry.gateway1_time_received
            assert isinstance(gateway1_time_received, float)

            if all_hashes_file:
                f = all_hashes_file
                assert f is not None
                f.write(f"{tx_hash}, {datetime.fromtimestamp(gateway1_time_received)}, 0\n")
            new_tx_from_gateway1_feed_first += 1
            total_tx_from_gateway1 += 1
            continue

        gateway2_time_received = hash_entry.gateway2_time_received
        gateway1_time_received = hash_entry.gateway1_time_received
        assert isinstance(gateway2_time_received, float)
        assert isinstance(gateway1_time_received, float)

        total_tx_from_gateway1 += 1
        total_tx_from_gateway2 += 1

        if abs(gateway1_time_received - gateway2_time_received) > ignore_delta:
            high_delta_hashes.add(hash_entry.tx_hash)
            continue

        if all_hashes_file:
            f = all_hashes_file
            assert f is not None
            f.write(
                f"{tx_hash}, "
                f"{datetime.fromtimestamp(gateway1_time_received)}, "
                f"{datetime.fromtimestamp(gateway2_time_received)}\n"
            )

        if gateway1_time_received < gateway2_time_received:
            new_tx_from_gateway1_feed_first += 1
            tx_seen_by_both_feeds_gateway1_first += 1
            tx_received_by_gateway1_first_total_delta += gateway2_time_received - gateway1_time_received
        elif gateway2_time_received < gateway1_time_received:
            new_tx_from_gateway2_feed_first += 1
            tx_seen_by_both_feeds_gateway2_first += 1
            tx_received_by_gateway2_first_total_delta += gateway1_time_received - gateway2_time_received

    new_tx_seen_by_both_feeds = tx_seen_by_both_feeds_gateway1_first + tx_seen_by_both_feeds_gateway2_first
    tx_received_by_gw1_first_avg_delta = 0 if tx_seen_by_both_feeds_gateway1_first == 0 \
        else round((tx_received_by_gateway1_first_total_delta / float(tx_seen_by_both_feeds_gateway1_first) * 1000), 1)
    tx_received_by_gw2_first_avg_delta = 0 if tx_seen_by_both_feeds_gateway2_first == 0 \
        else round((tx_received_by_gateway2_first_total_delta / float(tx_seen_by_both_feeds_gateway2_first) * 1000), 1)
    percentage_tx_seen_first_by_gateway = 0 if new_tx_seen_by_both_feeds == 0 \
        else int((tx_seen_by_both_feeds_gateway1_first / new_tx_seen_by_both_feeds) * 100)

    results_str = (
        f"\nAnalysis of Transactions received on both feeds:\n"
        f"Number of transactions: {new_tx_seen_by_both_feeds}\n"
        f"Number of transactions received from Gateway1 first: {tx_seen_by_both_feeds_gateway1_first}\n"
        f"Number of transactions received from Gateway2 first: {tx_seen_by_both_feeds_gateway2_first}\n"
        f"Percentage of transactions seen first from Gateway1: {percentage_tx_seen_first_by_gateway}%\n"
        f"Average time difference for transactions received first from Gateway1 (ms): {tx_received_by_gw1_first_avg_delta}\n"
        f"Average time difference for transactions received first from Gateway2 (ms): {tx_received_by_gw2_first_avg_delta}\n"

        f"\nTotal Transactions summary:\n"
        f"Total tx from Gateway1: {total_tx_from_gateway1}\n"
        f"Total tx from Gateway2: {total_tx_from_gateway2}\n"
        f"Number of low fee tx ignored: {len(low_fee_hashes)}\n"
    )

    verbose_results_str = (
        f"Number of high delta tx ignored: {len(high_delta_hashes)}\n"
        f"Number of new transactions received first from Gateway1: {new_tx_from_gateway1_feed_first}\n"
        f"Number of new transactions received first from Gateway2: {new_tx_from_gateway2_feed_first}\n"
        f"Total number of transactions seen: {new_tx_from_gateway2_feed_first + new_tx_from_gateway1_feed_first}\n"
    )

    if verbose:
        results_str = results_str + verbose_results_str
    return results_str


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway1", type=str, default="ws://127.0.0.1:28333/ws")
    parser.add_argument("--gateway2", type=str, default="ws://127.0.0.1:28344/ws")
    parser.add_argument("--feed-name", type=str, default="newTxs", choices=["newTxs", "pendingTxs", "transactionStatus"])
    parser.add_argument("--min-gas-price", type=float, default=None, help="Gas price in gigawei")
    parser.add_argument("--addresses", type=str, default=None, help="Comma separated list of Node addresses")
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
    parser.add_argument("--auth-header1", type=str, help="Authorization header created with account id and password for python gateway")
    parser.add_argument("--auth-header2", type=str, help="Authorization header created with account id and password for light gateway")
    parser.add_argument("--use-light-gateway1", action="store_true")
    parser.add_argument("--use-light-gateway2", action="store_true")

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
