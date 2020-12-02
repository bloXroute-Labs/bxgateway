import argparse
import asyncio
import sys
import time
from datetime import datetime
from typing import Optional, Dict, TextIO, Set, cast

from bloxroute_cli.provider.cloud_wss_provider import CloudWssProvider
from bloxroute_cli.provider.ws_provider import WsProvider
from bxcommon.rpc.external.eth_ws_subscriber import EthWsSubscriber


class HashEntry:
    block_hash = None
    gateway_time_received: Optional[float] = None
    eth_node_time_received: Optional[float] = None

    def __init__(
        self, block_hash: str, gateway_time_received: Optional[float] = None, node_time_received: Optional[float] = None
    ) -> None:
        self.block_hash = block_hash
        self.gateway_time_received = gateway_time_received
        self.eth_node_time_received = node_time_received

    def __eq__(self, other) -> bool:
        return (
            isinstance(other, HashEntry)
            and other.block_hash == self.block_hash
        )


ws_provider: WsProvider
eth_ws_provider: EthWsSubscriber
time_to_begin_comparison = 0.0
time_to_end_comparison = 0.0
all_hashes_file: TextIO
missing_hashes_file: TextIO
trail_new_hashes: Set[str] = set()
lead_new_hashes: Set[str] = set()
seen_hashes: Dict[str, HashEntry] = {}
num_intervals_passed = 0
num_intervals: Optional[int] = None


async def main() -> None:
    global seen_hashes
    global trail_new_hashes
    global time_to_begin_comparison
    global time_to_end_comparison
    global num_intervals_passed
    global num_intervals

    parser = get_argument_parser()
    args = parser.parse_args()

    global all_hashes_file
    if "ALL" in args.dump:
        all_hashes_file = cast(TextIO, open("all_block_hashes.csv", "w"))
        all_hashes_file.write("block-hash,blxr Time,eth Time\n")
    else:
        all_hashes_file = cast(TextIO, None)

    global missing_hashes_file
    if "MISSING" in args.dump:
        missing_hashes_file = cast(TextIO, open("missing_block_hashes.txt", "w"))
    else:
        missing_hashes_file = cast(TextIO, None)

    time_to_begin_comparison = time.time() + args.lead_time
    time_to_end_comparison = time_to_begin_comparison + args.interval
    num_intervals = args.num_intervals

    if args.use_cloud_api:
        asyncio.create_task(process_new_blocks_cloud_api(
            args.cloud_api_ws_uri,
            args.auth_header,
            args.feed_name,
            args.exclude_block_contents
        ))
    else:
        asyncio.create_task(process_new_blocks_gateway(
            args.gateway,
            args.feed_name,
            args.exclude_block_contents
        ))
    asyncio.create_task(process_new_blocks_eth(
        args.eth,
        args.exclude_block_contents
    ))

    await asyncio.sleep(args.lead_time)
    while True:
        await asyncio.sleep(args.interval)
        trail_new_hashes.clear()
        await asyncio.sleep(args.trail_time)
        print(
            f"-----------------------------------------------------\n"
            f"Interval: {args.interval} seconds. "
            f"End time: {datetime.now().isoformat(sep=' ', timespec='seconds')} {stats(args.ignore_delta)}"
        )
        seen_hashes.clear()
        lead_new_hashes.clear()
        time_to_end_comparison = time.time() + args.interval

        num_intervals_passed += 1
        if num_intervals_passed == args.num_intervals:
            print(f"{num_intervals_passed} of {num_intervals} intervals complete. Exiting.")
            sys.exit(0)


async def process_new_blocks_bdn(feed_name: str, exclude_block_contents: bool):
    options = None
    if exclude_block_contents:
        options = {"include": ["hash", "header"]}

    subscription_id = await ws_provider.subscribe(feed_name, options)

    while True:
        next_notification = await ws_provider.get_next_subscription_notification_by_id(subscription_id)
        time_received = time.time()
        block_hash = next_notification.notification["hash"]

        if time_received < time_to_begin_comparison:
            lead_new_hashes.add(block_hash)
            continue

        if block_hash in seen_hashes:
            hash_entry = seen_hashes[block_hash]
            if hash_entry.gateway_time_received is None:
                hash_entry.gateway_time_received = time_received
        elif time_received < time_to_end_comparison and block_hash not in trail_new_hashes and block_hash not in lead_new_hashes:
            seen_hashes[block_hash] = HashEntry(block_hash, gateway_time_received=time.time())
        else:
            trail_new_hashes.add(block_hash)


async def process_new_blocks_cloud_api(
    ws_uri: str, auth_header: str, feed_name: str, exclude_block_contents: bool
) -> None:
    print(f"Initiating connection to: {ws_uri}")
    async with WsProvider(
        uri=ws_uri,
        headers={"Authorization": auth_header}
    ) as ws:
        print(f"websockets endpoint: {ws_uri} established")
        global ws_provider
        ws_provider = cast(CloudWssProvider, ws)

        await process_new_blocks_bdn(feed_name, exclude_block_contents)


async def process_new_blocks_gateway(gateway_url: str, feed_name: str, exclude_block_contents: bool) -> None:
    print(f"Initiating connection to: {gateway_url}")
    async with WsProvider(gateway_url) as ws:
        print(f"websockets endpoint: {gateway_url} established")
        global ws_provider
        ws_provider = cast(WsProvider, ws)

        await process_new_blocks_bdn(feed_name, exclude_block_contents)


def handle_eth_block_hash(block_hash, time_received: float):
    if block_hash in seen_hashes:
        hash_entry = seen_hashes[block_hash]
        if hash_entry.eth_node_time_received is None:
            hash_entry.eth_node_time_received = time_received
    elif time_received < time_to_end_comparison and block_hash not in trail_new_hashes and block_hash not in lead_new_hashes:
        seen_hashes[block_hash] = HashEntry(block_hash, node_time_received=time_received)
    else:
        trail_new_hashes.add(block_hash)


async def process_new_blocks_eth(eth_url: str, exclude_block_contents: bool) -> None:
    print(f"Initiating connection to: {eth_url}")
    async with EthWsSubscriber(eth_url) as eth_ws:
        print(f"websockets endpoint: {eth_url} established")

        global eth_ws_provider
        eth_ws_provider = cast(EthWsSubscriber, eth_ws)
        subscription_id = await eth_ws.subscribe("newHeads")

        while True:
            next_notification = await eth_ws.get_next_subscription_notification_by_id(subscription_id)
            time_received = time.time()

            header = next_notification.notification
            block_hash = header["hash"]

            if time_received < time_to_begin_comparison:
                lead_new_hashes.add(block_hash)
                continue

            if not exclude_block_contents:
                asyncio.create_task(fetch_block(block_hash))
            else:
                handle_eth_block_hash(block_hash, time_received)


async def fetch_block(block_hash: str) -> None:
    try:
        response = await eth_ws_provider.call_rpc(
            "eth_getBlockByHash",
            [block_hash, True]
        )
        time_received = time.time()
        block = response.result
        if block is None:
            return

        handle_eth_block_hash(block_hash, time_received)

    except Exception:
        print(f"Attempt to fetch full block for {block_hash} failed. Abandoning.")
        pass


def stats(ignore_delta: int) -> str:
    blocks_seen_by_both_feeds_gateway_first = 0
    blocks_seen_by_both_feeds_eth_node_first = 0
    block_received_by_gateway_first_total_delta = 0
    block_received_by_eth_node_first_total_delta = 0
    new_blocks_from_gateway_feed_first = 0
    new_blocks_from_eth_node_feed_first = 0
    total_blocks_from_gateway = 0
    total_blocks_from_eth_node = 0

    for block_hash, hash_entry in seen_hashes.items():
        if hash_entry.gateway_time_received is None:
            eth_node_time_received = hash_entry.eth_node_time_received
            assert isinstance(eth_node_time_received, float)

            if missing_hashes_file:
                missing_hashes_file.write(f"{block_hash}\n")
            if all_hashes_file:
                all_hashes_file.write(f"{block_hash}, 0, {datetime.fromtimestamp(eth_node_time_received)}\n")
            new_blocks_from_eth_node_feed_first += 1
            total_blocks_from_eth_node += 1
            continue
        if hash_entry.eth_node_time_received is None:
            gateway_time_received = hash_entry.gateway_time_received
            assert isinstance(gateway_time_received, float)

            if all_hashes_file:
                all_hashes_file.write(f"{block_hash}, {datetime.fromtimestamp(gateway_time_received)}, 0\n")
            new_blocks_from_gateway_feed_first += 1
            total_blocks_from_gateway += 1
            continue

        eth_node_time_received = hash_entry.eth_node_time_received
        gateway_time_received = hash_entry.gateway_time_received
        assert isinstance(eth_node_time_received, float)
        assert isinstance(gateway_time_received, float)

        total_blocks_from_gateway += 1
        total_blocks_from_eth_node += 1

        if all_hashes_file:
            all_hashes_file.write(
                f"{block_hash},"
                f"{datetime.fromtimestamp(gateway_time_received)},"
                f"{datetime.fromtimestamp(eth_node_time_received)}\n"
            )

        if abs(gateway_time_received - eth_node_time_received) > ignore_delta:
            continue

        if gateway_time_received < eth_node_time_received:
            new_blocks_from_gateway_feed_first += 1
            blocks_seen_by_both_feeds_gateway_first += 1
            block_received_by_gateway_first_total_delta += eth_node_time_received - gateway_time_received
        elif eth_node_time_received < gateway_time_received:
            new_blocks_from_eth_node_feed_first += 1
            blocks_seen_by_both_feeds_eth_node_first += 1
            block_received_by_eth_node_first_total_delta += gateway_time_received - eth_node_time_received

    new_block_seen_by_both_feeds = blocks_seen_by_both_feeds_gateway_first + blocks_seen_by_both_feeds_eth_node_first
    block_received_by_gw_first_avg_delta = 0 if blocks_seen_by_both_feeds_gateway_first == 0 \
        else round((block_received_by_gateway_first_total_delta / float(blocks_seen_by_both_feeds_gateway_first) * 1000), 1)
    block_received_by_eth_node_first_avg_delta = 0 if blocks_seen_by_both_feeds_eth_node_first == 0 \
        else round((block_received_by_eth_node_first_total_delta / float(blocks_seen_by_both_feeds_eth_node_first) * 1000), 1)
    percentage_block_seen_first_by_gateway = 0 if new_block_seen_by_both_feeds == 0 \
        else int((blocks_seen_by_both_feeds_gateway_first / new_block_seen_by_both_feeds) * 100)

    return (
        f"\nBlocks summary:\n"
        f"Number of new blocks received first from gateway: {new_blocks_from_gateway_feed_first}\n"
        f"Number of new blocks received first from node: {new_blocks_from_eth_node_feed_first}\n"
        f"Total number of blocks seen: {new_blocks_from_eth_node_feed_first + new_blocks_from_gateway_feed_first}\n"
        f"Total blocks from gateway: {total_blocks_from_gateway}\n"
        f"Total blocks from eth node: {total_blocks_from_eth_node}\n"

        f"\nAnalysis of Blocks received on both feeds:\n"
        f"Number of blocks: {new_block_seen_by_both_feeds}\n"
        f"Number of blocks received from Gateway first: {blocks_seen_by_both_feeds_gateway_first}\n"
        f"Number of blocks received from Ethereum node first: {blocks_seen_by_both_feeds_eth_node_first}\n"
        f"Percentage of blocks seen first from gateway: {percentage_block_seen_first_by_gateway}%\n"
        f"Average time difference for blocks received first from gateway (ms): {block_received_by_gw_first_avg_delta}\n"
        f"Average time difference for blocks received first from Ethereum node (ms): {block_received_by_eth_node_first_avg_delta}\n"
    )


def get_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway", type=str, default="ws://127.0.0.1:28333")
    parser.add_argument("--eth", type=str, default="ws://127.0.0.1:8546")
    parser.add_argument("--feed-name", type=str, default="newBlocks", choices=["newBlocks"])
    parser.add_argument("--exclude-block-contents", action="store_true")
    parser.add_argument("--interval", type=int, default=600, help="Length of feed sample interval in seconds")
    parser.add_argument("--num-intervals", type=int, default=6)
    parser.add_argument("--lead-time", type=int, default=60, help="Seconds to wait before starting to compare feeds")
    parser.add_argument("--trail-time", type=int, default=60, help="Seconds to wait after interval to receive block on both feeds")
    parser.add_argument("--dump", type=str, default="", choices=["ALL", "MISSING", "ALL,MISSING"])
    parser.add_argument("--ignore-delta", type=int, default=5, help="Ignore block with delta above this amount (seconds)")
    parser.add_argument("--use-cloud-api", action="store_true")
    parser.add_argument("--auth-header", type=str, help="Authorization header created with account id and password")
    parser.add_argument("--cloud-api-ws-uri", type=str, default="wss://api.blxrbdn.com/ws")
    parser.add_argument("--verbose", action="store_true")
    return parser


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print(f"{num_intervals_passed} of {num_intervals} intervals complete. Exiting.")
        sys.exit(0)
