# Transaction Feed

bloXroute Gateway supports publish / subscribe to new transactions seen in the BDN.
These messages are sent as JSON-RPC notifications over the gateway's websocket
RPC server.

Example:
```
# create subscription
>>> {"id": 1, "method": "subscribe", "params": ["newTxs", {"include": ["tx_hash"], "duplicates": false}]}
<<< {"jsonrpc": "2.0", "id": 1", "result": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8"}
# notification
<<< {"jsonrpc": "2.0", "id": null, "method": "subscribe", "params": {"subscription": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8", "result": {"tx_hash": "14f3d7a1553b252b896fa4ea9d0b6ee78c6d4541fa6bfbc7fc664fde3214f3de", "tx_contents": {"from": "0xa7a7899d944fe658c4b0a1803bab2f490bd3849e", "gas": 400000, "gas_price": 50000000000, "hash": "0x14f3d7a1553b252b896fa4ea9d0b6ee78c6d4541fa6bfbc7fc664fde3214f3de", "input": "0xef3435880000000000000000000000000000000000000000000000000627bd1c0c4602e00000000000000000000000000000000000000000000003cfc82e37e9a740000000000000000000000000000000000000000000000000000000000000000186a000000000000000000000000000000000000000000000000000000172b8d056ec0000000000000000000000000000000000000000000000000627bd1c0c4602e00000000000000000000000000000000000000000000000000000000000000fa7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000577247fffa1a7100000000000000000000000000000000000000000000000000000000000000000000000000000000000000004730fb1463a6f1f44aeb45f6c5c422427f37f4d0000000000000000000000000af57021337aa6ffb1a66cae78e6272fff6cb9739000000000000000000000000ce39c3059fff13edcbe011f5653fd193f5520fc5000000000000000000000000000000000000000000000000000000000000001c000000000000000000000000000000000000000000000000000000000000001b4db908a89ac4667ea45c70e39c4301c186bb139a71fdcb938847d0bb67430d883e54e6c63caef62541145beab43cd34388fae0e5c06889e72d6eb73743fac0a902d290f2d7a26f7677cce503ec9bfcd1cd5fbfea085eec07874af797cf64e6534420e7ed52eb04f1efe5b3ba7e9aafdf00da6fa9648107a78344d415cd39e915", "nonce": 5349614, "to": "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208", "value": 0, "v": "0x26", "r": "0x56f7c0ed99aed122751ec456ec95fac33694ab95144fc393ca13c36c738c3d27", "s": "0x46ef66da559ab73620bdf9b34e32b5ec680fe038af53e85bffc7094cc55dc0cc"}}}}
# cancel subscription
>>> {"id": 1, "method": "unsubscribe", "params": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8"}
<<< {"jsonrpc": "2.0", "id": 1", "result": true}
```

## Enabling websocket RPC

Start the bloXroute Gateway with the following additional arguments:

```
--ws True
```

You can also optionally specify the host and port that the server will start on:

```
--ws-host 127.0.0.1 --ws-port 28333
```

## Creating a subscription

Subscriptions are created with the RPC call `subscribe`, with the subscription name
and subscription options as parameters. Subscription options currently support filtering
which fields you would like to be included in the notification, and if you would like
duplicate transactions included (typically very low fee transactions being repeatedly 
accepted into the mempool). The currently available options are `tx_hash` and 
`tx_contents`. Omitting these options will include all fields, but filter out duplicate transactions.
`subscribe` return a subscription ID that all related notifications will be paired with.

```
>>> {"id": 1, "method": "subscribe", "params": ["newTxs", {"include": ["tx_hash"], "duplicates": false}]}
<<< {"jsonrpc": "2.0", "id": 1", "result": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8"}
```

## Cancelling a Subscription

Subscriptions are cancelled with the RPC call `unsubscribe` with the subscription ID.

```
>>> {"id": 1, "method": "subscribe", "params": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8"}
<<< {"jsonrpc": "2.0", "id": 1", "result": true}
```

## Available Subscriptions

The bloXroute Gateway currently supports two subscription feeds: `newTxs` and `pendingTxs`.

By design, the bloXroute Gateway does not perform the same detail of transaction
validation that most blockchain nodes do, and cannot completely guarantee that
all transactions propagated are valid (e.g. the gateway does not check for double spends).

For expedience, all transactions received through the BDN are immediately published 
to the `newTxs` feed. These transactions have had basic validations done (e.g. checksums and other
sanity checks) but may not be accepted into the mempool.

The bloXroute Gateway will then leverage the BDN for further validation of the transaction
(e.g. check that it will be accepted to the mempool), and publish results to the 
`pendingTxs` feed. You can optionally enable validation against your local Ethereum node
as well, which may be faster in some scenarios (see the section below).


Both feeds will look like this:

```
{"jsonrpc": "2.0", "id": null, "method": "subscribe", "params": {"subscription": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8", "result": {"tx_hash": "14f3d7a1553b252b896fa4ea9d0b6ee78c6d4541fa6bfbc7fc664fde3214f3de", "tx_contents": {"from": "0xa7a7899d944fe658c4b0a1803bab2f490bd3849e", "gas": 400000, "gas_price": 50000000000, "hash": "0x14f3d7a1553b252b896fa4ea9d0b6ee78c6d4541fa6bfbc7fc664fde3214f3de", "input": "0xef3435880000000000000000000000000000000000000000000000000627bd1c0c4602e00000000000000000000000000000000000000000000003cfc82e37e9a740000000000000000000000000000000000000000000000000000000000000000186a000000000000000000000000000000000000000000000000000000172b8d056ec0000000000000000000000000000000000000000000000000627bd1c0c4602e00000000000000000000000000000000000000000000000000000000000000fa7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000577247fffa1a7100000000000000000000000000000000000000000000000000000000000000000000000000000000000000004730fb1463a6f1f44aeb45f6c5c422427f37f4d0000000000000000000000000af57021337aa6ffb1a66cae78e6272fff6cb9739000000000000000000000000ce39c3059fff13edcbe011f5653fd193f5520fc5000000000000000000000000000000000000000000000000000000000000001c000000000000000000000000000000000000000000000000000000000000001b4db908a89ac4667ea45c70e39c4301c186bb139a71fdcb938847d0bb67430d883e54e6c63caef62541145beab43cd34388fae0e5c06889e72d6eb73743fac0a902d290f2d7a26f7677cce503ec9bfcd1cd5fbfea085eec07874af797cf64e6534420e7ed52eb04f1efe5b3ba7e9aafdf00da6fa9648107a78344d415cd39e915", "nonce": 5349614, "to": "0x2a0c0dbecc7e4d658f48e01e3fa353f44050c208", "value": 0, "v": "0x26", "r": "0x56f7c0ed99aed122751ec456ec95fac33694ab95144fc393ca13c36c738c3d27", "s": "0x46ef66da559ab73620bdf9b34e32b5ec680fe038af53e85bffc7094cc55dc0cc"}}}}
{"jsonrpc": "2.0", "id": null, "method": "subscribe", "params": {"subscription": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8", "result": {"tx_hash": "d7a4ed5ce46dc7438649f52e693129bffc1059c55b11e50b389508f9a8bf9ba4", "tx_contents": {"from": "0xcb756522ec37cd247da16aef9d3a44914d639875", "gas": 60000, "gas_price": 38000001235, "hash": "0xd7a4ed5ce46dc7438649f52e693129bffc1059c55b11e50b389508f9a8bf9ba4", "input": "0xa9059cbb0000000000000000000000005cbba25357cb42c6e31d7eb3182372bced8f1e3f00000000000000000000000000000000000000000000001b1ae4d6e2ef500000", "nonce": 198, "to": "0x5f75112bbb4e1af516fbe3e21528c63da2b6a1a5", "value": 0, "v": "0x25", "r": "0x896b5c0224cfbe796810a4ec794f81b4009dee6176bbad86dc67778789568be2", "s": "0x7edd0fc8cc89fa5e81add0d36ff9de76f4c93b03f5b5cdf1c5818856574ef245"}}}}
{"jsonrpc": "2.0", "id": null, "method": "subscribe", "params": {"subscription": "909e4bae-2c48-43f3-a007-f17d4c8a3ce8", "result": {"tx_hash": "0bc3edac5ca5c6bf1b8ba57a088a01a4fddb87a977079d88a458c44cc5b70808", "tx_contents": {"from": "0xa7efae728d2936e78bda97dc267687568dd593f3", "gas": 210000, "gas_price": 56000000000, "hash": "0x0bc3edac5ca5c6bf1b8ba57a088a01a4fddb87a977079d88a458c44cc5b70808", "input": "0x0", "nonce": 107386, "to": "0xd4a41118e72ff8ae1a48e1b2b8883c508ad8f22b", "value": 800000000000000000, "v": "0x1b", "r": "0x4e7f4b46e4dca57d1d7ab98e0f504b6366e1204014940c37dfd55569a59f1fec", "s": "0x6a75f909a678f780ce55b2b628a54df39b64789c702f2cd34a96c199e001f622"}}}}
```

### Local Ethereum Validation

If your local node is configured with the websocket server or you would like to enable it, the
bloXroute Gateway can confirm its received transactions with your node's mempool.
Include the following arguments to launch Ethereum's [websocket server][eth_ws]
when starting your Ethereum node:

```
--ws --wsport 8546 --wsapi eth
```

Start the bloXroute gateway with the following arguments, in addition to the 
[ones earlier](#enabling-websocket-rpc).

```
--eth-ws-uri ws://127.0.0.1:8546
```

Replace the IP address with your Ethereum node's IP address if the two processes
are not running on the same machine. The effect of this validation is similar in
speed to Ethereum's `newPendingTransactions` stream, though the one from your 
gateway will include transaction contents by default.

## SDK

bloXroute Gateway includes a small Python SDK for connecting to this service.

Recommended usage (with context manager):

```python
from bxcommon.rpc.provider.ws_provider import WsProvider

ws_uri = "ws://127.0.0.1:28333"
async with WsProvider(ws_uri) as ws:
    subscription_id = await ws.subscribe("newTxs", {"include": ["tx_hash"], "duplicates": False})

    while True:
        next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
        print(next_notification)  # or process it generally
```

Without context manager:

```python
from bxcommon.rpc.provider.ws_provider import WsProvider

ws_uri = "ws://127.0.0.1:28333"
ws = await WsProvider(ws_uri)

try:
    subscription_id = await ws.subscribe("newTxs", {"include": ["tx_hash"]})

    while True:
        next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
        print(next_notification)  # or process it generally
except:
    await ws.close()
```

Callback interface:

```python
from bxcommon.rpc.provider.ws_provider import WsProvider
import asyncio

ws_uri = "ws://127.0.0.1:28333"
ws = WsProvider(ws_uri)
await ws.initialize()


def process(subscription_message):
    print(subscription_message)


ws.subscribe_with_callback(process, "newTxs", {"include": ["tx_hash"]})

while True:
    await asyncio.sleep(0)  # otherwise program would exit
```

Unsubscribing:

```python
from bxcommon.rpc.provider.ws_provider import WsProvider

ws_uri = "ws://127.0.0.1:28333"

async with WsProvider(ws_uri) as ws:
    subscription_id = await ws.subscribe("newTxs")
    await ws.unsubscribe(subscription_id)
```

If you want to make this stream handle disconnection events, run the provider in 
a loop –– any `ws` operation will throw an exception if the websocket gets closed.

```python
from bxcommon.rpc.provider.ws_provider import WsProvider

ws_uri = "ws://127.0.0.1:28333"
while True:
    try:
        async with WsProvider(ws_uri) as ws:
            subscription_id = await ws.subscribe("newTxs", {"include": ["tx_hash"], "duplicates": False})

            while True:
                next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
                print(next_notification)  # or process it generally
    except Exception as e:
        print(e)
        continue
```

## Cloud Streaming

If you are not running your own gateway, you can subscribe to transaction feeds provided over
bloXroute's cloud API. These feeds are functionally identical to the websocket server that would
run on your gateway. To connect, you'll need to connect to the websocket server at
`wss://eth.feed.blxrbdn.com` with the same SSL certificate you would use for your gateway.

The Python SDK includes a provider for this purpose that's used similar as the above section:

```python
from bxcommon.rpc.provider.cloud_wss_provider import CloudWssProvider

async with CloudWssProvider() as ws:
    subscription_id = await ws.subscribe("newTxs", {"include": ["tx_hash"]})
    while True:
        next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
        print(next_notification)  # or process it generally
```

`CloudWssProvider` will connect to `eth.feed.blxrbdn.com` and by default look in the directory that 
your gateway would store its SSL certificates. You should customize this behavior to point to the 
artifacts you download when from registering your account.

```python
from bxcommon.rpc.provider.cloud_wss_provider import CloudWssProvider

async with CloudWssProvider(
    # assuming you unzipped bx_artifacts.zip to /Users/bloxroute/bx_artifacts
    ssl_dir="/Users/bloxroute/bx_artifacts/registration_only",
    ws_uri="wss://eth.feed.blxrbdn.com:28333"
) as ws:
    subscription_id = await ws.subscribe("newTxs", {"include": ["tx_hash"]})
    while True:
        next_notification = await ws.get_next_subscription_notification_by_id(subscription_id)
        print(next_notification)  # or process it generally
```


[eth_ws]: https://geth.ethereum.org/docs/rpc/server
