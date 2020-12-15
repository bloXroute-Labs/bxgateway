# Compare Transaction Feeds

compare_tx_feeds.py is a script that compares the performance of the Ethereum transaction feeds 
offered by the bloXroute Gateway or Cloud API against an Ethereum node's newPendingTransactions feed.

Example statistics output:
````
Interval length: 600 seconds. Interval end time: 2020-07-30 20:48:16
Analysis of Transactions received on both feeds:
Number of transactions: 6935
Number of transactions received from Gateway first: 6616
Number of transactions received from Ethereum node first: 319
Percentage of transactions seen first from gateway: 95%
Average time difference for transactions received first from gateway (ms): 927.3
Average time difference for transactions received first from Ethereum node (ms): 70.5

Total Transactions summary:
Total tx from gateway: 7462
Total tx from eth node: 2493
Number of low fee tx ignored: 320
````

# Installing and setting up

Install bloxroute-cli with the following command:
````
pip install bloxroute-cli
````

To enable websocket RPC on your Gateway, use the following parameters:
```
--ws True --ws-host <IP address of client application> --ws-port 28333
```

If you are testing with the pendingTxs feed, add the following parameter to have your Gateway connect to your Ethereum 
node's websocket server:
```
--eth-ws-uri ws://<eth node IP address>:8546
```

To enable websocket RPC on your Ethereum node, use the following parameters:
```
--ws --wsaddr <gateway IP address> --wsapi eth --wsport 8546 
```

Ensure you are running with Python 3.7 or later. 

# Arguments

Example command:

```
python compare_tx_feeds.py --gateway ws://127.0.0.1:28333 --eth ws://127.0.0.1:8546 --feed-name newTxs --min-gas-price 60 --interval 600 --num-intervals 1 --dump ALL
```

Use `--gateway` to specify URL of the Gateway running with websocket RPC enabled. 
Default: ws://127.0.0.1:28333

Use `--eth` to specify URL of an Ethereum node running with websocket RPC enabled. 
Default: ws://127.0.0.1:8546

Use `--feed-name` to specify the feed offered by bloXroute Gateway that you would like to compare against Ethereum's 
newPendingTransactions feed. Available options are `newTxs` and `pendingTxs`
Default: newTxs

Use `--min-gas-price` to set a minimum gas price threshold. All transactions with a gas price lower than the 
minimum gas price threshold will be ignored.
Default: None

Use `--addresses` to specify a comma separated list of Ethereum addresses. All transactions that are not sent to or 
from the specified addresses will be ignored.
Default: None

Use the `--exclude-tx-contents` flag to indicate if you would like to compare the performance of the feeds when fetching 
only the transaction hash.

Use `--interval` to set the length of the performance sample in seconds. Trail time specified by `--trail-time` is also 
added to the statistics interval.
Default: 600

Use `--num-intervals` to specify the number of intervals you would like to run.
Default: 6

Use `--lead-time` to specify the length of time in seconds to wait after feed subscriptions before beginning to compare
the feeds. 
Default: 60

Use `--trail-time` to specify the length of time in seconds to wait after the interval for seen transactions on a single 
stream to be provided on the other stream.
Default: 60

Use `--dump` to indicate if you would like to dump extra information to a file. Use option `ALL` to dump all seen 
transactions with time received to a csv file called all_hashes.csv. Use option `MISSING` to dump all the transactions 
received from the Ethereum node that are missing on the bloXroute stream to a file called missing_hashes.txt. Use option `ALL,MISSING` to do both.
Default: ""

Use the `--exclude-duplicates` flag if you are using the bloXroute pendingTxs feed and do not want to include duplicate
transactions on the feed. (Note: The Ethereum feed includes duplicates.)

Use `--ignore-delta` to filter out transactions above a threshold of difference in time received.
Default: 5 (seconds)

Use the `--use-cloud-api` flag to indicate if you would like to use the bloXroute Cloud API websocket endpoint instead of 
specifying a bloXroute Gateway endpoint. If set to True, you must also use the --ssl-dir argument, and you do not
need to use the --gateway argument.

Use `--auth-header` to specify an authorization header created with rpc username and rpc password if using the 
`--use-cloud-api` flag. See `https://docs.bloxroute.com/apis/constructing-api-headers#request` for instructions on
how to generate an authorization header using your rpc username and rpc password.

Use the `--verbose` flag to print extra statistics.

Use the `--exclude-from-blockchain` flag to have the bloXroute feed exclude transactions received from the gateway's 
blockchain node, instead only reporting those received from the BDN.
