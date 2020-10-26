# Compare On Block Feeds

compare_eth_on_block_feed.py is a script that compares the performance of the bloXroute ethOnBlock feed 
offered by the bloXroute Gateway or Cloud API against custom implementation with an Ethereum node's feeds.

Example statistics output:

````
Started feed comparison. Duration: 600s
Start: 2020-10-11 18:02:03.410172, End: 2020-10-11 18:12:05.430070
Blocks seen in duration: 47
Number of blocks with results: 46
Number of results from Blxr first: 42
Number of results from Eth first: 4
Percentage of results seen first from gateway: 91.30%
Average time difference for results received first from Blxr (ms): 2067.04
Average time difference for results received first from Eth (ms): 1285.99
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

Add the following parameter to have your Gateway connect to your Ethereum 
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
python compare_eth_on_block_feed.py --gateway ws://127.0.0.1:28333 --eth ws://127.0.0.1:8546 --duration 600
```

Use `--gateway` to specify URL of the Gateway running with websocket RPC enabled. 

Default: ws://127.0.0.1:28333

Use `--eth` to specify URL of an Ethereum node running with websocket RPC enabled. 

Default: ws://127.0.0.1:8546

Use `--duration` to set the total duration of the test in seconds.

Default: 600

Use `--gateway-type` to indicate if you would like to use the bloXroute Cloud API websocket endpoint or 
a local bloXroute Gateway endpoint. If set to Cloud, you must also use the --ssl-dir argument.

Options: Cloud, Local

Default: Cloud

Use `--ssl-dir` to specify the path to your bloXroute certificates if `--gateway-type` is Cloud. 

Example: --ssl-dir /home/user/ssl/external_gateway/registration_only

Use `--call-params-file` to specify the EthCall commands source file to be executed by the subscription.
It is recommended to run with the `--debug` flag, in order to verify that the commands are valid.

Default: on_block_feed_call_params_example.json

Use the `--verbose` flag to print extra information.

Use the `--debug` flag to log debug information.
