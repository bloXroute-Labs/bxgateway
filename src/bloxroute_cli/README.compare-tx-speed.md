# Compare Transaction Speed

`compare_tx_speed.py` is a script that compares the performance of sending raw transactions with bloXroute Cloud-API and 
other service providers -- Alchemy and Infura. 

Example statistics output:
```
Initial check completed. Sleeping 30 sec.
Sending tx group 1.
2020-10-20 23:27:24.255892 - Sending transaction to https://mainnet.infura.io/v3/XXXXXXXXXX. TX Hash: XXXXXXXXXX
2020-10-20 23:27:24.257892 - Sending transaction to https://eth-mainnet.alchemyapi.io/v2/XXXXXXXXXX. TX Hash: XXXXXXXXXX
2020-10-20 23:27:24.258892 - Sending transaction to bloXroute Cloud-API. TX Hash: XXXXXXXXXX
Sleeping 30 sec.
Sending tx group 2.
2020-10-20 23:27:54.417183 - Sending transaction to https://mainnet.infura.io/v3/XXXXXXXXXX. TX Hash: XXXXXXXXXX
2020-10-20 23:27:54.418183 - Sending transaction to https://eth-mainnet.alchemyapi.io/v2/XXXXXXXXXX. TX Hash: XXXXXXXXXX
2020-10-20 23:27:54.419183 - Sending transaction to bloXroute Cloud-API. TX Hash: XXXXXXXXXX
Sleeping 1 min before checking transaction status.
---------------------------------------------------------------------------------------------------------
Summary:
Sent 2 groups of transactions to bloXroute Cloud-API and other providers, 2 of them have been confirmed: 
Number of Alchemy transactions mined: 0
Number of Infura transactions mined: 0
Number of bloXrotue transactions mined: 2
```

# Installation
Install bloxroute-cli with the following command:
````
pip install bloxroute-cli
````

Example startup command:
```
python compare_tx_speed.py --alchemy-api-key XXXXXXXX --infura-api-key XXXXXXXXX  --blxr-auth-header XXXXXXXXXX \
--sender-private-key 0xabcdXXXX --receiver-address 0xabcdXXXX --gas-price 33 --num-tx-groups 2
```

# Arguments
`--alchemy-api-key`: Alchemy API key. Please visit https://alchemyapi.io/ for more information.

`--infura-api-key`: Infura API key / project ID. Please visit https://infura.io/ for more information.

`--blxr-auth-header`: bloXroute authorization header. Use base64 encoded value of account_id:secret_hash for Cloud-API. 
Please visit https://bloxroute.com/docs/bloxroute-documentation/cloud-api/overview/ for more information.

`--sender-private-key`: Private key of the Ethereum sender, which is required in order to sign transactions.

`--receiver-address`: Address of the transaction receiver.

`--gas-price`: Gas price (in Gwei) of the transactions.

`--num-tx-groups`: Number of groups of transactions to be sent. Default: 1.

`--delay`: Time (in sec) to sleep between two consecutive groups. Default: 30.