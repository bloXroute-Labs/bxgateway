[![](images/logo.png)](https://bloxroute.com)
<p align="center">
  <a href="https://github.com/bloXroute-Labs/bxgateway/blob/develop/LICENSE.md">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="bxgateway is released under the MIT license." />
  </a>
  <a href="https://github.com/bloXroute-Labs/bxgateway/blob/develop/CONTRIBUTING.md">
    <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs welcome!" />
  </a>
  <a href="https://badge.fury.io/py/bloxroute-gateway">
    <img src="https://badge.fury.io/py/bloxroute-gateway.svg" alt="Pypi version" />
  </a>
    <a href="https://python.org">
    <img alt="Python" src="https://img.shields.io/badge/Python-3.6%20%7C%203.7%20%7C%203.8-blue">  
  </a>
  <a href="https://discord.gg/FAYTEMm">
    <img alt="Discord" src="https://img.shields.io/discord/638409433860407300?logo=discord">  
  </a>
  <a href="https://twitter.com/intent/follow?screen_name=bloxroutelabs">
    <img alt="Twitter Follow" src="https://img.shields.io/twitter/follow/bloxroutelabs?style=social">  
  </a>
</p>

# bloXroute Gateway
The bloXroute Gateway is a blockchain client that attaches to blockchain nodes and acts as an entrypoint to bloXroute's BDN.
## What is bloXroute?

bloXroute is a blockchain scalability solution that allows all cryptocurrencies and blockchains to scale to 
thousands of transactions per second (TPS) on-chain, without any protocol changes.

bloXroute solves the scalability bottleneck by addressing the substantial time required for all nodes to synchronize 
when handling large volumes of TPS. Most importantly, bloXroute does this in a provably neutral way.

For more information, you can read our [white paper].

## Quick start

You can choose either to either install via [pip] or [Docker] (recommended). Refer to  
[our technical documentation][install] for full usage instructions.

### Arguments

You are required to specify a blockchain protocol (e.g. `BitcoinCash`) and a blockchain network (e.g. `Mainnet`, 
`Testnet`). If you are using Ethereum, you will also need to specify the public key of your Ethereum node via 
`--node-public-key [hex-encoded-key]`.

For detail on the all possible command-line arguments, refer to [our technical documentation][args].

## Development

Ensure you have Python 3.8+ installed. Again, we recommend that you use a `virtualenv` for `bxgateway` development.

Clone the `bxcommon` repo, which contains abstract interfaces for the gateway node's event loop, connection management, 
and message classes that are shared with other bloXroute nodes.

```bash
git clone https://github.com/bloXroute-Labs/bxcommon.git
```

Make sure `bxcommon/src` is in your `PYTHONPATH` and export some environment variables (you can also add this to your
`~/.bash_profile`, or however you prefer to manage your environment).

```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/bxcommon/src 
export DYLD_LIBRARY_PATH="/usr/local/opt/openssl@1.1/lib"  # for OpenSSL dependencies
```

Install dependencies:

```bash
pip install -r bxcommon/requirements.txt
pip install -r bxcommon/requirements-dev.txt
pip install -r bxgateway/requirements.txt
pip install -r bxgateway/requirements-dev.txt
```

Run unit and integration tests:

```bash
cd bxgateway/test
python -m unittest discover
```

Run `bxgateway` from source:

```bash
cd bxgateway/src/bxgateway
python main.py --blockchain-network [blockchain-network] --blockchain-protocol [blockchain-protocol]
```

### Extensions
`bxgateway` has references to C++ extensions for faster performance on CPU intensive operations. To make use of this, 
clone the `bxextensions` repository, build the source files, and add the entire `bxextensions` folder to your 
`PYTHONPATH`.

```bash
git clone --recursive https://github.com/bloXroute-Labs/bxextensions.git
```

Refer to [bxextensions] for information on building the C++ extensions.

### Branches
By default you will clone the `develop` branch. This branch refers to the most recent code that is usually running
on the bloXroute Testnet. If you are looking for the production code that is definitely supported in our Mainnet,
use the `master` branch.

## Documentation

You can find our full technical documentation and architecture [on our website][documentation].

## Troubleshooting

Contact us at support@bloxroute.com for further questions.


[white paper]: https://bloxroute.com/wp-content/uploads/2019/01/whitepaper-V1.1-1.pdf
[bxextensions]: https://github.com/bloXroute-Labs/bxextensions
[virtual environment]: https://virtualenv.pypa.io/en/latest/
[install]: https://bloxroute.com/documentation/deployment/
[args]: https://bloxroute.com/documentation/deployment/#command-line-arguments
[documentation]: https://bloxroute.com/documentation/
