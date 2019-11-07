# bloXroute Gateway

https://bloxroute.com

## What is bloXroute?

bloXroute is a blockchain scalability solution that allows all cryptocurrencies and blockchains to scale to 
thousands of transactions per second (TPS) on-chain, without any protocol changes.

bloXroute solves the scalability bottleneck by addressing the substantial time required for all nodes to synchronize 
when handling large volumes of TPS. Most importantly, bloXroute does this in a provably neutral way.

For more information, you can read our [white paper].

## Quick start

You can choose either to either install via [pip] or [Docker] (recommended). Refer to (TODO @Allan: LINK) 
our technical documentation for full usage instructions.

### pip

It's best to run this program inside of a [virtual environment], especially if your system's default Python 
version is not 3. You maybe need to substitute `pip3` for `pip` and/or add Python binaries to your 
`PATH` (usually `PATH=$PATH:~/.local`) for the following commands to work. `bloxroute-gateway` is not compatible with
Python 2.

`bloxroute-gateway` is optimized for Ubuntu, CentOS, and Alpine Linux, but should run fine on any Unix based system.
`bloxroute-gateway` is not compatible with Windows.

Install:
```
pip install requests==2.19.1 distro
pip install bloxroute-gateway
```

Running:
```
bloxroute_gateway --blockchain-protocol [blockchain-protocol] --blockchain-network [blockchain-network]
```

For more details, refer to `README.gateway.md`.


### Docker

Install:
```
docker pull bloxroute/bxgateway
```

Running:
```
docker run bloxroute/bxgateway --blockchain-protocol [blockchain-protocol] --blockchain-network [blockchain-network]
```

### Arguments

You are required to specify a blockchain protocol (e.g. `BitcoinCash`) and a blockchain network (e.g. `Mainnet`, 
`Testnet`). If you are using Ethereum, you will also need to specify the public key of your Ethereum node via 
`--node-public-key [hex-encoded-key]`.

For detail on the all possible command-line arguments, refer to (TODO @Allan: LINK).

## Development

Ensure you have Python 3.6+ installed. Again, we recommend that you use a `virtualenv` for `bxgateway` development.

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

## Documentation

You can find our technical documentation and architecture [on our website](TODO @Allan: LINK).

## Troubleshooting

Contact us at support@bloxroute.com for further questions.


[white paper]: https://bloxroute.com/wp-content/uploads/2019/01/whitepaper-V1.1-1.pdf
[pip]: https://pypi.org/project/pip/
[docker]: https://www.docker.com
[bxextensions]: https://github.com/bloXroute-Labs/bxextensions
[virtual environment]: https://virtualenv.pypa.io/en/latest/
