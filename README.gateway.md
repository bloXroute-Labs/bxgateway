bloxroute-gateway connects you to the BDN, a blockchain distribution network, to greatly speed up block propagation
times and scale your blockchain today.

# Prereqs

This project depends on Python 3 and its development extensions. These instructions assuming you are running an
operating system with Python 3 installed.

Ubuntu:
```bash
sudo apt-get update
sudo apt install python3-dev
```

CentOS:
```bash
yum install gcc libffi-devel
```

Alpine Linux:
```bash
apk add build-base automake libtool libffi-dev python3-dev linux-headers
```

# Installation

It's best to run this program inside of a [virtual environment][1], especially if your system's default Python 
version is not 3. You maybe need to substitute `pip3` for `pip` and/or add Python binaries to your 
`PATH` (usually `PATH=$PATH:~/.local`) for the following commands to work.
```bash
pip install virtualenv
virtualenv venv -p python3
source venv/bin/activate
```

bloxroute-gateway has C++ extensions that are compiled per operating system, so this PIP package is distributed 
only as an [Source Distribution][2]. During installation, we require `distro` to determine the operating system version
and `requests` to fetch the right set of C++ extensions from an S3 bucket.

```bash
pip install requests==2.22.0 distro
pip install bloxroute-gateway
```

# Running

```bash
$ bloxroute_gateway --blockchain-protocol [blockchain-protocol] --blockchain-network [blockchain-network]
```

If you are running an Ethereum gateway, you will also need to specify the public key of your Ethereum node:
```bash
$ bloxroute_gateway [...] --node-public-key [hex-encoded-key]
```

## Supported Protocols and Networks for bloXroute Mainnet
* BitcoinCash: Mainnet
* Ethereum: Mainnet

You can also specify a `BLXR_ENV` environment variable to specify which BDN you want to connect to.
 * `BLXR_ENV=test`: Testnet
 
# Documentation
You can find our full technical documentation and architecture [on our website][documentation].

# Troubleshooting

Contact us at support@bloxroute.com for further questions.

[1]: https://virtualenv.pypa.io/en/latest/
[2]: https://docs.python.org/3.7/distutils/sourcedist.html#manifest-template
[documentation]: https://bloxroute.com/documentation/
