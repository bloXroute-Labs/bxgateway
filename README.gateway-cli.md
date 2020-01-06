# App description

bloxroute-gateway-cli is a utility for communicating with the [bloxroute-gateway].

# Installation

It's best to run this program inside of a [virtual environment][1], especially if your system's default Python 
version is not 3. You maybe need to substitute `pip3` for `pip` and/or add Python binaries to your 
`PATH` (usually `PATH=$PATH:~/.local`) for the following commands to work.
```bash
pip install virtualenv
virtualenv venv -p python3
source venv/bin/activate
pip install bloxroute-gateway-cli
```

# Running

```bash
$ bloxroute-gateway-cli -h
```
 
# Documentation
You can find our full technical documentation and architecture [on our website][documentation].

# Troubleshooting

Contact us at support@bloxroute.com for further questions.

[1]: https://virtualenv.pypa.io/en/latest/
[documentation]: https://bloxroute.com/documentation/
[bloxroute-gateway]: https://pypi.org/project/bloxroute-gateway/
