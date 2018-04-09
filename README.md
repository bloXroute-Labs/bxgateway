# bloXroute client

The implementation of the bloXroute client node.

Note: we use docker for development, which takes care of installing all
dependencies and isolating a python environment for you on any machine. The
first time you run, it will take a while to build. Subsequently, it should be
nearly instantaneous.

## Running

    dev/run


The server reads its configuration from `config.cfg`. This file is currently a
sample. You can specify a section of the config to read using `-c`

    docker run -t -p 3001:3001 bloxroute-server -c [yourhostname]

Note that port 3001 in this case corresponds to the port that was opened in the
config.cfg for localhost.

### Overriding config.cfg with named params

If you don't want to use config.cfg at all you can override the peering string
which is a comma separated list of "[nodeip] [port] [index]"
And other parameters

    docker run -t bloxroute-server -p "google.com 80 12345" -P 1906 -i 12345

The full list of args that can be overridden to avoid reading config.cfg:

    -I network ip of node to be used for connecting to other nodes
    -p peering string
    -P port
    -i index of node
    -m index of manager
    -h help

Run the help to get more info

    docker run -t bloxroute-server -h
