# bloXroute client

The implementation of the bloXroute client node.

Note: we use docker for development, which takes care of installing all
dependencies and isolating a python environment for you on any machine. The
first time you run, it will take a while to build. Subsequently, it should be
nearly instantaneous.

## Dev: running locally

    dev/run -h

This is just a dockerized version of main.py, so equivalent:

    cd src/client
    python main.py -h

The dockerized version takes care of installing dependencies from
requirements.txt. If you really hate docker, you can install them yourself.
In this case, you have to set your machine up for python dev and then run:

    pip install -r src/client/requirements.txt

## How the client works

The client reads its configuration from `config.cfg`. This file is currently a
sample. You can specify a section of the config to read using `-c`

    dev/run -c [yourhostname]

### Overriding config.cfg with named params

If you don't want to use config.cfg at all you can override the peering string
which is a comma separated list of "[nodeip] [port] [index]"
And other parameters

    dev/run -p "google.com 80 1234"

Use the help to see the ful llist

    dev/run -h
