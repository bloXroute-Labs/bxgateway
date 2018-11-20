# bloXroute client gateway

The implementation of the bloXroute client gateway.

Note: we use docker for development, which takes care of installing all
dependencies and isolating a python environment for you on any machine. The
first time you run, it will take a while to build. Subsequently, it should be
nearly instantaneous.

## Dev: running locally

    dev/run -h

This is just a dockerized version of main.py, so equivalent:

    cd src/bxgateway
    python main.py -h

The dockerized version takes care of installing dependencies from
requirements.txt. If you really hate docker, you can install them yourself.
In this case, you have to set your machine up for python dev and then run:

    pip install -r requirements.txt

Example run

    dev/run --sdn-url=http://127.0.0.1:8080 --external-ip=127.0.0.1 --external-port=9000 --source-version=1.3.0 --blockchain-net-magic "12345" --blockchain-services 0 --bloxroute-version bloxroutetest --blockchain-version 70014

Use the help to see the ful llist

    dev/run -h
