#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Options for debugging
#

## Options for Falcon network for debugging purposes only.

# True if we want to avoid doing the database puts
FAKE_DB = False

# True if we want to take heap profiles
PROFILING = False


def crash():
    exit(1)


# negative if we are never going to crash
# Otherwise, it's the number of seconds until this Falcon node
# will crash.
CRASH_INTERVAL = -1

LOG_FOR_WEB = True

ENABLE_LOGGING = True

# If the peer is more this many blocks behind me, then we close the connection.
# This is useful to change for testing so that we can test tranfer rates for large numbers of blocks.
HEIGHT_DIFFERENCE = 100

FLUSH_LOG = False
