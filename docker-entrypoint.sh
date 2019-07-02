#!/bin/sh
set -e

# this script will start main.py

PROGNAME=$(basename $0)

USER="bxgateway"
GROUP="bxgateway"
PYTHON="/usr/local/bin/python"
WORKDIR="src/bxgateway"
STARTUP="$PYTHON main.py $@"

echo "$PROGNAME: Starting $STARTUP"
if [[ "$(id -u)" = '0' ]]; then
    # if running as root, chown and step-down from root
    find . \! -type l  -user ${USER} -exec chown ${USER}:${GROUP} '{}' +
    find ../bxcommon \! -user ${USER} -exec chown ${USER}:${GROUP} '{}' +
    cd ${WORKDIR}
    if [[ "${BLXR_COLLECT_CORE_DUMP}" == "1" || "${BLXR_COLLECT_CORE_DUMP}" == "true" ]]; then
        echo enabling collecting core dumps...
        ulimit -c unlimited
        mkdir -p /var/crash
        echo /var/crash/core.%e.%p.%h.%t > /proc/sys/kernel/core_pattern
    fi
    exec su-exec ${USER} ${STARTUP}
else
    # allow the container to be started with `--user`, in this case we cannot use su-exec
    cd ${WORKDIR}
    exec ${STARTUP}
fi
