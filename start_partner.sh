#!/bin/bash

if [ -z "$GOPATH" ]; then
    echo "FAIL: GOPATH environment variable is not set"
    exit 1
fi

if [ -n "$(go version | grep 'darwin/amd64')" ]; then
    GOOS="darwin_amd64"
elif [ -n "$(go version | grep 'linux/amd64')" ]; then
    GOOS="linux_amd64"
else
    echo "FAIL: only 64-bit Mac OS X and Linux operating systems are supported"
    exit 1
fi

# Build the participant binary.
# Exit immediately if there was a compile-time error.
go install github.com/potay/partner/partner/participant
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Build the manager binary.
# Exit immediately if there was a compile-time error.
go install github.com/potay/partner/partner/manager
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

MANAGER="$GOPATH"/bin/manager
PARTICIPANT="$GOPATH"/bin/participant
N=10
HOSTPORT="localhost:8080"

trap 'kill $(jobs -p) && killall participant' EXIT

# Start manager.
"${MANAGER}" -N=${N} -hostport=${HOSTPORT} -binary="${PARTICIPANT}" &

echo "Press [CTRL+C] to stop.."
while true
do
  sleep 1
done
