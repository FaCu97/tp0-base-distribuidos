#!/usr/bin/env bash

set -euo pipefail

NETWORK="tp0_testing_net"
HOST="server"
PORT="12345"
MSG="ping-echo-test"

OUT="$(docker run --rm --network "$NETWORK" alpine sh -c "echo '$MSG' | nc $HOST $PORT" || true)"

if [[ "$OUT" == "$MSG" ]]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
  exit 1
fi