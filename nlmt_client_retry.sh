#!/bin/bash

# Cleanup on Ctrl-C
cleanup() {
  echo "[Info] Caught Ctrl-C. Cleaning up..."
  [[ -n "$client_pid" ]] && kill "$client_pid" 2>/dev/null
  wait "$client_pid" 2>/dev/null
  exit 0
}

trap cleanup SIGINT

while true; do
  # Step 1: Ensure route exists
  if ip route | grep -q "192.168.70.128/26"; then
    echo "[Info] Route already exists"
  else
    echo "[Info] Attempting to add route..."
    ip route add 192.168.70.128/26 via 10.0.0.1 || {
      echo "[Warn] Failed to add route. Retrying in 1s..."
      sleep 1
      continue
    }
  fi

  echo "[Info] Starting nlmt client..."

  # Step 2: Start nlmt client in background, capture output
  log_file="/tmp/nlmt_$(date +%s).log"
  ./nlmt client --tripm=oneway -i 50ms  -g edaf1/test -l 100 -m 1 -d 10s -o d --outdir=/tmp/ 192.168.70.129 2>&1 | tee "$log_file" &
  client_pid=$!

  # Wait up to 3 seconds for "[Connected]" in the output
  for i in {1..3}; do
    sleep 1
    if grep -q "\[Connected\]" "$log_file"; then
      echo "[Info] nlmt connected. Letting it run."
      wait $client_pid
      break
    fi
  done

  # If not connected, kill the client and retry
  if ! grep -q "\[Connected\]" "$log_file"; then
    echo "[Warn] nlmt did not connect in time. Killing and retrying..."
    kill $client_pid 2>/dev/null
    wait $client_pid 2>/dev/null
  fi

  sleep 1
done
