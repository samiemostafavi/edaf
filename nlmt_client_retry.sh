#!/bin/bash

# Cleanup on Ctrl-C
cleanup() {
  echo "[Info] Caught Ctrl-C. Cleaning up..."
  [[ -n "$client_pid" ]] && kill -9 "$client_pid" 2>/dev/null
  [[ -n "$tail_pid" ]] && kill -9 "$tail_pid" 2>/dev/null
  wait "$client_pid" 2>/dev/null
  wait "$tail_pid" 2>/dev/null
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

  # Step 2: Start nlmt client in background and capture output
  log_file="/tmp/nlmt_$(date +%s).log"
  rm -f "$log_file"
  touch "$log_file"

  ./nlmt client --tripm=oneway -i 50ms -g edaf1/test -l 100 -m 1 -d 20s -o d --outdir=/tmp/ 192.168.70.129 \
    >> "$log_file" 2>&1 &
  client_pid=$!

  tail -f "$log_file" &
  tail_pid=$!

  # Wait up to 3 seconds for "[Connected]"
  for i in {1..3}; do
    sleep 1
    if grep -q "\[Connected\]" "$log_file"; then
      echo "[Info] nlmt connected. Letting it run for 10 seconds..."
      break
    fi
  done

  if ! grep -q "\[Connected\]" "$log_file"; then
    echo "[Warn] nlmt did not connect in time. Force killing and retrying..."
    kill -9 "$client_pid" "$tail_pid" 2>/dev/null
    wait "$client_pid" "$tail_pid" 2>/dev/null
    sleep 1
    continue
  fi

  # Let it run for exactly 10 seconds
  sleep 10
  echo "[Info] 10s finished. Force killing nlmt client..."
  kill -9 "$client_pid" "$tail_pid" 2>/dev/null
  wait "$client_pid" "$tail_pid" 2>/dev/null

  sleep 1
done
