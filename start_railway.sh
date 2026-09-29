#!/bin/bash
set -euo pipefail

export DISPLAY="${DISPLAY:-:100}"
DISPLAY_NUM="${DISPLAY#:}"
mkdir -p /app/data/chrome_profile /tmp/aviator

rm -f "/tmp/.X${DISPLAY_NUM}-lock" "/tmp/.X11-unix/X${DISPLAY_NUM}"

echo "[RUNTIME] starting Xvfb on ${DISPLAY}" >&2
Xvfb "${DISPLAY}" -screen 0 1440x900x24 -ac +extension RANDR > /tmp/aviator/xvfb.log 2>&1 &
XVFB_PID=$!

for i in $(seq 1 50); do
  if ! kill -0 "${XVFB_PID}" 2>/dev/null; then
    cat /tmp/aviator/xvfb.log >&2 || true
    exit 1
  fi
  if [ -S "/tmp/.X11-unix/X${DISPLAY_NUM}" ]; then
    break
  fi
  sleep 0.1
done

if ! kill -0 "${XVFB_PID}" 2>/dev/null || [ ! -S "/tmp/.X11-unix/X${DISPLAY_NUM}" ]; then
  cat /tmp/aviator/xvfb.log >&2 || true
  exit 1
fi

if ! pgrep -x chromium >/dev/null 2>&1; then
  rm -f /app/data/chrome_profile/SingletonLock         /app/data/chrome_profile/SingletonCookie         /app/data/chrome_profile/SingletonSocket
fi

echo "[RUNTIME] starting fluxbox" >&2
fluxbox > /tmp/aviator/fluxbox.log 2>&1 &
FLUXBOX_PID=$!

echo "[RUNTIME] starting x11vnc" >&2
x11vnc -display "${DISPLAY}" -forever -shared -localhost -nopw -rfbport 5900 > /tmp/aviator/x11vnc.log 2>&1 &
X11VNC_PID=$!

echo "[RUNTIME] starting browser API server" >&2
python -m browser_collector.server > /tmp/aviator/browser-server.log 2>&1 &
SERVER_PID=$!

echo "[RUNTIME] starting Telegram bot" >&2
python -m bot > /tmp/aviator/bot.log 2>&1 &
BOT_PID=$!

echo "[RUNTIME] starting Railway Chromium collector" >&2
(
  while true; do
    python -m tools.railway_collector > /tmp/aviator/collector.log 2>&1 || true
    echo "[RUNTIME] Railway collector exited; retrying" >&2
    sleep 5
  done
) &
COLLECTOR_PID=$!

tail -F /tmp/aviator/browser-server.log 2>/dev/null &
SERVER_LOG_PID=$!
tail -F /tmp/aviator/bot.log 2>/dev/null &
BOT_LOG_PID=$!
tail -F /tmp/aviator/collector.log 2>/dev/null &
COLLECTOR_LOG_PID=$!

cleanup() {
  kill "$COLLECTOR_LOG_PID" "$BOT_LOG_PID" "$SERVER_LOG_PID" 2>/dev/null || true
  kill "$COLLECTOR_PID" "$BOT_PID" "$SERVER_PID" "$X11VNC_PID" "$FLUXBOX_PID" "$XVFB_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

while kill -0 "$SERVER_PID" 2>/dev/null && kill -0 "$XVFB_PID" 2>/dev/null; do
  sleep 5
done

echo "[RUNTIME] critical server/Xvfb process exited" >&2
cat /tmp/aviator/browser-server.log >&2 || true
cat /tmp/aviator/xvfb.log >&2 || true
exit 1
