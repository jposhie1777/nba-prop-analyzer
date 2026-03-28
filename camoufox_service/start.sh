#!/bin/sh
# Start a virtual display then launch the FastAPI service.
# Firefox's GTK libraries require a display socket even with headless=True;
# Xvfb provides a virtual one so no physical screen is needed.
set -e

Xvfb :99 -screen 0 1920x1080x24 -ac +extension GLX +render -noreset &
XVFB_PID=$!

# Give Xvfb a moment to initialize before Firefox tries to connect
sleep 1

echo "Xvfb started (pid=$XVFB_PID, DISPLAY=$DISPLAY)"
exec python -m uvicorn main:app --host 0.0.0.0 --port 8080
