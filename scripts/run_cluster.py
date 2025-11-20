#!/usr/bin/env python3
import subprocess
import time
import signal
import sys

# Commands to run each node.
# We assume you're running this from the repo root, so ./cmd/kvs exists.
NODE_CMDS = [
    ["go", "run", "./cmd/kvs", "-id", "n1"],
    ["go", "run", "./cmd/kvs", "-id", "n2"],
    ["go", "run", "./cmd/kvs", "-id", "n3"],
    ["go", "run", "./cmd/kvs", "-id", "n4"],
    ["go", "run", "./cmd/kvs", "-id", "n5"],
    ["go", "run", "./cmd/kvs", "-id", "n6"],
]

ROUTER_CMD = ["go", "run", "./cmd/router", "-addr", ":8080", "-backend-host", "127.0.0.1"]


def main():
    procs = []
    try:
        # Start all KV nodes
        for i, cmd in enumerate(NODE_CMDS, start=1):
            print(f"Starting node {i}: {' '.join(cmd)}")
            p = subprocess.Popen(cmd)
            procs.append(("node", p))

        # Give nodes a tiny moment to start up
        time.sleep(1.0)

        # Start router
        print(f"Starting router: {' '.join(ROUTER_CMD)}")
        router_proc = subprocess.Popen(ROUTER_CMD)
        procs.append(("router", router_proc))

        print("\nCluster is starting up.")
        print("Nodes: n1..n6, Router: :8080")
        print("Press Ctrl-C to stop everything.\n")

        # Just sit and wait; children keep running.
        while True:
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nReceived Ctrl-C, shutting down cluster...")
    finally:
        # Try to terminate all started processes
        for kind, p in procs:
            if p.poll() is None:  # still running
                print(f"Terminating {kind} (pid={p.pid})")
                p.send_signal(signal.SIGINT)
        # Give them a moment to die cleanly
        time.sleep(1.0)
        # Force kill anything still alive
        for kind, p in procs:
            if p.poll() is None:
                print(f"Killing {kind} (pid={p.pid})")
                p.kill()

        print("All processes stopped. Bye 👋")
        sys.exit(0)


if __name__ == "__main__":
    main()
