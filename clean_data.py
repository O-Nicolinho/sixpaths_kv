#!/usr/bin/env python3
"""
clean_data.py

Deletes all per-node data directories (including WAL files) so you can
start the cluster from a clean slate.

"""

import os
import shutil

# Directories used by your nodes. Adjust if your NodeConfig changes.
DATA_DIRS = [
    "./data",   # original single-node dir, if you used it
    "./data1",
    "./data2",
    "./data3",
    "./data4",
    "./data5",
    "./data6",
]

def main():
    for d in DATA_DIRS:
        if os.path.isdir(d):
            print(f"Removing {d} ...")
            shutil.rmtree(d)
        else:
            print(f"Skipping {d} (does not exist)")

    print("Done. All node data/WAL directories have been cleaned (if they existed).")

if __name__ == "__main__":
    main()
