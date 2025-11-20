# sixpaths-kv

Sixpaths KV is a small distributed sharded KV store written entirely in Go. 

To summarize:
- Each node is its own durable KV server with a write-ahead log (WAL) and in-memory store (to maintain data integrity).
- A separate, stateless router process sits in front and shards keys across 6 nodes using hashing. 
- Duplicate client writes are ignored by using ClientID and Seq. 
- The project offers basic metrics and helper scripts to run and reset the whole cluster.
- Functionally, the cluster works as a single KV store. Under the hood, data is distributed across 6 nodes, each with their own independent KV store.
- This approach leads to a more robust database since faults/malfunctions in one node will only affect keys that map to that node, all others will be unaffected.

---

## Features

- **Distributed sharding across 6 nodes**
  - Static cluster config (`cluster.go`) defines `n1`–`n6`, each with its own client port and data directory.
  - A router hashes the key to pick the node responsible for that key.

- **Per-node durability via WAL**
  - Each node keeps a write-ahead log (`wal.go`) of applied commands. 
  - On startup, the node replays the WAL and rebuilds its in-memory store (`node.go` + `store.go`).
  - This allows for nodes to rebuild their store after an unexpected crash.

- **Duplicate write prevention**
  - Each write carries a `(ClientID, Seq)` 
  - The store tracks the last `(seq, result)` per client and returns the previous result for duplicates instead of re-applying.

- **Backend HTTP API**
  - We include several node-level endpoints (`http_server.go`):  
    - `POST /put` – upsert value  
    - `POST /delete` – delete value  
    - `GET /get?key=...` – fetch value  
    - `GET /health` – basic health / last log index  
    - `GET /metrics` – per-node counters   
  - The frontend Router exposes the same `/put`, `/delete`, and `/get` API and then sends the requests to the correct node.

- **Metrics**
  - Per-node counters for total execs, puts, deletes, and dedup hits (`metrics.go`). 
  - The Router aggregates `/metrics` from all the nodes to give comprehensive info about the cluster

- **Helper scripts**
  - `run_cluster.py` – starts all 6 nodes and the router in one command
  - `clean_data.py` – wipes all node data/WAL directories for a fresh start :)

---

## Architecture

![Cluster architecture](docs/visual.png)

## Demo

![Cluster demo](docs/SixpathsKVS_Demo.gif)




