# Two‑Node Cassandra Test Cluster (Podman)

This README walks you through spinning up a **minimal two‑node Apache Cassandra 5.0.4 cluster** with **Podman** (or Docker), creating an **isolated Python virtual‑environment**, and running two flavours of **replication‑sanity tests**:

* **SQL‑style** – fixed‑schema table (`host-replication-test.py`).
* **NoSQL‑style** – document stored in a `MAP<text,text>` column (`host-replication-nosql.py`).

Both scripts write through **node A** (`localhost:9042`), read through **node B** (`localhost:9043`), and exit `0` on success.


## Directory layout

```text
project‑root/
├─ compose.yaml                     # two‑node cluster definition
├─ host-replication-test.py         # verbose SQL‑style test
├─ host-replication-nosql.py        # verbose NoSQL (MAP column) test
├─ requirements.txt                 # Python deps (just cassandra‑driver)
└─ README.md                        # ← this file
```


## 0 · Prerequisites

| Tool           | Version tested | Install hint                                                                                       |
| -------------- | -------------- | -------------------------------------------------------------------------------------------------- |
| **Podman**     | ≥ 4.x          | [https://podman.io](https://podman.io) (`sudo dnf install podman` / `brew install podman`)         |
| **Python**     | ≥ 3.8          | Bundled on most Linux/macOS; Windows: [https://python.org/downloads](https://python.org/downloads) |
| **pip / venv** | same as Python | Comes with Python 3                                                                                |

> **Rootless vs rootful Podman** – the compose file works in **either** mode. If you stay rootless, Podman ignores any static IP directives; that’s fine for this demo.


## 1 · Spin up the cluster

### 1.1 Clone / copy the repo

```bash
git clone <your‑repo> cassandra‑demo
cd cassandra‑demo
```

### 1.2 Start the containers

```bash
podman compose up -d            # or: docker compose up -d
```

* `cassandra-seed` listens on **localhost:9042**
* `cassandra-node` listens on **localhost:9043**

```bash
podman ps --format '{{.Names}} {{.Ports}}'
```

Expected:

```
cassandra-seed  0.0.0.0:9042->9042/tcp
cassandra-node  0.0.0.0:9043->9042/tcp
```

Wait until the seed JVM prints:

```bash
podman logs -f cassandra-seed | grep -m1 'listening for CQL clients'
```


## 2 · Create and activate a virtual‑env

```bash
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate.bat
pip install --upgrade pip setuptools wheel   # optional
pip install -r requirements.txt               # installs cassandra‑driver
```

You should see `(.venv)` in the prompt.


## 3 · Run the replication tests

### 3.1 SQL‑style table replication

```bash
python host-replication-test.py
```

Expected tail:

```
[10] Replication SUCCESS ✅ — peer returned expected value
[11] Shutting down driver sessions …
[12] Done.
```

### 3.2 NoSQL document replication

```bash
python host-replication-nosql.py
```

Sample output:

```
[10] Replication SUCCESS ✅ — document matched
[11] Shutting down driver sessions …
[12] Done.
```

Both scripts exit with status `0` on success, `1` on failure—handy for CI.

#### Custom ports / hosts

If you mapped different ports:

```bash
CASSANDRA_SEED_PORT=19042 CASSANDRA_PEER_PORT=19043 \
python host-replication-test.py
```

(similar for the NoSQL script.)


## 4 · Cleanup / teardown

Remove **containers, volumes, and network** in one go:

```bash
podman compose down -v          # stop + delete volumes
podman network rm cassandra_net # remove the bridge (ignored if rootless)
```

Want to keep data? Use **stop** instead:

```bash
podman compose stop             # containers halted, volumes preserved
```

Happy experimenting! 🎉
