# sovereign-agent-mesh

Decentralized P2P mesh networking for SOVEREIGN agent swarms.

## Features

- TCP message propagation with TTL-based flood control
- UDP multicast peer discovery
- Topic-based message routing
- Automatic deduplication

## Quick Start

```python
import asyncio
from sovereign_agent_mesh.mesh import MeshNode
from sovereign_agent_mesh.discovery import DiscoveryService

node = MeshNode("node-alpha", listen_port=7799)
disc = DiscoveryService("node-alpha", mesh_port=7799)

node.on("heartbeat", lambda m: print(f"Heartbeat from {m.sender}"))

async def main():
    disc.start()
    await node.start()

asyncio.run(main())
```
