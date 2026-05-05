import asyncio, time
from dataclasses import dataclass
@dataclass
class MeshNode:
    node_id: str
    host: str
    port: int
    last_seen: float
class MeshCore:
    def __init__(self, node_id, port=7946):
        self.node_id = node_id
        self.port = port
        self.peers = {}
    async def start(self):
        server = await asyncio.start_server(self.handle, '0.0.0.0', self.port)
        async with server:
            await self.heartbeat()
    async def handle(self, reader, writer):
        data = await reader.read(4096)
        writer.write(b'PONG:' + self.node_id.encode())
        await writer.drain()
        writer.close()
    async def heartbeat(self):
        while True:
            await asyncio.sleep(30)
            print(f'[HEARTBEAT] {len(self.peers)} peers')
    def add_peer(self, node):
        self.peers[node.node_id] = node
if __name__ == '__main__':
    m = MeshCore('alpha-1')
    m.add_peer(MeshNode('beta-1', '192.168.1.10', 7946, time.time()))
    asyncio.run(m.start())
