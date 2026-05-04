"""
Mesh Node — P2P agent discovery and message routing.
"""
import asyncio
import json
import hashlib
from typing import Set, Dict, Callable
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class MeshMessage:
    msg_id: str
    sender: str
    recipient: str
    topic: str
    payload: dict
    ttl: int = 5
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
        if not self.msg_id:
            self.msg_id = hashlib.sha256(
                f"{self.sender}:{self.timestamp}:{json.dumps(self.payload, sort_keys=True)}".encode()
            ).hexdigest()[:16]

class MeshNode:
    def __init__(self, node_id: str, listen_host: str = "0.0.0.0", listen_port: int = 7799):
        self.node_id = node_id
        self.host = listen_host
        self.port = listen_port
        self.peers: Set[str] = set()
        self.handlers: Dict[str, Callable] = {}
        self.seen_messages: Set[str] = set()
        self.server = None

    def on(self, topic: str, handler: Callable):
        self.handlers[topic] = handler

    def add_peer(self, host: str, port: int):
        self.peers.add(f"{host}:{port}")

    async def broadcast(self, topic: str, payload: dict):
        msg = MeshMessage(sender=self.node_id, recipient="*", topic=topic, payload=payload)
        await self._propagate(msg)

    async def send(self, recipient_id: str, topic: str, payload: dict):
        msg = MeshMessage(sender=self.node_id, recipient=recipient_id, topic=topic, payload=payload)
        await self._propagate(msg)

    async def _propagate(self, msg: MeshMessage):
        if msg.msg_id in self.seen_messages:
            return
        self.seen_messages.add(msg.msg_id)
        if len(self.seen_messages) > 10000:
            self.seen_messages.clear()

        data = json.dumps(asdict(msg)).encode()
        for peer in list(self.peers):
            try:
                host, port = peer.rsplit(":", 1)
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, int(port)), timeout=2.0
                )
                writer.write(len(data).to_bytes(4, "big") + data)
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        if msg.recipient == "*" or msg.recipient == self.node_id:
            handler = self.handlers.get(msg.topic)
            if handler:
                asyncio.create_task(handler(msg))

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            raw_len = await asyncio.wait_for(reader.read(4), timeout=5.0)
            if len(raw_len) < 4:
                return
            length = int.from_bytes(raw_len, "big")
            data = await asyncio.wait_for(reader.read(length), timeout=10.0)
            msg_dict = json.loads(data.decode())
            msg = MeshMessage(**msg_dict)
            if msg.ttl > 0:
                msg.ttl -= 1
                await self._propagate(msg)
        except Exception:
            pass
        finally:
            writer.close()

    async def start(self):
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)
        async with self.server:
            await self.server.serve_forever()

    def stop(self):
        if self.server:
            self.server.close()
