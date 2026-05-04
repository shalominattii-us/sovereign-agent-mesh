"""
Simple UDP multicast discovery for local mesh peers.
"""
import socket
import struct
import json
import threading
import time

MULTICAST_GROUP = "239.255.77.99"
DISCOVERY_PORT = 7798

class DiscoveryService:
    def __init__(self, node_id: str, mesh_port: int = 7799):
        self.node_id = node_id
        self.mesh_port = mesh_port
        self.running = False
        self.peers = {}

    def announce(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
        while self.running:
            msg = json.dumps({"node_id": self.node_id, "mesh_port": self.mesh_port, "ts": time.time()})
            sock.sendto(msg.encode(), (MULTICAST_GROUP, DISCOVERY_PORT))
            time.sleep(5)

    def listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        while self.running:
            try:
                data, addr = sock.recvfrom(1024)
                info = json.loads(data.decode())
                self.peers[info["node_id"]] = (addr[0], info["mesh_port"])
            except Exception:
                pass

    def start(self):
        self.running = True
        threading.Thread(target=self.announce, daemon=True).start()
        threading.Thread(target=self.listen, daemon=True).start()

    def stop(self):
        self.running = False
