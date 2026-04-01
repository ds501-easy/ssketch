import socket
import threading
import hashlib
import random
from CMSCU import CMSCU

from Cuckoo import TopkCuckooHash

class HLLSwitchServer:
    def __init__(self, switch_id, total_switches, m, port, cmscu_width, cmscu_depth, bs_width, bs_entries, port_map, ssthreshold):
        self.switch_id = switch_id
        self.total = total_switches
        self.m = m
        self.c = m
        self.segment_size = m // total_switches
        self.start = switch_id * self.segment_size
        self.end = self.start + self.segment_size
        self.ssthreshold = ssthreshold
        self.A = [0] * m
        self.p = 1.0
        self.max_val = 7
        self.cmscu = CMSCU(cmscu_width, cmscu_depth)
        # self.bubblesketch = BubbleSketch(width=bs_width, k=bs_entries, cmscu=self.cmscu)
        self.topkcuckoo = TopkCuckooHash(width=bs_width, k=bs_entries, cmscu=self.cmscu, threshold=ssthreshold)

        self.total_received = 0
        self.sampled_count = 0
        self.forwarded_count = 0
        self.forwarded_to = {i: 0 for i in range(self.total)}

        self.port = port
        self.port_map = port_map

        self.server_thread = threading.Thread(target=self.run_server, daemon=True)
        self.server_thread.start()
        self.unique_packets_seen = set()


    def _hash(self, src, dst):
        h = hashlib.sha256(f"{src}-{dst}".encode()).hexdigest()
        val = int(h, 16)
        return val % self.m, val

    def _leading_zeros(self, hval):
        bits = bin(hval)[2:].zfill(256)
        return min(len(bits) - len(bits.lstrip('0')) + 1, self.max_val)

    def owns(self, idx):
        return self.start <= idx < self.end

    def receive_packet(self, src, dst, originating_id=None):
        self.total_received += 1
        self.unique_packets_seen.add((src, dst))

        idx, hval = self._hash(src, dst)
        g = self._leading_zeros(hval)
        if self.topkcuckoo.query(dst) >= self.ssthreshold:
            return False
        if self.A[idx] >= g:
            return False
    
        if self.owns(idx):
            p_prime = self.p
            old_val = self.A[idx]
            self.A[idx] = g
            self.c -= 1
            
                
            delta = -(1 / self.segment_size) * (2**-old_val - 2**-g)
            if g==self.max_val:
                delta = -(1 / self.segment_size) * (2**-old_val)
            self.p += delta
            #self.p = self.c * 1.0 / self.m

            # self.cmscu.increment(dst, p_prime)
            self.topkcuckoo.insert(dst, p_prime)

            self.sampled_count += 1
            return True
        else:
            # Do NOT touch A[idx] or self.p here!
            if originating_id is None or originating_id != self.switch_id:
                self.forward_packet(src, dst, idx)
                self.forwarded_count += 1
            return False


    def forward_packet(self, src, dst, idx):
        target_id = idx // self.segment_size
        if target_id == self.switch_id:
            return

        self.forwarded_to[target_id] += 1

        host, port = self.port_map[target_id]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                msg = f"{src},{dst},{self.switch_id}"
                s.sendall(msg.encode('utf-8'))
        except Exception as e:
            print(f"[Switch {self.switch_id}] Error forwarding to Switch {target_id}: {e}")

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(('127.0.0.1', self.port))
            server.listen()
            print(f"Switch {self.switch_id} listening on port {self.port}")
            while True:
                conn, _ = server.accept()
                with conn:
                    data = conn.recv(1024).decode('utf-8')
                    src, dst, origin = data.split(',')
                    self.receive_packet(src, dst, int(origin))

    def query(self, dst):
        # return self.cmscu.query(dst)
        return self.topkcuckoo.query(dst)


    def stats(self):
        lines = [
            f"[Switch {self.switch_id}]",
            f"  Total Packets Received: {self.total_received}",
            f"  Unique Packets Seen  : {len(self.unique_packets_seen)}",
            f"  Sampled Packets       : {self.sampled_count}",
            f"  Forwarded Packets     : {self.forwarded_count}"
        ]
        for target_id in sorted(self.forwarded_to):
            if target_id != self.switch_id:
                lines.append(f"    → Forwarded to Switch {target_id}: {self.forwarded_to[target_id]}")
        return "\n".join(lines)
