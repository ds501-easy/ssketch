import hashlib
import random

class TopkCuckooHash:
    def __init__(self, width, k, cmscu, threshold):
        self.width = width
        self.k = k
        self.buckets = [[{"key": None, "val": 0} for _ in range(k)] for _ in range(width)]
        self.cmscu = cmscu
        self.threshold = threshold

    def _hash1(self, flow_id):
        return int(hashlib.sha256(("salt1-" + flow_id).encode()).hexdigest(), 16) % self.width

    def _hash2(self, flow_id):
        return int(hashlib.sha256(("tlas2-" + flow_id).encode()).hexdigest(), 16) % self.width

    def insert(self, flow_id, p):
        idx1 = self._hash1(flow_id)
        idx2 = self._hash2(flow_id)
        bucket1 = self.buckets[idx1]
        bucket2 = self.buckets[idx2]

        inv_p = 1 / p
        delta = int(inv_p)
        if random.random() < (inv_p - delta):
            delta += 1

        # Step 1: Look for existing flow
        #for bucket in [bucket1, bucket2]:
        #    for entry in bucket:
        #        if entry["key"] == flow_id:
        #            entry["val"] += delta
        #            bucket.sort(key=lambda x: x["val"], reverse=True)
        #            return

        # Step 2: Empty slot?
        #for bucket in [bucket1, bucket2]:
        #    for entry in bucket:
        #        if entry["key"] is None:
        #            entry["key"] = flow_id
        #            entry["val"] = delta
        #            bucket.sort(key=lambda x: x["val"], reverse=True)
        #            return

        # Step 3: No empty slot → Insert into CU
        self.cmscu.increment(flow_id, p)

        # Step 4: Query CU, if estimate > threshold, replace
        cu_estimate = self.cmscu.query(flow_id)
        if cu_estimate >= self.threshold:
            # Find bucket with the smallest tail
            if bucket1[-1]["val"] <= bucket2[-1]["val"]:
                victim_bucket = bucket1
            else:
                victim_bucket = bucket2

            # Evict the last entry
            evicted_key = victim_bucket[-1]["key"]
            evicted_val = victim_bucket[-1]["val"]

            #if evicted_key is not None:
                # Add evicted flow into CU with its current count
            #    for _ in range(int(evicted_val)):
            #        self.cmscu.increment(evicted_key, 1.0)

            # Replace with new flow
            victim_bucket[-1] = {"key": flow_id, "val": cu_estimate}
            victim_bucket.sort(key=lambda x: x["val"], reverse=True)

    def query(self, flow_id):
        idx1 = self._hash1(flow_id)
        idx2 = self._hash2(flow_id)
        for entry in self.buckets[idx1]:
            if entry["key"] == flow_id:
                return entry["val"]
        for entry in self.buckets[idx2]:
            if entry["key"] == flow_id:
                return entry["val"]
        return 0    #self.cmscu.query(flow_id)

    def dump_buckets(self, filename="topkcuckoohash_buckets.txt"):
        with open(filename, "w") as f:
            for i, bucket in enumerate(self.buckets):
                f.write(f"Bucket {i}:\n")
                for entry in bucket:
                    key = entry["key"] if entry["key"] else "None"
                    val = entry["val"]
                    f.write(f"  key: {key}, val: {val:.2f}\n")
                f.write("\n")
