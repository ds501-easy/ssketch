import numpy as np
import hashlib
import random

class CMSCU:
    def __init__(self, width, depth):
        self.width = width
        self.depth = depth
        self.sketch = np.zeros((depth, width), dtype=int)

    def _hash(self, item, seed):
        return int(hashlib.sha256(f"{seed}-{item}".encode()).hexdigest(), 16) % self.width
    
    def increment(self, item, p):
        indices = [self._hash(item, i) for i in range(self.depth)]
        current_vals = [self.sketch[i][idx] for i, idx in enumerate(indices)]

        min_value = min(current_vals)

        inv_p = 1 / p
        delta = int(inv_p)
        if random.random() < (inv_p - delta):
            delta += 1

        new_value = min_value + delta
        for i in range(self.depth):
            index = indices[i]
            self.sketch[i][index] = max(self.sketch[i][index], new_value)

    def query(self, item):
        indices = [self._hash(item, i) for i in range(self.depth)]
        return min(self.sketch[i][indices[i]] for i in range(self.depth))
