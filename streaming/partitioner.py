"""Deterministic key -> partition assignment.

Uses md5 rather than the builtin hash() because hash() is randomized per
process (PYTHONHASHSEED), which would make partition assignment for a given
key inconsistent between the producer and consumer processes -- or even
between two runs of the same process.
"""

from __future__ import annotations

import hashlib


def key_to_partition(key: str, num_partitions: int) -> int:
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    digest = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], byteorder="big") % num_partitions
