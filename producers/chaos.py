"""Controlled fault injection for producers (Phases 7, 11, 12, 37-39).

Every fault is *deterministic* given the seed and the event sequence, and every physical delivery is
logged with its fault label so reconciliation can explain each record downstream.
"""

from __future__ import annotations

import copy
import random
from dataclasses import dataclass, field
from typing import Any

from contracts import events as ev

INVALID_MUTATIONS = ("NULL_ID", "NEGATIVE_AMOUNT", "UNKNOWN_STATUS", "UNKNOWN_CURRENCY")
CORRUPTIONS = ("TRUNCATED_AVRO", "BAD_MAGIC", "UNKNOWN_SCHEMA_ID", "RANDOM_BYTES", "NOT_AVRO_JSON")


@dataclass(frozen=True)
class ChaosConfig:
    duplicate_probability: float = 0.0
    late_probability: float = 0.0
    late_delay_s: float = 30.0  # wall-clock delay before a "late" delivery is released
    reorder_window: int = 0  # >0: shuffle deliveries inside windows of this size (out-of-order)
    invalid_probability: float = 0.0
    corrupt_probability: float = 0.0
    seed: int = 1234
    topics: tuple[str, ...] = field(default_factory=lambda: ev.TOPICS)

    @property
    def enabled(self) -> bool:
        return any(
            (
                self.duplicate_probability,
                self.late_probability,
                self.reorder_window,
                self.invalid_probability,
                self.corrupt_probability,
            )
        )


class Chaos:
    def __init__(self, cfg: ChaosConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)

    def roll(self, p: float) -> bool:
        return p > 0 and self.rng.random() < p

    def mutate_invalid(self, rec: dict[str, Any], topic: str) -> tuple[dict[str, Any], str]:
        """Produce a schema-valid but semantically invalid copy (keeps event_id for traceability)."""
        bad = copy.deepcopy(rec)
        options = [m for m in INVALID_MUTATIONS if _applicable(m, bad, topic)]
        mutation = self.rng.choice(options) if options else "NULL_ID"
        if mutation == "NULL_ID":
            key = ev.PARTITION_KEY[topic] if topic != ev.TOPIC_INVENTORY else "store_id"
            bad[key] = ""
        elif mutation == "NEGATIVE_AMOUNT":
            field_name = "amount_minor" if bad.get("amount_minor") is not None else "unit_price_minor"
            bad[field_name] = -abs(int(bad[field_name]) or 1)
        elif mutation == "UNKNOWN_STATUS":
            bad["status"] = "TELEPORTED"
        elif mutation == "UNKNOWN_CURRENCY":
            bad["currency"] = "XXX"
        return bad, f"invalid:{mutation}"

    def corrupt(self, payload: bytes) -> tuple[bytes, str]:
        kind = self.rng.choice(CORRUPTIONS)
        if kind == "TRUNCATED_AVRO":
            out = payload[: max(5, len(payload) // 2)]
        elif kind == "BAD_MAGIC":
            out = b"\x07" + payload[1:]
        elif kind == "UNKNOWN_SCHEMA_ID":
            out = b"\x00" + (999_999).to_bytes(4, "big") + payload[5:]
        elif kind == "RANDOM_BYTES":
            out = bytes(self.rng.getrandbits(8) for _ in range(len(payload)))
        else:
            out = b'{"this": "is json, not avro"}'
        return out, f"corrupt:{kind}"


def _applicable(mutation: str, rec: dict[str, Any], topic: str) -> bool:
    if mutation == "NEGATIVE_AMOUNT":
        return rec.get("amount_minor") is not None or rec.get("unit_price_minor") is not None
    if mutation == "UNKNOWN_STATUS":
        return "status" in rec
    if mutation == "UNKNOWN_CURRENCY":
        return rec.get("currency") is not None
    return True
