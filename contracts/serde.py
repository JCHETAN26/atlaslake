"""Confluent wire format: ``0x00 | schema_id (4 bytes, big-endian) | avro binary``."""

from __future__ import annotations

import io
import struct
from datetime import UTC, datetime
from typing import Any

import fastavro

MAGIC = 0
HEADER = struct.Struct(">bI")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class DecodeError(Exception):
    """Raised when a payload cannot be turned into a record. ``code`` is a stable DLQ reason."""

    def __init__(self, code: str, message: str, schema_id: int | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.schema_id = schema_id


def serialize(record: dict[str, Any], parsed_schema: Any, schema_id: int) -> bytes:
    buf = io.BytesIO()
    buf.write(HEADER.pack(MAGIC, schema_id))
    fastavro.schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()


def schema_id_of(payload: bytes | None) -> int:
    if payload is None or len(payload) < HEADER.size:
        raise DecodeError("CORRUPT_PAYLOAD", "payload shorter than wire-format header")
    magic, schema_id = HEADER.unpack_from(payload)
    if magic != MAGIC:
        raise DecodeError("CORRUPT_PAYLOAD", f"bad magic byte {magic}")
    return int(schema_id)


def _to_micros(value: Any) -> Any:
    if isinstance(value, datetime):
        delta = value - EPOCH
        return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return value


def deserialize(payload: bytes, writer_schema: Any, reader_schema: Any | None = None) -> dict[str, Any]:
    """Decode with the *writer's* schema, resolving into ``reader_schema`` (schema evolution)."""
    buf = io.BytesIO(payload)
    buf.seek(HEADER.size)
    try:
        rec = fastavro.schemaless_reader(buf, writer_schema, reader_schema)
    except Exception as exc:  # fastavro raises a variety of types on garbage input
        raise DecodeError("AVRO_DECODE_ERROR", f"{type(exc).__name__}: {exc}") from exc
    if buf.tell() != len(payload):
        raise DecodeError("AVRO_DECODE_ERROR", f"{len(payload) - buf.tell()} trailing bytes")
    if not isinstance(rec, dict):
        raise DecodeError("AVRO_DECODE_ERROR", "decoded value is not a record")
    for key in ("event_timestamp", "signup_timestamp"):
        if key in rec:
            rec[key] = _to_micros(rec[key])
    return rec
