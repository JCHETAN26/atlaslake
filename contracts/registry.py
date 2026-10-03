"""Minimal Confluent Schema Registry client (REST) with an id → schema cache.

Kept dependency-light (httpx) because it also runs inside Spark Python workers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import fastavro
import httpx

from contracts import events as ev
from contracts import schemas
from core.config import settings

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryError(RuntimeError):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"schema registry {status}: {body}")
        self.status = status
        self.body = body


@dataclass(frozen=True)
class CompatibilityResult:
    compatible: bool
    messages: list[str]


class SchemaRegistry:
    def __init__(self, url: str | None = None, timeout: float = 10.0) -> None:
        self.url = (url or settings().kafka.schema_registry_url).rstrip("/")
        self._http = httpx.Client(base_url=self.url, timeout=timeout, headers={"Content-Type": CONTENT_TYPE})
        self._by_id: dict[int, Any] = {}
        self._version_by_id: dict[int, int] = {}

    def _req(self, method: str, path: str, **kw) -> Any:
        r = self._http.request(method, path, **kw)
        if r.status_code >= 400:
            raise RegistryError(r.status_code, r.text)
        return r.json()

    # -------------------------------------------------------------- writes
    def set_compatibility(self, subject: str | None, level: str) -> None:
        path = f"/config/{subject}" if subject else "/config"
        self._req("PUT", path, content=json.dumps({"compatibility": level}))

    def register(self, subject: str, schema_str: str) -> int:
        return int(
            self._req("POST", f"/subjects/{subject}/versions", content=json.dumps({"schema": schema_str}))[
                "id"
            ]
        )

    def delete_subject(self, subject: str, permanent: bool = False) -> None:
        try:
            self._req("DELETE", f"/subjects/{subject}")
            if permanent:
                self._req("DELETE", f"/subjects/{subject}?permanent=true")
        except RegistryError as e:
            if e.status != 404:
                raise

    # --------------------------------------------------------------- reads
    def check_compatibility(
        self, subject: str, schema_str: str, version: str = "latest"
    ) -> CompatibilityResult:
        try:
            body = self._req(
                "POST",
                f"/compatibility/subjects/{subject}/versions/{version}?verbose=true",
                content=json.dumps({"schema": schema_str}),
            )
        except RegistryError as e:
            if e.status == 404:  # no subject yet => anything is compatible
                return CompatibilityResult(True, [])
            raise
        return CompatibilityResult(bool(body.get("is_compatible")), list(body.get("messages") or []))

    def get_compatibility(self, subject: str | None = None) -> str:
        path = f"/config/{subject}?defaultToGlobal=true" if subject else "/config"
        body = self._req("GET", path)
        return body.get("compatibilityLevel") or body.get("compatibility")

    def subjects(self) -> list[str]:
        return list(self._req("GET", "/subjects"))

    def versions(self, subject: str) -> list[int]:
        return list(self._req("GET", f"/subjects/{subject}/versions"))

    def latest(self, subject: str) -> dict:
        return dict(self._req("GET", f"/subjects/{subject}/versions/latest"))

    def lookup(self, subject: str, schema_str: str) -> dict | None:
        try:
            return dict(self._req("POST", f"/subjects/{subject}", content=json.dumps({"schema": schema_str})))
        except RegistryError as e:
            if e.status == 404:
                return None
            raise

    def schema_by_id(self, schema_id: int) -> Any:
        """Parsed writer schema for an id (cached forever: ids are immutable)."""
        if schema_id not in self._by_id:
            body = self._req("GET", f"/schemas/ids/{schema_id}")
            self._by_id[schema_id] = fastavro.parse_schema(json.loads(body["schema"]))
        return self._by_id[schema_id]


def register_all(
    registry: SchemaRegistry, max_versions: dict[str, int] | None = None
) -> dict[str, dict[int, int]]:
    """Register every contract version in order (v1, v2, ...). Returns {topic: {version: schema_id}}."""
    out: dict[str, dict[int, int]] = {}
    for topic in ev.TOPICS:
        out[topic] = {}
        limit = (max_versions or {}).get(topic, schemas.latest_version(topic))
        for version in sorted(schemas.SCHEMA_FILES[topic]):
            if version > limit:
                continue
            out[topic][version] = registry.register(
                schemas.subject(topic), schemas.schema_text(topic, version)
            )
    return out
