from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

JsonScalar = str | int | float | bool | None
JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
SqlValue = str | int | float | bool | None
SqlRow = tuple[SqlValue, ...]
PgValue = SqlValue | dt.datetime
PgRow = tuple[PgValue, ...]


class MigrationPreflightError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TableDigest:
    name: str
    row_count: int
    null_counts: dict[str, int]
    min_times: dict[str, int | None]
    max_times: dict[str, int | None]
    content_hash: str

    @property
    def table(self) -> str:
        return self.name

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "row_count": self.row_count,
            "null_counts": {key: value for key, value in self.null_counts.items()},
            "min_times": {key: value for key, value in self.min_times.items()},
            "max_times": {key: value for key, value in self.max_times.items()},
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class StorageDigest:
    fingerprint: str
    tables: tuple[TableDigest, ...]

    @property
    def tables_by_name(self) -> dict[str, TableDigest]:
        return {table.name: table for table in self.tables}

    def table(self, name: str) -> TableDigest:
        return self.tables_by_name[name]

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "fingerprint": self.fingerprint,
            "tables": {
                table.name: table.to_json()
                for table in self.tables
            },
        }


@dataclass(frozen=True, slots=True)
class MigrationReceipt:
    run_id: str
    source_fingerprint: str
    completed: bool
    source: StorageDigest
    target: StorageDigest | None

    def to_json(self) -> dict[str, JsonValue]:
        visible_digest = self.target if self.target is not None else self.source
        return {
            "run_id": self.run_id,
            "source_fingerprint": self.source_fingerprint,
            "completed": self.completed,
            "tables": {
                table.name: table.to_json()
                for table in visible_digest.tables
            },
            "source": self.source.to_json(),
            "target": self.target.to_json() if self.target is not None else None,
        }
