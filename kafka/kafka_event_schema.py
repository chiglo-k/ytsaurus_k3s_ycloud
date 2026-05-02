from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import uuid


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class KafkaEvent:
    source: str
    stream_id: str
    event_type: str
    event_ts: str
    entity_id: str
    payload_json: str
    ingest_batch_id: str
    ingest_ts: str

    @classmethod
    def from_payload(
        cls,
        source: str,
        stream_id: str,
        event_type: str,
        entity_id: str | int,
        payload: dict,
        ingest_batch_id: str | None = None,
    ) -> "KafkaEvent":
        return cls(
            source=source,
            stream_id=stream_id,
            event_type=event_type,
            event_ts=_utc_now(),
            entity_id=str(entity_id),
            payload_json=json.dumps(payload, ensure_ascii=False),
            ingest_batch_id=ingest_batch_id or str(uuid.uuid4()),
            ingest_ts=_utc_now(),
        )

    def to_dict(self) -> dict:
        return asdict(self)
