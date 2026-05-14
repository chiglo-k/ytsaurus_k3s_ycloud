#!/usr/bin/env python3
from __future__ import annotations

import os
import yt.wrapper as yt

YT_PROXY = os.environ.get("YT_PROXY", "http://localhost:31103").replace("http://", "").replace("https://", "").rstrip("/")
YT_TOKEN = os.environ.get("YT_TOKEN")
REF_PATH = os.environ.get("COUNTRY_REF_PATH", "//home/ref/iso_dim")
SILVER_PATH = os.environ.get("SILVER_PATH", "//home/silver_stage/greenhub_telemetry")

ISO_SCHEMA = [
    {"name": "country_code", "type": "string"},
    {"name": "country_name", "type": "string"},
    {"name": "alpha3_code", "type": "string"},
    {"name": "region", "type": "string"},
    {"name": "sub_region", "type": "string"},
]

# MVP seed. Extend this list if/when new country codes appear in source.
ISO_ROWS = [
    {
        "country_code": "SK",
        "country_name": "Slovakia",
        "alpha3_code": "SVK",
        "region": "Europe",
        "sub_region": "Eastern Europe",
    },
]

SILVER_EXTRA_COLUMNS = [
    {"name": "country_name", "type": "string"},
    {"name": "country_alpha3_code", "type": "string"},
    {"name": "country_region", "type": "string"},
    {"name": "country_sub_region", "type": "string"},
]


def client() -> yt.YtClient:
    return yt.YtClient(proxy=YT_PROXY, token=YT_TOKEN)


def create_or_replace_iso_dim(c: yt.YtClient) -> None:
    parent = REF_PATH.rsplit("/", 1)[0]
    if parent:
        c.create("map_node", parent, recursive=True, ignore_existing=True)

    if c.exists(REF_PATH):
        c.remove(REF_PATH, force=True)

    c.create("table", REF_PATH, recursive=True, attributes={"schema": ISO_SCHEMA})
    c.write_table(f"<append=%false>{REF_PATH}", ISO_ROWS, format="json")
    print(f"[OK] recreated {REF_PATH} with {len(ISO_ROWS)} row(s)")


def ensure_silver_schema_columns(c: yt.YtClient) -> None:
    if not c.exists(SILVER_PATH):
        raise RuntimeError(f"Silver table does not exist: {SILVER_PATH}")

    schema = list(c.get(f"{SILVER_PATH}/@schema"))
    existing = {col["name"] for col in schema}

    to_add = [col for col in SILVER_EXTRA_COLUMNS if col["name"] not in existing]
    if not to_add:
        print(f"[OK] silver schema already has ISO enrichment columns: {SILVER_PATH}")
        return

    new_schema = schema[:]
    insert_after = "country_code"
    insert_idx = next((i for i, col in enumerate(new_schema) if col["name"] == insert_after), None)

    if insert_idx is None:
        new_schema.extend(to_add)
    else:
        for col in reversed(to_add):
            new_schema.insert(insert_idx + 1, col)

    try:
        c.alter_table(SILVER_PATH, schema=new_schema)
        print(f"[OK] altered silver schema, added: {', '.join(col['name'] for col in to_add)}")
    except Exception:
        print("[ERROR] Could not alter silver schema in-place.")
        print("If YTsaurus refuses schema alter, recreate silver table with these extra columns and reload silver.")
        raise


def main() -> int:
    c = client()
    create_or_replace_iso_dim(c)
    ensure_silver_schema_columns(c)
    print("[DONE] iso_dim setup and silver schema update complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
