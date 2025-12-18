#!/usr/bin/env python3
"""Bridge inventory ingestion and normalization.

This script downloads bridge inventory data from the BTS ArcGIS Feature Service,
archives the raw response as GeoJSON and Parquet, and emits a cleaned CSV plus a
JSON schema summary for downstream processing.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import importlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, MutableMapping, Optional

import requests

DEFAULT_SERVICE_URL = (
    "https://services.arcgis.com/VTyQ9soqVUKDOhoj/ArcGIS/rest/services/"
    "Bridge_Inventory/FeatureServer/0"
)

RAW_FILENAME = "bridge_inventory"
NORMALIZED_COLUMNS = [
    "structure_id",
    "state_code",
    "county_code",
    "element_code",
    "condition_rating",
    "inspection_date",
    "latitude",
    "longitude",
]
FIELD_PRIORITIES = {
    "structure_id": [
        "STRUCTURE_NUMBER_008",
        "STRUCTURENUMBER",
        "structure_id",
        "bridge_id",
    ],
    "state_code": ["STATE_CODE_001", "state", "STATE"],
    "county_code": ["COUNTY_CODE_003", "county", "COUNTY"],
    "element_code": ["ELEMENT_NUMBER", "ELEMENT", "ELEMENT_NO", "element"],
    "condition_rating": [
        "CONDITION_RATING",
        "condition_rating",
        "Deck_Condition_Rating",
        "DECK_COND_058",
    ],
    "inspection_date": [
        "DATE_OF_INSPECT",
        "inspection_date",
        "INSP_DATE",
        "inspection",
    ],
}


class FeatureServiceError(RuntimeError):
    """Raised when the feature service returns a malformed response."""


class SchemaSummary(Dict[str, Any]):
    """Convenience type for schema summaries."""


def _timestamp_tag() -> str:
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _coerce_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # ArcGIS services often return epoch milliseconds
        try:
            return dt.datetime.utcfromtimestamp(value / 1000).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:  # NaN check
        return None
    return number


def _extract_first(attributes: MutableMapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in attributes:
            val = attributes[key]
            if val not in (None, ""):
                return val
    return None


def fetch_features(
    service_url: str,
    batch_size: int = 1000,
    max_features: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Stream all features from the ArcGIS Feature Service."""

    features: List[Dict[str, Any]] = []
    offset = 0
    while True:
        params = {
            "f": "json",
            "where": "1=1",
            "outFields": "*",
            "resultOffset": offset,
            "resultRecordCount": batch_size,
            "outSR": 4326,
        }
        response = requests.get(f"{service_url}/query", params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("features")
        if batch is None:
            raise FeatureServiceError("Response is missing 'features' key")

        features.extend(batch)
        if max_features is not None and len(features) >= max_features:
            return features[:max_features]

        if not payload.get("exceededTransferLimit"):
            break

        offset += batch_size

    return features


def normalize_features(features: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten the raw features into tabular rows."""

    normalized: List[Dict[str, Any]] = []
    for feature in features:
        attrs: MutableMapping[str, Any] = feature.get("attributes") or {}
        geom: MutableMapping[str, Any] = feature.get("geometry") or {}

        latitude = _coerce_float(geom.get("y") or geom.get("lat"))
        longitude = _coerce_float(geom.get("x") or geom.get("lon"))

        row = {
            "structure_id": _extract_first(attrs, FIELD_PRIORITIES["structure_id"]),
            "state_code": _extract_first(attrs, FIELD_PRIORITIES["state_code"]),
            "county_code": _extract_first(attrs, FIELD_PRIORITIES["county_code"]),
            "element_code": _extract_first(attrs, FIELD_PRIORITIES["element_code"]),
            "condition_rating": _coerce_float(
                _extract_first(attrs, FIELD_PRIORITIES["condition_rating"])
            ),
            "inspection_date": _coerce_date(
                _extract_first(attrs, FIELD_PRIORITIES["inspection_date"])
            ),
            "latitude": latitude,
            "longitude": longitude,
        }
        normalized.append(row)

    return normalized


def write_geojson(features: List[Dict[str, Any]], path: Path) -> Path:
    content = {"type": "FeatureCollection", "features": features}
    path.write_text(json.dumps(content, indent=2))
    return path


def write_parquet(records: List[Dict[str, Any]], path: Path) -> Optional[Path]:
    pyarrow_spec = importlib.util.find_spec("pyarrow")
    if pyarrow_spec is None:
        logging.warning("pyarrow not installed; skipping Parquet export at %s", path)
        return None

    parquet_spec = importlib.util.find_spec("pyarrow.parquet")
    if parquet_spec is None:
        logging.warning("pyarrow.parquet unavailable; skipping Parquet export at %s", path)
        return None

    pyarrow = importlib.import_module("pyarrow")
    parquet = importlib.import_module("pyarrow.parquet")

    table = pyarrow.Table.from_pylist(records)
    parquet.write_table(table, path)
    return path


def write_csv(rows: List[Dict[str, Any]], path: Path) -> Path:
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=NORMALIZED_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col) for col in NORMALIZED_COLUMNS})
    return path


def summarize_schema(rows: List[Dict[str, Any]]) -> SchemaSummary:
    summary: SchemaSummary = {
        "record_count": len(rows),
        "fields": {},
    }

    for column in NORMALIZED_COLUMNS:
        values = [row[column] for row in rows if row.get(column) not in (None, "")]
        types = sorted({type(v).__name__ for v in values}) if values else []
        example = values[0] if values else None
        summary["fields"][column] = {
            "types": types,
            "non_null": len(values),
            "example": example,
        }

    return summary


def archive_raw(
    features: List[Dict[str, Any]],
    raw_dir: Path,
    prefix: str,
) -> Dict[str, Optional[Path]]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = _timestamp_tag()
    paths: Dict[str, Optional[Path]] = {}

    geojson_path = raw_dir / f"{prefix}_{stamp}.geojson"
    paths["geojson"] = write_geojson(features, geojson_path)

    parquet_records = []
    for feature in features:
        record = dict(feature.get("attributes") or {})
        geometry = feature.get("geometry")
        record["geometry"] = json.dumps(geometry) if geometry is not None else None
        parquet_records.append(record)

    parquet_path = raw_dir / f"{prefix}_{stamp}.parquet"
    paths["parquet"] = write_parquet(parquet_records, parquet_path)
    return paths


def persist_processed(rows: List[Dict[str, Any]], processed_dir: Path, prefix: str) -> Dict[str, Path]:
    processed_dir.mkdir(parents=True, exist_ok=True)
    stamp = _timestamp_tag()

    csv_path = processed_dir / f"{prefix}_clean_{stamp}.csv"
    schema_path = processed_dir / f"{prefix}_schema_{stamp}.json"

    write_csv(rows, csv_path)
    schema = summarize_schema(rows)
    schema_path.write_text(json.dumps(schema, indent=2))

    return {"csv": csv_path, "schema": schema_path}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--service-url",
        default=DEFAULT_SERVICE_URL,
        help="ArcGIS feature service URL (base path without /query).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/bridge_inventory"),
        help="Root directory for raw and processed artifacts.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of records to request per API page.",
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=None,
        help="Cap the number of features to fetch (useful for smoke tests).",
    )
    parser.add_argument(
        "--prefix",
        default=RAW_FILENAME,
        help="Filename prefix for emitted artifacts.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = parse_args()

    logging.info("Fetching bridge inventory from %s", args.service_url)
    features = fetch_features(args.service_url, batch_size=args.batch_size, max_features=args.max_features)
    logging.info("Fetched %d features", len(features))

    raw_dir = args.out_dir / "raw"
    processed_dir = args.out_dir / "processed"

    archives = archive_raw(features, raw_dir=raw_dir, prefix=args.prefix)
    logging.info("Raw artifacts saved: %s", {k: str(v) for k, v in archives.items()})

    normalized = normalize_features(features)
    processed_paths = persist_processed(normalized, processed_dir=processed_dir, prefix=args.prefix)
    logging.info("Processed outputs saved: %s", {k: str(v) for k, v in processed_paths.items()})


if __name__ == "__main__":
    main()
