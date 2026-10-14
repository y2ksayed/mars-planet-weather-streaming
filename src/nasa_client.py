"""Client for NASA's Mars weather data.

The real InSight lander stopped sending weather data, but the API still
serves the historical archive, which is perfect for backfilling a
streaming pipeline from a REST source.

If the request fails (no key, rate limit, network down), we fall back to
the bundled sample file so the demo keeps working offline. This keeps
the producer simple — it never has to handle a "no data" case.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

import requests

from config import settings

logger = logging.getLogger(__name__)

# Keys the API uses for the per-sol blocks; anything else is metadata.
_RESERVED_KEYS = {"sol_keys", "validity_checks"}


def fetch_from_api() -> Dict[str, Any]:
    """Fetch the raw InSight weather payload from NASA.

    Raises:
        requests.RequestException: on network or HTTP errors.
    """
    logger.info("Fetching Mars weather from NASA API")
    response = requests.get(
        settings.nasa_api_url,
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def load_sample_data() -> Dict[str, Any]:
    """Load the bundled sample payload from disk."""
    path = Path(settings.sample_data_path)
    logger.info("Loading sample Mars weather from %s", path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _safe_float(value: Any) -> float | None:
    """Convert a value to float, returning None for missing data."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_sol(sol_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Turn one raw sol block into a flat, typed record.

    The InSight format nests everything (AT.av, PRE.av, ...). Flattening
    here keeps the Spark schema simple downstream.
    """
    block = payload.get(sol_key, {})
    air_temp = block.get("AT", {})
    pressure = block.get("PRE", {})
    wind = block.get("HWS", {})

    now = datetime.now(timezone.utc).isoformat()

    return {
        "sol": int(sol_key),
        "terrestrial_date": block.get("First_UTC", "")[:10] or None,
        "season": block.get("Season"),
        # Air temperature is in Celsius from the API.
        "air_temp_avg_c": _safe_float(air_temp.get("av")),
        "air_temp_min_c": _safe_float(air_temp.get("mn")),
        "air_temp_max_c": _safe_float(air_temp.get("mx")),
        "air_temp_samples": air_temp.get("ct"),
        # Pressure is in Pascals.
        "pressure_pa": _safe_float(pressure.get("av")),
        "pressure_samples": pressure.get("ct"),
        # Horizontal wind speed is in m/s.
        "wind_speed_ms": _safe_float(wind.get("av")),
        "wind_speed_max_ms": _safe_float(wind.get("mx")),
        "ingested_at": now,
        "source": "nasa_insight_api",
    }


def iter_observations(payload: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Yield one normalized record per sol in the payload."""
    sol_keys: List[str] = payload.get("sol_keys", [])
    for sol_key in sol_keys:
        if sol_key in _RESERVED_KEYS:
            continue
        try:
            yield normalize_sol(sol_key, payload)
        except (ValueError, TypeError) as exc:
            # One bad sol should not kill the whole batch.
            logger.warning("Skipping sol %s: %s", sol_key, exc)


def get_observations(use_sample: bool = False) -> List[Dict[str, Any]]:
    """Get normalized observations, trying the API first.

    Args:
        use_sample: skip the API and go straight to the sample file.
    """
    if not use_sample:
        try:
            payload = fetch_from_api()
        except requests.RequestException as exc:
            logger.warning(
                "NASA API request failed (%s); using sample data", exc
            )
            payload = load_sample_data()
    else:
        payload = load_sample_data()

    observations = list(iter_observations(payload))
    logger.info("Normalized %d sol observations", len(observations))
    return observations


if __name__ == "__main__":
    # Quick manual check: python src/nasa_client.py
    logging.basicConfig(level=logging.INFO)
    for obs in get_observations():
        print(json.dumps(obs, indent=2))
