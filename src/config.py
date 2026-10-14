"""Central configuration for the Mars weather streaming pipeline.

All settings are read from environment variables with sensible local
defaults, so the same code runs on a laptop and on a server without
editing files. Keep secrets out of this file — the NASA key comes from
the environment, never from a hardcoded value.
"""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    """Runtime settings for producer and streaming job."""

    # NASA API — get a free key at https://api.nasa.gov
    nasa_api_key: str = os.getenv("NASA_API_KEY", "DEMO_KEY")
    nasa_base_url: str = "https://api.nasa.gov/insight_weather/"
    nasa_feedtype: str = "json"
    nasa_version: str = "1.0"
    request_timeout_seconds: int = 20

    # Kafka
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "mars-weather-raw")
    kafka_client_id: str = os.getenv("KAFKA_CLIENT_ID", "mars-producer")

    # Paths — relative to the repo root by default
    output_base: str = os.getenv("OUTPUT_BASE", "data/output")
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "data/checkpoints")
    sample_data_path: str = os.getenv(
        "SAMPLE_DATA_PATH", "data/sample_mars_weather.json"
    )

    # Polling
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

    # Spark
    spark_app_name: str = "MarsWeatherStreaming"
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    # How often the streaming query writes a micro-batch
    trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    # Keep late data for up to 1 day for watermarking
    watermark_delay: str = "1 day"

    @property
    def bronze_path(self) -> str:
        """Where raw JSON lands."""
        return f"{self.output_base}/bronze"

    @property
    def silver_path(self) -> str:
        """Where cleaned Parquet lands."""
        return f"{self.output_base}/silver"

    @property
    def gold_path(self) -> str:
        """Where daily aggregates land."""
        return f"{self.output_base}/gold"

    @property
    def nasa_api_url(self) -> str:
        """Full InSight weather URL with query params."""
        return (
            f"{self.nasa_base_url}?api_key={self.nasa_api_key}"
            f"&feedtype={self.nasa_feedtype}&ver={self.nasa_version}"
        )


# Single shared instance — import this from other modules.
settings = Settings()


def ensure_dirs() -> None:
    """Create output and checkpoint directories if missing.

    The Spark job also creates these, but the producer and local
    scripts benefit from calling this early so a missing folder
    never causes a confusing error halfway through a run.
    """
    import pathlib

    for path in (
        settings.bronze_path,
        settings.silver_path,
        settings.gold_path,
        settings.checkpoint_dir,
    ):
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def summary() -> str:
    """One-line human-readable summary, handy for startup logs."""
    return (
        f"topic={settings.kafka_topic} "
        f"brokers={settings.kafka_bootstrap_servers} "
        f"output={settings.output_base}"
    )
