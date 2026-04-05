from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
import pandas as pd

from app.domain.entities.location import Location


@dataclass
class OpenMeteoParameters:
    latitude: float
    longitude: float
    hourly: str
    timezone: str
    forecast_days: int


class OpenMeteoRepository:
    def __init__(
        self,
    ) -> None:
        self.base_url = "https://api.open-meteo.com/v1/forecast"

        self.HOURLY_VARS = [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "pressure_msl",
            "surface_pressure",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "weather_code",
            "snow_depth",
            "freezing_level_height",
            "visibility",
            "is_day",
        ]

    def fetch_open_meteo_hourly(self, location: Location) -> pd.DataFrame:
        params: OpenMeteoParameters = OpenMeteoParameters(
            latitude=location.latitude,
            longitude=location.longitude,
            hourly=",".join(self.HOURLY_VARS),
            timezone="Asia/Tokyo",
            forecast_days=7,
        )

        response = requests.get(self.base_url, params=params.__dict__, timeout=30)
        response.raise_for_status()
        data = response.json()

        hourly = data.get("hourly")
        if not hourly:
            raise ValueError("APIレスポンスに hourly がありません")

        times = hourly.get("time")
        if not times:
            raise ValueError("APIレスポンスに hourly.time がありません")

        row_count = len(times)

        records: list[dict[str, Any]] = []
        ingested_at = datetime.now(timezone.utc)

        for i in range(row_count):
            record: dict[str, Any] = {
                "location_id": location.location_id,
                "location_name": location.location_name,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "observed_at": pd.to_datetime(times[i], utc=False),
                "ingested_at": ingested_at,
            }

            for var in self.HOURLY_VARS:
                values = hourly.get(var)
                value = values[i] if values and i < len(values) else None

                if var == "is_day":
                    if value is None:
                        record[var] = None
                    else:
                        record[var] = bool(value)
                else:
                    record[var] = value

            records.append(record)

        df = pd.DataFrame(records)

        df["observed_at"] = pd.to_datetime(df["observed_at"]).dt.tz_localize(
            "Asia/Tokyo"
        )
        df["observed_at"] = df["observed_at"].dt.tz_convert("UTC")

        return df
