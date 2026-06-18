import asyncio
import json
import tempfile
import unittest
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.config import Settings
from app.schemas import FlightScheduleSnapshot, WeatherSnapshot
from app.services import weather


class WeatherCircuitBreakerTests(unittest.TestCase):
    def test_live_horizon_cache_ttl_is_shorter_than_regular_forecast_ttl(self) -> None:
        settings = Settings(
            weather_cache_fresh_hours=1,
            weather_live_cache_fresh_minutes=15,
        )

        self.assertAlmostEqual(weather._fresh_cache_max_age_hours(settings, horizon_days=0), 0.25)
        self.assertAlmostEqual(weather._fresh_cache_max_age_hours(settings, horizon_days=1), 0.25)
        self.assertEqual(weather._fresh_cache_max_age_hours(settings, horizon_days=2), 1.0)

    def test_live_horizon_refreshes_cache_older_than_live_ttl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = str(Path(tmp_dir) / "weather_cache.sqlite")
            target_date = date.today()
            settings = Settings(
                weather_forecast_cache_path=cache_path,
                weather_cache_fresh_hours=1,
                weather_live_cache_fresh_minutes=15,
                weather_cache_stale_hours=72,
                met_no_fallback_enabled=False,
            )
            cached_snapshot = WeatherSnapshot(
                source="open-meteo",
                available=True,
                visibility=640,
                cloud_cover_low=71,
            )
            refreshed_snapshot = WeatherSnapshot(
                source="open-meteo",
                available=True,
                visibility=8000,
                cloud_cover_low=20,
            )
            fetched_at = (weather._utc_now() - timedelta(minutes=30)).isoformat()

            with closing(weather._connect_weather_cache(cache_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO weather_forecast_cache (
                        target_date,
                        provider,
                        payload_json,
                        fetched_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        target_date.isoformat(),
                        weather.OPEN_METEO_CACHE_PROVIDER,
                        json.dumps(cached_snapshot.model_dump()),
                        fetched_at,
                    ),
                )
                conn.commit()

            live_fetch = AsyncMock(return_value=({target_date: refreshed_snapshot}, None))
            with patch("app.services.weather.get_settings", return_value=settings):
                with patch("app.services.weather._fetch_open_meteo_snapshots", live_fetch):
                    snapshot = asyncio.run(weather.fetch_weather_for_date(target_date))

            self.assertEqual(snapshot.source, "open-meteo")
            self.assertEqual(snapshot.visibility, 8000)
            live_fetch.assert_awaited_once()

    def test_open_circuit_uses_stale_cache_without_live_open_meteo_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = str(Path(tmp_dir) / "weather_cache.sqlite")
            target_date = date.today() + timedelta(days=1)
            settings = Settings(
                weather_forecast_cache_path=cache_path,
                weather_cache_fresh_hours=1,
                weather_cache_stale_hours=72,
                open_meteo_failure_cooldown_minutes=30,
                met_no_fallback_enabled=False,
            )
            cached_snapshot = WeatherSnapshot(
                source="open-meteo",
                available=True,
                visibility=640,
                cloud_cover_low=71,
            )
            fetched_at = (weather._utc_now() - timedelta(hours=4)).isoformat()

            with closing(weather._connect_weather_cache(cache_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO weather_forecast_cache (
                        target_date,
                        provider,
                        payload_json,
                        fetched_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        target_date.isoformat(),
                        weather.OPEN_METEO_CACHE_PROVIDER,
                        json.dumps(cached_snapshot.model_dump()),
                        fetched_at,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO weather_provider_state (
                        provider,
                        failure_count,
                        last_failure_at,
                        last_failure_reason,
                        cooldown_until
                    )
                    VALUES (?, 1, ?, ?, ?)
                    """,
                    (
                        weather.OPEN_METEO_STATE_PROVIDER,
                        weather._utc_now().isoformat(),
                        "Open-Meteo is temporarily unavailable.",
                        (weather._utc_now() + timedelta(minutes=30)).isoformat(),
                    ),
                )
                conn.commit()

            live_fetch = AsyncMock(side_effect=AssertionError("Open-Meteo live fetch should be skipped"))
            with patch("app.services.weather.get_settings", return_value=settings):
                with patch("app.services.weather._fetch_open_meteo_snapshots", live_fetch):
                    snapshot = asyncio.run(weather.fetch_weather_for_date(target_date))

            self.assertEqual(snapshot.source, "open-meteo-cache-stale")
            self.assertEqual(snapshot.visibility, 640)
            live_fetch.assert_not_awaited()

    def test_fresh_cache_uses_short_schedule_window_when_hourly_rows_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_path = str(Path(tmp_dir) / "weather_cache.sqlite")
            target_date = date.today()
            settings = Settings(
                weather_forecast_cache_path=cache_path,
                weather_cache_fresh_hours=1,
                weather_live_cache_fresh_minutes=15,
                weather_scheduled_window_duration_hours=1.5,
                met_no_fallback_enabled=False,
            )
            cached_snapshot = WeatherSnapshot(
                source="open-meteo",
                available=True,
                visibility=540,
                cloud_cover_low=95,
                fog_low_cloud_risk_level="high",
                flight_window_available=False,
                hourly_rows=[
                    {
                        "hour": 14,
                        "temperature_2m": 15.5,
                        "relative_humidity_2m": 83,
                        "dew_point_2m": 12.7,
                        "pressure_msl": 1015.1,
                        "cloud_cover": 97,
                        "cloud_cover_low": 89,
                        "precipitation": 0,
                        "wind_speed_10m": 10.1,
                        "wind_gusts_10m": 31.3,
                        "wind_direction_10m": 141,
                        "weather_code": 3,
                        "visibility": 30420,
                    },
                    {
                        "hour": 15,
                        "temperature_2m": 15.8,
                        "relative_humidity_2m": 83,
                        "dew_point_2m": 13.0,
                        "pressure_msl": 1014.7,
                        "cloud_cover": 88,
                        "cloud_cover_low": 43,
                        "precipitation": 0,
                        "wind_speed_10m": 10.7,
                        "wind_gusts_10m": 28.4,
                        "wind_direction_10m": 131,
                        "weather_code": 3,
                        "visibility": 1540,
                    },
                    {
                        "hour": 16,
                        "temperature_2m": 14.8,
                        "relative_humidity_2m": 88,
                        "dew_point_2m": 12.8,
                        "pressure_msl": 1014.8,
                        "cloud_cover": 97,
                        "cloud_cover_low": 91,
                        "precipitation": 0,
                        "wind_speed_10m": 13.1,
                        "wind_gusts_10m": 33.8,
                        "wind_direction_10m": 133,
                        "weather_code": 3,
                        "visibility": 26340,
                    },
                ],
            )
            schedule = FlightScheduleSnapshot(
                source="test",
                available=True,
                first_departure_hour=14,
                first_scheduled_hour=18,
                last_scheduled_hour=18,
                active_flight_hour=18,
                active_flight_time="18:05",
                active_flight_numbers="HZ-3035",
                active_flight_status="scheduled",
            )

            with closing(weather._connect_weather_cache(cache_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO weather_forecast_cache (
                        target_date,
                        provider,
                        payload_json,
                        fetched_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        target_date.isoformat(),
                        weather.OPEN_METEO_CACHE_PROVIDER,
                        json.dumps(cached_snapshot.model_dump()),
                        weather._utc_now().isoformat(),
                    ),
                )
                conn.commit()

            live_fetch = AsyncMock(side_effect=AssertionError("Fresh cache should be used"))
            with patch("app.services.weather.get_settings", return_value=settings):
                with patch("app.services.weather._fetch_open_meteo_snapshots", live_fetch):
                    snapshot = asyncio.run(weather.fetch_weather_for_date(target_date, schedule=schedule))

            self.assertEqual(snapshot.source, "open-meteo-cache")
            self.assertEqual(snapshot.aggregation_window_start_hour, 14)
            self.assertEqual(snapshot.aggregation_window_end_hour, 16)
            self.assertEqual(snapshot.aggregation_window_hours, 3)
            self.assertTrue(snapshot.flight_window_available)
            self.assertGreater(snapshot.visibility or 0, 5000)
            live_fetch.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
