import asyncio
import json
import tempfile
import unittest
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.config import Settings
from app.schemas import WeatherSnapshot
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


if __name__ == "__main__":
    unittest.main()
