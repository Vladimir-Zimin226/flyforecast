import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.flight_status.collect_kunashir_status import (
    BoardFlight,
    WeatherSnapshot,
    build_dataset_rows,
    parse_board_text_date,
)


class CollectKunashirStatusTests(unittest.TestCase):
    def test_extracts_russian_month_date_from_combined_status(self) -> None:
        observed_at = datetime(2026, 6, 20, 10, 0, tzinfo=ZoneInfo("Asia/Sakhalin"))

        self.assertEqual(
            parse_board_text_date("Совмещён с HZ 3034 21 Июн", observed_at),
            "2026-06-21",
        )
        self.assertEqual(
            parse_board_text_date("Перенесён с 20 Июн на 21 Июн", observed_at),
            "2026-06-21",
        )

    def test_combined_status_date_overrides_time_only_actual_cell(self) -> None:
        observed_at = datetime(2026, 6, 20, 10, 0, tzinfo=ZoneInfo("Asia/Sakhalin"))
        rows = build_dataset_rows(
            flights=[
                BoardFlight(
                    source="airportus",
                    source_url="https://airportus.ru/board/",
                    direction="departure",
                    flight_numbers="HZ-3036 SU-4601",
                    route="Южно-Курильск",
                    scheduled_raw="19.06 11:20",
                    actual_raw="09:50",
                    status_raw="Совмещён с HZ 3034 21 Июн",
                    radar_flight_number="",
                    raw_row_text=(
                        "HZ-3036 SU-4601 | Южно-Курильск | 19.06 11:20 | "
                        "09:50 | Совмещён с HZ 3034 21 Июн"
                    ),
                )
            ],
            weather=WeatherSnapshot(),
            observed_at=observed_at,
        )

        self.assertEqual(rows[0].status_normalized, "combined")
        self.assertEqual(rows[0].flight_date, "2026-06-19")
        self.assertEqual(rows[0].flight_time, "11:20")
        self.assertEqual(rows[0].actual_date, "2026-06-21")
        self.assertEqual(rows[0].actual_time, "09:50")


if __name__ == "__main__":
    unittest.main()
