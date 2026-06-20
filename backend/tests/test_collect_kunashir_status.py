import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from pipelines.flight_status.collect_kunashir_status import (
    BoardFlight,
    WeatherSnapshot,
    build_dataset_rows,
    parse_board_html,
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

    def test_text_fallback_parser_extracts_kunashir_rows_without_board_divs(self) -> None:
        html = """
        <html><body>
        <div>Рейс</div><div>Авиакомпания</div><div>Город</div>
        <div>Время</div><div>по расписанию</div><div>Время</div><div>фактическое</div><div>Cтатус</div>
        <div>HZ-3036 SU-4601</div>
        <div>Южно-Курильск</div>
        <div>По расписанию  19.06 11:20</div>
        <div>15:00</div>
        <div>Задержан до 15:00</div>
        <div>Регистрация: 00:00 - 00:00</div>
        <div>Рейс</div><div>Авиакомпания</div><div>Город</div>
        <div>Время</div><div>по расписанию</div><div>Время</div><div>фактическое</div><div>Cтатус</div>
        <div>HZ-3037 SU-4602</div>
        <div>Южно-Курильск</div>
        <div>По расписанию  19.06 14:45</div>
        <div>17:00</div>
        <div>Задержан до 17:00</div>
        </body></html>
        """

        rows = parse_board_html(html, source="airportus", source_url="https://airportus.ru/board/")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].direction, "departure")
        self.assertEqual(rows[0].flight_numbers, "HZ-3036 SU-4601")
        self.assertEqual(rows[0].scheduled_raw, "По расписанию 19.06 11:20")
        self.assertEqual(rows[0].actual_raw, "15:00")
        self.assertEqual(rows[0].status_raw, "Задержан до 15:00")
        self.assertEqual(rows[1].direction, "arrival")
        self.assertEqual(rows[1].flight_numbers, "HZ-3037 SU-4602")

    def test_text_block_fallback_parser_extracts_collapsed_board_rows(self) -> None:
        html = """
        <html><body>
        <div>Рейс Авиакомпания Город Время по расписанию Время фактическое Cтатус</div>
        <div>HZ-3036 SU-4601 Южно-Курильск По расписанию  19.06 11:20 09:50 Совмещён с HZ 3034 21 Июн Регистрация: 00:00 - 00:00</div>
        <div>HZ-3034 SU-4666 Южно-Курильск По расписанию  20.06 14:50 09:50 Задержан до 09:50 Регистрация: 00:00 - 00:00</div>
        <div>HZ-3032 SU-4544 Южно-Курильск По расписанию  10:20 10:20 Регистрация: 00:00 - 00:00</div>
        <div>HZ-3036 SU-4601 Южно-Курильск По расписанию  11:30 13:35 Задержан до 13:35 Регистрация: 00:00 - 00:00</div>
        <div>Рейс Авиакомпания Город Время по расписанию Время фактическое Cтатус</div>
        <div>HZ-3033 SU-4545 Южно-Курильск По расписанию  13:45 13:45</div>
        <div>HZ-3037 SU-4602 Южно-Курильск По расписанию  14:55 16:50 Задержан до 16:50</div>
        </body></html>
        """

        rows = parse_board_html(html, source="airportus", source_url="https://airportus.ru/board/")

        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[0].direction, "departure")
        self.assertEqual(rows[0].flight_numbers, "HZ-3036 SU-4601")
        self.assertEqual(rows[0].route, "Южно-Курильск")
        self.assertEqual(rows[0].scheduled_raw, "По расписанию 19.06 11:20")
        self.assertEqual(rows[0].actual_raw, "09:50")
        self.assertEqual(rows[0].status_raw, "Совмещён с HZ 3034 21 Июн")
        self.assertEqual(rows[3].status_raw, "Задержан до 13:35")
        self.assertEqual(rows[4].direction, "arrival")
        self.assertEqual(rows[4].flight_numbers, "HZ-3033 SU-4545")
        self.assertEqual(rows[5].status_raw, "Задержан до 16:50")


if __name__ == "__main__":
    unittest.main()
