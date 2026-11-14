
import copy
import unittest

from streaming.event_contract import (
    build_event_id,
    validate_event
)


# ============================================================
# 1. FIXTURE: EVENTO VÁLIDO DE REFERENCIA
# ============================================================

def create_valid_event():

    municipality_code = "76020"
    observation_time = "2025-01-01"
    simulation_id = "SIM001"

    return {
        "schema_version": "1.0",
        "event_id": build_event_id(
            municipality_code,
            observation_time,
            simulation_id
        ),
        "event_type": "daily_weather_report",
        "simulation_id": simulation_id,
        "source": "Open-Meteo Archive",
        "source_mode": "historical_replay",
        "is_simulated": True,
        "municipality_code": municipality_code,
        "municipality": "ALCALA",
        "department_code": "76",
        "department": "VALLE DEL CAUCA",
        "observation_time": observation_time,
        "published_at": "2026-11-12T22:00:00Z",
        "temperature_mean": 20.5,
        "temperature_max": 29.4,
        "temperature_min": 14.2,
        "precipitation_mm": 3.8,
        "relative_humidity": 84.5,
        "et0_mm": 3.2
    }


# ============================================================
# 2. PRUEBAS UNITARIAS DEL CONTRATO
# ============================================================

class TestEventContract(unittest.TestCase):

    def setUp(self):

        self.event = create_valid_event()

    # --------------------------------------------------------
    # 2.1. PRUEBAS ORIGINALES
    # --------------------------------------------------------

    def test_valid_event(self):

        self.assertTrue(
            validate_event(
                self.event,
                kafka_key="76020"
            )
        )

    def test_missing_field(self):

        event = copy.deepcopy(self.event)

        del event["temperature_mean"]

        with self.assertRaisesRegex(
            ValueError,
            "Missing required fields"
        ):
            validate_event(event)

    def test_invalid_municipality_code(self):

        event = copy.deepcopy(self.event)

        event["municipality_code"] = "7602A"

        with self.assertRaisesRegex(
            ValueError,
            "municipality_code must contain 5 digits"
        ):
            validate_event(event)

    def test_negative_precipitation(self):

        event = copy.deepcopy(self.event)

        event["precipitation_mm"] = -2.5

        with self.assertRaisesRegex(
            ValueError,
            "precipitation_mm cannot be negative"
        ):
            validate_event(event)

    def test_invalid_humidity(self):

        event = copy.deepcopy(self.event)

        event["relative_humidity"] = 105

        with self.assertRaisesRegex(
            ValueError,
            "relative_humidity must be between 0 and 100"
        ):
            validate_event(event)

    def test_invalid_temperature_order(self):

        event = copy.deepcopy(self.event)

        event["temperature_min"] = 25.0

        with self.assertRaisesRegex(
            ValueError,
            "Temperature integrity rule failed"
        ):
            validate_event(event)

    def test_invalid_simulation_flag(self):

        event = copy.deepcopy(self.event)

        event["is_simulated"] = False

        with self.assertRaisesRegex(
            ValueError,
            "Historical replay events must be simulated"
        ):
            validate_event(event)

    def test_invalid_kafka_key(self):

        with self.assertRaisesRegex(
            ValueError,
            "Kafka key does not match municipality_code"
        ):
            validate_event(
                self.event,
                kafka_key="76036"
            )

    # --------------------------------------------------------
    # 2.2. PRUEBAS ADICIONALES DE FECHAS
    # --------------------------------------------------------

    def test_invalid_observation_date_format(self):

        event = copy.deepcopy(self.event)

        event["observation_time"] = "2025-1-01"

        with self.assertRaisesRegex(
            ValueError,
            "observation_time must use YYYY-MM-DD format"
        ):
            validate_event(event)

    def test_invalid_observation_date_value(self):

        event = copy.deepcopy(self.event)

        event["observation_time"] = "2025-02-30"

        with self.assertRaisesRegex(
            ValueError,
            "Invalid observation_time"
        ):
            validate_event(event)

    def test_published_at_without_timezone(self):

        event = copy.deepcopy(self.event)

        event["published_at"] = "2026-11-12T22:00:00"

        with self.assertRaisesRegex(
            ValueError,
            "published_at must use ISO 8601 format"
        ):
            validate_event(event)

    def test_published_at_without_iso_separator(self):

        event = copy.deepcopy(self.event)

        event["published_at"] = "2026-11-12 22:00:00Z"

        with self.assertRaisesRegex(
            ValueError,
            "published_at must use ISO 8601 format"
        ):
            validate_event(event)

    # --------------------------------------------------------
    # 2.3. PRUEBAS DE COHERENCIA TEMPORAL
    # --------------------------------------------------------

    def test_published_at_before_observation_date(self):

        event = copy.deepcopy(self.event)

        event["published_at"] = "2024-12-31T23:59:59Z"

        with self.assertRaisesRegex(
            ValueError,
            "published_at cannot be earlier"
        ):
            validate_event(event)

    # --------------------------------------------------------
    # 2.4. PRUEBAS DE INTEGRIDAD NUMÉRICA
    # --------------------------------------------------------

    def test_temperature_nan(self):

        event = copy.deepcopy(self.event)

        event["temperature_mean"] = float("nan")

        with self.assertRaisesRegex(
            ValueError,
            "temperature_mean must be finite"
        ):
            validate_event(event)

    def test_et0_infinite(self):

        event = copy.deepcopy(self.event)

        event["et0_mm"] = float("inf")  

        with self.assertRaisesRegex(
            ValueError,
            "et0_mm must be finite"
        ):
            validate_event(event)

    # --------------------------------------------------------
    # 2.5. PRUEBA DE INTEGRIDAD DEL IDENTIFICADOR
    # --------------------------------------------------------

    def test_invalid_event_id(self):

        event = copy.deepcopy(self.event)

        event["event_id"] = "ODS2-76036-20250101-SIM001"

        with self.assertRaisesRegex(
            ValueError,
            "event_id does not match event data"
        ):
            validate_event(event)


# ============================================================
# 3. EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":
    unittest.main()
