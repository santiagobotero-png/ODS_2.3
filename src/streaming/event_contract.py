
import math
import re

from datetime import date, datetime, time, timezone


# ============================================================
# 1. VERSION Y CONFIGURACIÓN DEL CONTRATO
# ============================================================

SCHEMA_VERSION = "1.0"

EVENT_TYPE = "daily_weather_report"

SOURCE = "Open-Meteo Archive"

SOURCE_MODE = "historical_replay"

DEPARTMENT_CODE = "76"

DEPARTMENT_NAME = "VALLE DEL CAUCA"


# ============================================================
# 2. CAMPOS OBLIGATORIOS
# ============================================================

REQUIRED_FIELDS = {
    "schema_version",
    "event_id",
    "event_type",
    "simulation_id",
    "source",
    "source_mode",
    "is_simulated",
    "municipality_code",
    "municipality",
    "department_code",
    "department",
    "observation_time",
    "published_at",
    "temperature_mean",
    "temperature_max",
    "temperature_min",
    "precipitation_mm",
    "relative_humidity",
    "et0_mm"
}


# ============================================================
# 3. CAMPOS NUMÉRICOS
# ============================================================

NUMERIC_FIELDS = {
    "temperature_mean",
    "temperature_max",
    "temperature_min",
    "precipitation_mm",
    "relative_humidity",
    "et0_mm"
}


# ============================================================
# 4. EXPRESIONES REGULARES
# ============================================================

DATE_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}$"
)

DATETIME_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T"
    r"\d{2}:\d{2}:\d{2}"
    r"(?:\.\d+)?"
    r"(?:Z|[+-]\d{2}:\d{2})$"
)

MUNICIPALITY_CODE_PATTERN = re.compile(
    r"^\d{5}$"
)

SIMULATION_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9_-]+$"
)


# ============================================================
# 5. FUNCIONES AUXILIARES
# ============================================================

def validate_string(value, field):

    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")

    if not value.strip():
        raise ValueError(f"{field} cannot be empty")


def validate_numeric(value, field):

    # bool es una subclase de int en Python.
    # Se rechaza explícitamente para evitar True = 1 y False = 0.

    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")

    if not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")

    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")


def validate_observation_time(value):

    if not DATE_PATTERN.fullmatch(value):
        raise ValueError(
            "observation_time must use YYYY-MM-DD format"
        )

    try:
        return date.fromisoformat(value)

    except ValueError:
        raise ValueError("Invalid observation_time")


def validate_published_at(value):

    if not DATETIME_PATTERN.fullmatch(value):
        raise ValueError(
            "published_at must use ISO 8601 format "
            "with timezone information"
        )

    normalized_value = value.replace("Z", "+00:00")

    try:
        timestamp = datetime.fromisoformat(
            normalized_value
        )

    except ValueError:
        raise ValueError("Invalid published_at")

    if timestamp.tzinfo is None:
        raise ValueError(
            "published_at must include timezone information"
        )

    return timestamp.astimezone(timezone.utc)


def validate_temporal_consistency(
    observation_date,
    published_timestamp
):

    # La publicación debe ocurrir después del inicio
    # de la fecha meteorológica observada.

    observation_start = datetime.combine(
        observation_date,
        time.min,
        tzinfo=timezone.utc
    )

    if published_timestamp < observation_start:
        raise ValueError(
            "published_at cannot be earlier than "
            "the observation date"
        )


def build_event_id(
    municipality_code,
    observation_time,
    simulation_id
):

    return (
        f"ODS2-{municipality_code}-"
        f"{observation_time.replace('-', '')}-"
        f"{simulation_id}"
    )


# ============================================================
# 6. VALIDACIÓN PRINCIPAL
# ============================================================

def validate_event(event, kafka_key=None):

    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")

    # --------------------------------------------------------
    # 6.1. ESTRUCTURA
    # --------------------------------------------------------

    missing_fields = REQUIRED_FIELDS - set(event.keys())

    if missing_fields:
        raise ValueError(
            f"Missing required fields: {sorted(missing_fields)}"
        )

    extra_fields = set(event.keys()) - REQUIRED_FIELDS

    if extra_fields:
        raise ValueError(
            f"Unexpected fields: {sorted(extra_fields)}"
        )

    # --------------------------------------------------------
    # 6.2. TIPOS Y CADENAS
    # --------------------------------------------------------

    string_fields = REQUIRED_FIELDS - NUMERIC_FIELDS - {
        "is_simulated"
    }

    for field in string_fields:
        validate_string(event[field], field)

    if not isinstance(event["is_simulated"], bool):
        raise ValueError("is_simulated must be boolean")

    # --------------------------------------------------------
    # 6.3. VERSION Y TIPO DE EVENTO
    # --------------------------------------------------------

    if event["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Unsupported schema_version")

    if event["event_type"] != EVENT_TYPE:
        raise ValueError("Invalid event_type")

    # --------------------------------------------------------
    # 6.4. SIMULACIÓN Y PROCEDENCIA
    # --------------------------------------------------------

    if event["source"] != SOURCE:
        raise ValueError("Invalid source")

    if event["source_mode"] != SOURCE_MODE:
        raise ValueError("Invalid source_mode")

    if event["is_simulated"] is not True:
        raise ValueError(
            "Historical replay events must be simulated"
        )

    if not SIMULATION_ID_PATTERN.fullmatch(
        event["simulation_id"]
    ):
        raise ValueError("Invalid simulation_id")

    # --------------------------------------------------------
    # 6.5. IDENTIFICACIÓN GEOGRÁFICA
    # --------------------------------------------------------

    municipality_code = event["municipality_code"]

    if not MUNICIPALITY_CODE_PATTERN.fullmatch(
        municipality_code
    ):
        raise ValueError(
            "municipality_code must contain 5 digits"
        )

    if event["department_code"] != DEPARTMENT_CODE:
        raise ValueError("Invalid department_code")

    if event["department"] != DEPARTMENT_NAME:
        raise ValueError("Invalid department")

    # --------------------------------------------------------
    # 6.6. VALIDACIÓN ESTRICTA DE FECHAS
    # --------------------------------------------------------

    observation_date = validate_observation_time(
        event["observation_time"]
    )

    published_timestamp = validate_published_at(
        event["published_at"]
    )

    # --------------------------------------------------------
    # 6.7. COHERENCIA TEMPORAL
    # --------------------------------------------------------

    validate_temporal_consistency(
        observation_date,
        published_timestamp
    )

    # --------------------------------------------------------
    # 6.8. IDENTIFICADOR ÚNICO DEL EVENTO
    # --------------------------------------------------------

    expected_event_id = build_event_id(
        municipality_code,
        event["observation_time"],
        event["simulation_id"]
    )

    if event["event_id"] != expected_event_id:
        raise ValueError(
            "event_id does not match event data"
        )

    # --------------------------------------------------------
    # 6.9. VALIDACIÓN DE VARIABLES METEOROLÓGICAS
    # --------------------------------------------------------

    for field in NUMERIC_FIELDS:
        validate_numeric(event[field], field)

    if event["precipitation_mm"] < 0:
        raise ValueError(
            "precipitation_mm cannot be negative"
        )

    if event["et0_mm"] < 0:
        raise ValueError(
            "et0_mm cannot be negative"
        )

    if not 0 <= event["relative_humidity"] <= 100:
        raise ValueError(
            "relative_humidity must be between 0 and 100"
        )

    if not (
        event["temperature_min"]
        <= event["temperature_mean"]
        <= event["temperature_max"]
    ):
        raise ValueError(
            "Temperature integrity rule failed"
        )

    # --------------------------------------------------------
    # 6.10. VALIDACIÓN DE CLAVE KAFKA
    # --------------------------------------------------------

    if kafka_key is not None:

        if isinstance(kafka_key, bytes):

            try:
                kafka_key = kafka_key.decode("utf-8")

            except UnicodeDecodeError:
                raise ValueError(
                    "Kafka key must contain valid UTF-8"
                )

        if not isinstance(kafka_key, str):
            raise ValueError(
                "Kafka key must be a string or bytes"
            )

        if kafka_key != municipality_code:
            raise ValueError(
                "Kafka key does not match municipality_code"
            )

    return True
