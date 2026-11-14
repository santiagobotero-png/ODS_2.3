
import argparse
import json
import logging
import os
import time

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from streaming.event_contract import (
    SOURCE,
    SOURCE_MODE,
    DEPARTMENT_CODE,
    DEPARTMENT_NAME,
    SCHEMA_VERSION,
    EVENT_TYPE,
    build_event_id,
    validate_event,
)


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_TOPIC = "ods2.weather.daily"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("ods2.producer")


# ============================================================
# 2. CONEXIÓN A POSTGRESQL
# ============================================================

def get_database_engine() -> Engine:
    """
    Construye la conexión al PostgreSQL de negocio
    utilizando las variables del archivo .env.
    """

    required_variables = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_DB_PASSWORD",
    ]

    missing = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing:
        raise EnvironmentError(
            "Faltan variables de PostgreSQL en .env: "
            + ", ".join(missing)
        )

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_DB_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        database=os.getenv("POSTGRES_DB"),
    )

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10},
    )

    # Comprobar conexión antes de iniciar la simulación.
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    logger.info("Conexión a PostgreSQL establecida.")

    return engine


# ============================================================
# 3. EXTRACCIÓN DE REGISTROS METEOROLÓGICOS
# ============================================================

WEATHER_QUERY = text("""
    SELECT
        g.codigo_municipio AS municipality_code,
        g.municipio AS municipality,
        g.codigo_departamento AS department_code,
        g.departamento AS department,

        f.fecha AS observation_time,

        c.temperatura_media AS temperature_mean,
        c.temperatura_maxima AS temperature_max,
        c.temperatura_minima AS temperature_min,
        c.precipitacion AS precipitation_mm,
        c.humedad_relativa_media AS relative_humidity,
        c.evapotranspiracion_et0 AS et0_mm

    FROM weather.fact_clima_diario c

    INNER JOIN comun.dim_geografia g
        ON c.sk_geografia = g.sk_geografia

    INNER JOIN weather.dim_fecha f
        ON c.sk_fecha = f.sk_fecha

    WHERE
        g.codigo_departamento = :department_code
        AND f.fecha >= :start_date
        AND f.fecha < :end_date

    ORDER BY
        f.fecha,
        g.codigo_municipio
""")


def fetch_weather_records(
    engine: Engine,
    start_date: date,
    simulation_days: int,
) -> list[dict]:
    """
    Extrae los registros diarios de Valle del Cauca
    para el rango solicitado.

    No realiza imputaciones ni reemplaza valores nulos.
    """

    end_date = start_date + timedelta(days=simulation_days)

    parameters = {
        "department_code": DEPARTMENT_CODE,
        "start_date": start_date,
        "end_date": end_date,
    }

    with engine.connect() as connection:
        result = connection.execute(
            WEATHER_QUERY,
            parameters,
        )

        records = [
            dict(row)
            for row in result.mappings().all()
        ]

    logger.info(
        "Registros extraídos: %s | Periodo: %s a %s",
        len(records),
        start_date,
        end_date - timedelta(days=1),
    )

    return records


# ============================================================
# 4. CONSTRUCCIÓN DEL EVENTO
# ============================================================

def to_float(value, field_name: str) -> float:
    """
    Convierte valores numéricos de PostgreSQL a float.
    Rechaza valores nulos.
    """

    if value is None:
        raise ValueError(
            f"El campo {field_name} contiene un valor nulo."
        )

    if isinstance(value, bool):
        raise ValueError(
            f"El campo {field_name} no puede ser booleano."
        )

    if isinstance(value, (int, float, Decimal)):
        return float(value)

    raise ValueError(
        f"Tipo numérico no válido para {field_name}: "
        f"{type(value).__name__}"
    )


def build_weather_event(
    row: dict,
    simulation_id: str,
    published_at: str,
) -> dict:
    """
    Construye un evento meteorológico diario
    compatible con event_contract.py.
    """

    municipality_code = str(row["municipality_code"])
    observation_date = row["observation_time"].isoformat()

    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": build_event_id(
            municipality_code,
            observation_date,
            simulation_id,
        ),
        "event_type": EVENT_TYPE,
        "simulation_id": simulation_id,

        "source": SOURCE,
        "source_mode": SOURCE_MODE,
        "is_simulated": True,

        "municipality_code": municipality_code,
        "municipality": row["municipality"],
        "department_code": str(row["department_code"]),
        "department": row["department"],

        "observation_time": observation_date,
        "published_at": published_at,

        "temperature_mean": to_float(
            row["temperature_mean"],
            "temperature_mean",
        ),
        "temperature_max": to_float(
            row["temperature_max"],
            "temperature_max",
        ),
        "temperature_min": to_float(
            row["temperature_min"],
            "temperature_min",
        ),
        "precipitation_mm": to_float(
            row["precipitation_mm"],
            "precipitation_mm",
        ),
        "relative_humidity": to_float(
            row["relative_humidity"],
            "relative_humidity",
        ),
        "et0_mm": to_float(
            row["et0_mm"],
            "et0_mm",
        ),
    }

    return event


# ============================================================
# 5. CONFIGURACIÓN DEL PRODUCTOR KAFKA
# ============================================================

def get_kafka_producer() -> KafkaProducer:
    """
    Crea el productor Kafka con confirmación de entrega.
    """

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,

        key_serializer=lambda key: key.encode("utf-8"),

        value_serializer=lambda event: json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"),

        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
    )

    logger.info(
        "Conexión con Kafka establecida: %s",
        bootstrap_servers,
    )

    return producer


# ============================================================
# 6. PUBLICACIÓN DE UNA JORNADA
# ============================================================

def publish_daily_batch(
    producer: KafkaProducer,
    topic: str,
    daily_records: list[dict],
    simulation_id: str,
) -> tuple[int, int]:
    """
    Publica todos los registros correspondientes
    a una fecha meteorológica.

    Retorna:
        (publicados, rechazados)
    """

    published = 0
    rejected = 0

    # Un único instante de publicación para la jornada.
    published_at = (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )

    for row in daily_records:
        municipality_code = str(row["municipality_code"])

        try:
            event = build_weather_event(
                row=row,
                simulation_id=simulation_id,
                published_at=published_at,
            )

            # Validación del contrato antes de publicar.
            validate_event(
                event,
                kafka_key=municipality_code,
            )

            # Publicación con municipio como clave Kafka.
            future = producer.send(
                topic,
                key=municipality_code,
                value=event,
            )

            # Esperar confirmación del broker.
            metadata = future.get(timeout=30)

            published += 1

            logger.debug(
                "Evento confirmado | municipio=%s | "
                "partición=%s | offset=%s",
                municipality_code,
                metadata.partition,
                metadata.offset,
            )

        except Exception as error:
            rejected += 1

            logger.exception(
                "Evento rechazado | municipio=%s | motivo=%s",
                municipality_code,
                error,
            )

    producer.flush(timeout=30)

    return published, rejected


# ============================================================
# 7. SIMULACIÓN DE REPRODUCCIÓN HISTÓRICA
# ============================================================

def run_simulation(
    start_date: date,
    simulation_days: int,
    interval_seconds: float,
) -> None:
    """
    Reproduce jornadas meteorológicas históricas.

    Cada jornada publica un lote de eventos y espera
    el intervalo indicado antes de continuar.
    """

    if simulation_days < 1:
        raise ValueError(
            "simulation_days debe ser mayor o igual a 1."
        )

    if interval_seconds < 0:
        raise ValueError(
            "interval_seconds no puede ser negativo."
        )

    engine = None
    producer = None

    total_published = 0
    total_rejected = 0

    simulation_id = (
        datetime.now(timezone.utc)
        .strftime("SIM_%Y%m%dT%H%M%SZ")
    )

    topic = os.getenv(
        "KAFKA_TOPIC",
        DEFAULT_TOPIC,
    )

    try:
        engine = get_database_engine()

        records = fetch_weather_records(
            engine=engine,
            start_date=start_date,
            simulation_days=simulation_days,
        )

        # Agrupar registros por fecha meteorológica.
        records_by_date = defaultdict(list)

        for row in records:
            records_by_date[row["observation_time"]].append(row)

        producer = get_kafka_producer()

        logger.info("=" * 60)
        logger.info("INICIO DE REPRODUCCIÓN HISTÓRICA")
        logger.info("Simulation ID: %s", simulation_id)
        logger.info("Topic: %s", topic)
        logger.info("Jornadas solicitadas: %s", simulation_days)
        logger.info("Intervalo: %s segundos", interval_seconds)
        logger.info("=" * 60)

        for day_number in range(simulation_days):

            current_date = start_date + timedelta(
                days=day_number
            )

            daily_records = records_by_date.get(
                current_date,
                [],
            )

            logger.info(
                "Jornada %s/%s | Fecha: %s | Registros: %s",
                day_number + 1,
                simulation_days,
                current_date,
                len(daily_records),
            )

            published, rejected = publish_daily_batch(
                producer=producer,
                topic=topic,
                daily_records=daily_records,
                simulation_id=simulation_id,
            )

            total_published += published
            total_rejected += rejected

            logger.info(
                "Resultado jornada | publicados=%s | rechazados=%s",
                published,
                rejected,
            )

            # Esperar únicamente entre jornadas.
            if day_number < simulation_days - 1:
                logger.info(
                    "Esperando %s segundos para la siguiente jornada...",
                    interval_seconds,
                )

                time.sleep(interval_seconds)

        logger.info("=" * 60)
        logger.info("RESUMEN DE LA SIMULACIÓN")
        logger.info("Simulation ID: %s", simulation_id)
        logger.info("Eventos publicados: %s", total_published)
        logger.info("Eventos rechazados: %s", total_rejected)
        logger.info("=" * 60)

    finally:
        if producer is not None:
            producer.flush(timeout=30)
            producer.close(timeout=30)

        if engine is not None:
            engine.dispose()

        logger.info("Recursos de conexión cerrados.")


# ============================================================
# 8. INTERFAZ DE EJECUCIÓN
# ============================================================

def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Productor Kafka para reproducción histórica "
            "de eventos meteorológicos ODS2."
        )
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Fecha inicial en formato YYYY-MM-DD.",
    )

    parser.add_argument(
        "--simulation-days",
        type=int,
        default=1,
        help="Cantidad de jornadas consecutivas.",
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5,
        help="Tiempo de espera entre jornadas.",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    try:
        start_date = date.fromisoformat(args.start_date)

        run_simulation(
            start_date=start_date,
            simulation_days=args.simulation_days,
            interval_seconds=args.interval_seconds,
        )

    except Exception:
        logger.exception(
            "La ejecución del productor terminó con errores."
        )
        raise


if __name__ == "__main__":
    main()
