
import json
import logging
import os
from kafka.structs import OffsetAndMetadata, TopicPartition
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from streaming.event_contract import validate_event


# ============================================================
# 1. CONFIGURACIÓN
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_TOPIC = "ods2.weather.daily"
DEFAULT_GROUP_ID = "ods2-weather-consumer"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("ods2.consumer")


# ============================================================
# 2. CONEXIÓN A POSTGRESQL
# ============================================================

def get_database_engine() -> Engine:
    """Construye y comprueba la conexión a PostgreSQL."""

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

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    logger.info("Conexión a PostgreSQL establecida.")

    return engine


# ============================================================
# 3. CONFIGURACIÓN DEL CONSUMIDOR KAFKA
# ============================================================

def get_kafka_consumer() -> KafkaConsumer:
    """Crea un consumidor Kafka con confirmación manual."""

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)

    group_id = os.getenv(
        "KAFKA_GROUP_ID",
        DEFAULT_GROUP_ID,
    )

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,

        # Se reciben los bytes originales.
        # La deserialización se realiza manualmente.
        enable_auto_commit=False,

        # Permite recuperar mensajes desde el inicio
        # si el grupo no tiene offsets confirmados.
        auto_offset_reset="earliest",

        # Evita confirmar offsets automáticamente.
        value_deserializer=None,
        key_deserializer=None,


    )

    consumer.subscribe([topic])

    logger.info(
        "Consumidor Kafka configurado | broker=%s | topic=%s | group=%s",
        bootstrap_servers,
        topic,
        group_id,
    )

    return consumer


# ============================================================
# 4. CONSULTAS DE DIMENSIONES
# ============================================================

GET_GEOGRAPHY = text("""
    SELECT sk_geografia
    FROM comun.dim_geografia
    WHERE codigo_municipio = :municipality_code
""")

GET_DATE = text("""
    SELECT sk_fecha
    FROM weather.dim_fecha
    WHERE fecha = :observation_date
""")


# ============================================================
# 5. REGISTRO RAW
# ============================================================

INSERT_RAW_EVENT = text("""
    INSERT INTO streaming.raw_events (
        event_id,
        kafka_topic,
        kafka_key,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        raw_payload,
        parsed_payload,
        processing_status
    )
    VALUES (
        :event_id,
        :kafka_topic,
        :kafka_key,
        :kafka_partition,
        :kafka_offset,
        :kafka_timestamp,
        :raw_payload,
        CAST(:parsed_payload AS JSONB),
        'received'
    )
    ON CONFLICT (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
    DO NOTHING
    RETURNING raw_event_id
""")


UPDATE_RAW_EVENT = text("""
    UPDATE streaming.raw_events
    SET
        processing_status = :processing_status,
        error_message = :error_message,
        processed_at = CURRENT_TIMESTAMP
    WHERE raw_event_id = :raw_event_id
""")


# ============================================================
# 6. INSERCIÓN EN LA TABLA DE HECHOS
# ============================================================

INSERT_WEATHER_FACT = text("""
    INSERT INTO streaming.fact_weather_daily (
        raw_event_id,
        event_id,
        simulation_id,
        sk_geografia,
        sk_fecha,
        is_simulated,
        temperature_mean,
        temperature_max,
        temperature_min,
        precipitation_mm,
        relative_humidity,
        et0_mm,
        published_at
    )
    VALUES (
        :raw_event_id,
        :event_id,
        :simulation_id,
        :sk_geografia,
        :sk_fecha,
        :is_simulated,
        :temperature_mean,
        :temperature_max,
        :temperature_min,
        :precipitation_mm,
        :relative_humidity,
        :et0_mm,
        :published_at
    )
""")


# ============================================================
# 7. PROCESAMIENTO DE UN MENSAJE
# ============================================================

def process_message(
    engine: Engine,
    message,
) -> str:
    """
    Procesa un mensaje Kafka y persiste su resultado.

    Retorna:
        processed
        rejected
        duplicate

    Los errores de infraestructura se propagan para
    impedir la confirmación del offset.
    """

    # --------------------------------------------------------
    # 7.1. RECUPERAR EL CONTENIDO ORIGINAL
    # --------------------------------------------------------

    raw_bytes = message.value

    try:
        raw_payload = raw_bytes.decode("utf-8")

    except UnicodeDecodeError:
        raw_payload = raw_bytes.decode(
            "utf-8",
            errors="replace",
        )

    parsed_payload = None
    event = None
    parse_error = None

    try:
        parsed_payload = json.loads(raw_payload)

        if isinstance(parsed_payload, dict):
            event = parsed_payload

    except (json.JSONDecodeError, UnicodeError) as error:
        parse_error = f"JSON inválido: {error}"

    # --------------------------------------------------------
    # 7.2. RECUPERAR LA CLAVE KAFKA
    # --------------------------------------------------------

    kafka_key = message.key

    if isinstance(kafka_key, bytes):
        try:
            kafka_key_text = kafka_key.decode("utf-8")
        except UnicodeDecodeError:
            kafka_key_text = kafka_key.decode(
                "utf-8",
                errors="replace",
            )
    else:
        kafka_key_text = kafka_key

    event_id = None

    if isinstance(event, dict):
        event_id = event.get("event_id")

    kafka_timestamp = None

    if message.timestamp is not None:
        kafka_timestamp = datetime.fromtimestamp(
            message.timestamp / 1000,
            tz=timezone.utc,
        )

    # --------------------------------------------------------
    # 7.3. INICIAR TRANSACCIÓN POSTGRESQL
    # --------------------------------------------------------

    with engine.begin() as connection:

        raw_result = connection.execute(
            INSERT_RAW_EVENT,
            {
                "event_id": (
                    event_id
                    if isinstance(event_id, str)
                    else None
                ),
                "kafka_topic": message.topic,
                "kafka_key": kafka_key_text,
                "kafka_partition": message.partition,
                "kafka_offset": message.offset,
                "kafka_timestamp": kafka_timestamp,
                "raw_payload": raw_payload,
                "parsed_payload": (
                    json.dumps(
                        parsed_payload,
                        ensure_ascii=False,
                        allow_nan=False,
                    )
                    if parsed_payload is not None
                    else None
                ),
            },
        )

        raw_event_id = raw_result.scalar_one_or_none()

        # ----------------------------------------------------
        # 7.4. CONTROL DE DUPLICADOS
        # ----------------------------------------------------

        if raw_event_id is None:
            logger.warning(
                "Mensaje ya registrado | topic=%s | "
                "partition=%s | offset=%s",
                message.topic,
                message.partition,
                message.offset,
            )

            return "duplicate"

        # ----------------------------------------------------
        # 7.5. VALIDACIÓN DEL EVENTO
        # ----------------------------------------------------

        try:

            if parse_error:
                raise ValueError(parse_error)

            if event is None:
                raise ValueError(
                    "El contenido JSON debe ser un objeto."
                )

            validate_event(
                event,
                kafka_key=kafka_key,
            )

            # ------------------------------------------------
            # 7.6. RESOLUCIÓN DE DIMENSIONES
            # ------------------------------------------------

            geography_result = connection.execute(
                GET_GEOGRAPHY,
                {
                    "municipality_code": event[
                        "municipality_code"
                    ]
                },
            )

            sk_geografia = geography_result.scalar_one_or_none()

            if sk_geografia is None:
                raise ValueError(
                    "Municipio no encontrado en "
                    "comun.dim_geografia."
                )

            observation_date = date.fromisoformat(
                event["observation_time"]
            )

            date_result = connection.execute(
                GET_DATE,
                {
                    "observation_date": observation_date
                },
            )

            sk_fecha = date_result.scalar_one_or_none()

            if sk_fecha is None:
                raise ValueError(
                    "Fecha no encontrada en "
                    "weather.dim_fecha."
                )

            # ------------------------------------------------
            # 7.7. INSERTAR HECHO METEOROLÓGICO
            # ------------------------------------------------

            published_at = datetime.fromisoformat(
                event["published_at"].replace(
                    "Z",
                    "+00:00",
                )
            )

            connection.execute(
                INSERT_WEATHER_FACT,
                {
                    "raw_event_id": raw_event_id,
                    "event_id": event["event_id"],
                    "simulation_id": event["simulation_id"],
                    "sk_geografia": sk_geografia,
                    "sk_fecha": sk_fecha,
                    "is_simulated": event["is_simulated"],
                    "temperature_mean": event["temperature_mean"],
                    "temperature_max": event["temperature_max"],
                    "temperature_min": event["temperature_min"],
                    "precipitation_mm": event["precipitation_mm"],
                    "relative_humidity": event["relative_humidity"],
                    "et0_mm": event["et0_mm"],
                    "published_at": published_at,
                },
            )

            # ------------------------------------------------
            # 7.8. MARCAR RAW COMO PROCESADO
            # ------------------------------------------------

            connection.execute(
                UPDATE_RAW_EVENT,
                {
                    "raw_event_id": raw_event_id,
                    "processing_status": "processed",
                    "error_message": None,
                },
            )

            return "processed"

        except (ValueError, TypeError, KeyError) as error:

            # ------------------------------------------------
            # 7.9. REGISTRAR EVENTO RECHAZADO
            # ------------------------------------------------

            connection.execute(
                UPDATE_RAW_EVENT,
                {
                    "raw_event_id": raw_event_id,
                    "processing_status": "rejected",
                    "error_message": str(error)[:4000],
                },
            )

            logger.warning(
                "Evento rechazado | event_id=%s | motivo=%s",
                event_id,
                error,
            )

            return "rejected"


# ============================================================
# 8. EJECUCIÓN DEL CONSUMIDOR
# ============================================================

def run_consumer() -> None:
    """Ejecuta el consumidor y procesa mensajes continuamente."""

    engine = None
    consumer = None

    total_processed = 0
    total_rejected = 0
    total_duplicates = 0

    try:

        engine = get_database_engine()
        consumer = get_kafka_consumer()

        logger.info("=" * 60)
        logger.info("INICIO DEL CONSUMIDOR ODS2")
        logger.info("=" * 60)

        for message in consumer:

            try:

                result = process_message(
                    engine=engine,
                    message=message,
                )

                # Confirmar únicamente después de que
                # process_message haya completado la transacción.
                consumer.commit(
                    offsets={
                        TopicPartition(
                            message.topic,
                            message.partition,
                        ): OffsetAndMetadata(
                            message.offset + 1,
                            "",
                            -1,
                        )
                    }
                )

                if result == "processed":
                    total_processed += 1

                elif result == "rejected":
                    total_rejected += 1

                elif result == "duplicate":
                    total_duplicates += 1

                logger.info(
                    "Mensaje procesado | resultado=%s | "
                    "partición=%s | offset=%s",
                    result,
                    message.partition,
                    message.offset,
                )

            except Exception:

                logger.exception(
                    "Error de procesamiento o persistencia. "
                    "No se confirmó el offset."
                )

                # Detener el consumidor para evitar avanzar
                # a mensajes posteriores de forma insegura.
                raise

    except KeyboardInterrupt:

        logger.info(
            "Interrupción solicitada por el usuario."
        )

    finally:

        if consumer is not None:
            consumer.close()

        if engine is not None:
            engine.dispose()

        logger.info("=" * 60)
        logger.info("RESUMEN DEL CONSUMIDOR")
        logger.info("Eventos procesados: %s", total_processed)
        logger.info("Eventos rechazados: %s", total_rejected)
        logger.info("Duplicados detectados: %s", total_duplicates)
        logger.info("=" * 60)

        logger.info("Recursos de conexión cerrados.")


# ============================================================
# 9. PUNTO DE ENTRADA
# ============================================================

def main():
    run_consumer()


if __name__ == "__main__":
    main()
