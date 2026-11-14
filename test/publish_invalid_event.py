import json
from datetime import datetime, timezone

from kafka import KafkaProducer

from streaming.event_contract import build_event_id


def main():
    bootstrap_servers = "localhost:9092"
    topic = "ods2.weather.daily"

    now = datetime.now(timezone.utc)
    simulation_id = now.strftime("SIM_%Y%m%dT%H%M%SZ")

    municipality_code = "76001"
    observation_time = "2019-01-01"

    event = {
        "schema_version": "1.0",
        "event_id": build_event_id(
            municipality_code,
            observation_time,
            simulation_id,
        ),
        "event_type": "daily_weather_report",
        "simulation_id": simulation_id,
        "source": "Open-Meteo Archive",
        "source_mode": "historical_replay",
        "is_simulated": True,
        "municipality_code": municipality_code,
        "municipality": "CALI",
        "department_code": "76",
        "department": "VALLE DEL CAUCA",
        "observation_time": observation_time,
        "published_at": now.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "temperature_mean": 24.0,
        "temperature_max": 30.0,
        "temperature_min": 20.0,
        "precipitation_mm": 0.0,
        "relative_humidity": 150.0,
        "et0_mm": 4.0,
    }

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        acks="all",
    )

    try:
        future = producer.send(
            topic,
            key=municipality_code,
            value=event,
        )

        metadata = future.get(timeout=10)

        print("Evento de prueba publicado.")
        print(f"Topic: {metadata.topic}")
        print(f"Partición: {metadata.partition}")
        print(f"Offset: {metadata.offset}")
        print(f"Event ID: {event['event_id']}")
        print(f"Humedad inválida: {event['relative_humidity']}")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()