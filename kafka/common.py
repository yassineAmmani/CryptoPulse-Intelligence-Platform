import os
from typing import Dict


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


def kafka_producer_config(client_id: str) -> Dict[str, str]:
    return {
        "bootstrap.servers": env("KAFKA_BOOTSTRAP_SERVERS", required=True),
        "security.protocol": env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": env("KAFKA_SASL_MECHANISMS", "PLAIN"),
        "sasl.username": env("KAFKA_SASL_USERNAME", required=True),
        "sasl.password": env("KAFKA_SASL_PASSWORD", required=True),
        "client.id": client_id,
    }
