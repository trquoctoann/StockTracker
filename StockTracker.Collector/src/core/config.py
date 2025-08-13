import os
from dataclasses import dataclass, field
from typing import Dict, Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class RateLimitConfig:
    limit: int = field(default_factory=lambda: int(os.getenv("DEFAULT_RATE_LIMIT", "3000")))
    period: int = field(default_factory=lambda: int(os.getenv("DEFAULT_RATE_PERIOD", "60")))
    burst_limit: Optional[int] = None


@dataclass
class ConcurrencyConfig:
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "12")))
    max_async_connections: int = field(
        default_factory=lambda: int(os.getenv("MAX_ASYNC_CONNECTIONS", "120"))
    )


@dataclass
class DatabaseConfig:
    url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", ""))


@dataclass
class RedisConfig:
    url: str = field(default_factory=lambda: os.getenv("REDIS_URL", ""))


@dataclass
class RabbitMQConfig:
    url: str = field(default_factory=lambda: os.getenv("RABBITMQ_URL", ""))


@dataclass
class SeleniumConfig:
    headless: bool = field(
        default_factory=lambda: os.getenv("SELENIUM_HEADLESS", "true").lower() == "true"
    )
    timeout: int = field(default_factory=lambda: int(os.getenv("SELENIUM_TIMEOUT", "30")))


@dataclass
class LoggingConfig:
    level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    format: str = field(default_factory=lambda: os.getenv("LOG_FORMAT", "json"))


@dataclass
class AppConfig:
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)
    selenium: SeleniumConfig = field(default_factory=SeleniumConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    api_keys: Dict[str, str] = field(
        default_factory=lambda: {
            "alpha_vantage": os.getenv("ALPHA_VANTAGE_API_KEY", ""),
            "finnhub": os.getenv("FINNHUB_API_KEY", ""),
        }
    )

    def get_api_key(self, service: str) -> Optional[str]:
        return self.api_keys.get(service)


config = AppConfig()
