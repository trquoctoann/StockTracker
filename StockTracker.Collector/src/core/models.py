import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class DataSource(Enum):
    API = "api"
    LIBRARY = "library"
    WEB_SCRAPING = "web_scraping"


class DataType(Enum):
    COMPANIES = "companies"
    SHAREHOLDERS = "shareholders"
    OFFICERS = "officers"
    SUBSIDIARIES = "subsidiaries"
    AFFILIATES = "affiliates"
    EVENTS = "events"
    NEWS = "news"


@dataclass
class StockData:
    symbol: str
    data_type: DataType
    source: DataSource
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "data_type": self.data_type.value,
            "source": self.source.value,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StockData":
        return cls(
            symbol=data["symbol"],
            data_type=DataType(data["data_type"]),
            source=DataSource(data["source"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
        )


@dataclass
class CollectorResult:
    success: bool
    data: List[StockData] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.success and not self.data:
            self.data = []


@dataclass
class SenderResult:
    success: bool
    sent_count: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitStatus:
    source_id: str
    current_count: int
    limit: int
    period: int
    reset_time: datetime
    is_limited: bool = False

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.current_count)


@dataclass
class CollectorConfig:
    name: str
    source: DataSource
    enabled: bool = True
    rate_limit: Optional[Dict[str, Union[int, float]]] = None
    timeout: int = 30
    retry_attempts: int = 3
    custom_config: Dict[str, Any] = field(default_factory=dict)

    def get_rate_limit(self, key: str, default: Union[int, float] = 0) -> Union[int, float]:
        if not self.rate_limit:
            return default
        return self.rate_limit.get(key, default)
