import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

from ..core.base import BaseAPICollector
from ..core.models import (
    CollectorConfig,
    CollectorResult,
    DataSource,
    DataType,
    StockData,
)
from ..utils.async_manager import concurrency_manager
from ..utils.rate_limiter import RateLimitRule, rate_limiter

logger = structlog.get_logger()


class GenericAPICollector(BaseAPICollector):
    def __init__(self, collector_config: CollectorConfig, api_config: Dict[str, Any]):
        """
        Initialize generic API collector.

        Args:
            collector_config: Base collector configuration
            api_config: API-specific configuration including:
                - base_url: Base URL for the API
                - api_key: API key (optional)
                - headers: Additional headers
                - endpoints: Endpoint configurations
                - rate_limit: Rate limiting configuration
        """
        super().__init__(collector_config)
        self.api_config = api_config
        self.base_url = api_config.get("base_url", "")
        self.api_key = api_config.get("api_key", "")
        self.headers = api_config.get("headers", {})
        self.endpoints = api_config.get("endpoints", {})

        # Setup rate limiting
        if "rate_limit" in api_config:
            rate_config = api_config["rate_limit"]
            rule = RateLimitRule(
                limit=rate_config.get("limit", 100),
                period=rate_config.get("period", 60),
                burst_limit=rate_config.get("burst_limit"),
                burst_period=rate_config.get("burst_period"),
            )
            rate_limiter.add_rule(self.config.name, rule, adaptive=rate_config.get("adaptive", False))

    def validate_config(self) -> bool:
        """Validate API collector configuration."""
        if not self.base_url:
            self.logger.error("Base URL is required for API collector")
            return False

        if not self.endpoints:
            self.logger.error("At least one endpoint must be configured")
            return False

        return True

    async def fetch_data(self, symbol: str, data_type: DataType, **kwargs) -> Optional[StockData]:
        """
        Fetch data for a single symbol.

        Args:
            symbol: Stock symbol to fetch data for
            data_type: Type of data to fetch
            **kwargs: Additional parameters for the API call

        Returns:
            StockData object if successful, None otherwise
        """
        if not self.session:
            raise RuntimeError("HTTP session not initialized. Use async context manager.")

        try:
            if not await rate_limiter.is_allowed(self.config.name, symbol):
                await rate_limiter.wait_if_needed(self.config.name, symbol)

            endpoint_config = self.endpoints.get(data_type.value)
            if not endpoint_config:
                raise ValueError(f"No endpoint configured for data type: {data_type.value}")

            url = f"{self.base_url}{endpoint_config['path']}"
            params = self._build_params(symbol, endpoint_config, **kwargs)
            headers = self._build_headers(endpoint_config)

            start_time = time.time()
            async with self.session.get(url, params=params, headers=headers) as response:
                response_time = time.time() - start_time

                rate_limiter.record_response_time(self.config.name, response_time)
                if response.status == 200:
                    data = await response.json()
                    return self._parse_response(symbol, data_type, data, endpoint_config)
                else:
                    error_text = await response.text()
                    self.logger.error("API request failed", symbol=symbol, status=response.status, error=error_text)
                    return None

        except Exception as e:
            self.logger.error("Failed to fetch data", symbol=symbol, error=str(e))
            return None

    def _build_params(self, symbol: str, endpoint_config: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        params = endpoint_config.get("params", {}).copy()

        symbol_param = endpoint_config.get("symbol_param", "symbol")
        params[symbol_param] = symbol

        if self.api_key and endpoint_config.get("requires_api_key", True):
            api_key_param = endpoint_config.get("api_key_param", "apikey")
            params[api_key_param] = self.api_key

        params.update(kwargs)
        return params

    def _build_headers(self, endpoint_config: Dict[str, Any]) -> Dict[str, str]:
        headers = self.headers.copy()
        headers.update(endpoint_config.get("headers", {}))

        if self.api_key and endpoint_config.get("api_key_in_header"):
            header_name = endpoint_config.get("api_key_header", "X-API-Key")
            headers[header_name] = self.api_key

        return headers

    def _parse_response(
        self, symbol: str, data_type: DataType, response_data: Dict[str, Any], endpoint_config: Dict[str, Any]
    ) -> StockData:
        if "parser" in endpoint_config:
            parser_func = endpoint_config["parser"]
            parsed_data = parser_func(response_data)
        else:
            parsed_data = response_data

        return StockData(
            symbol=symbol,
            data_type=data_type,
            source=DataSource.API,
            timestamp=datetime.now(),
            data=parsed_data,
            metadata={
                "api_name": self.config.name,
                "endpoint": endpoint_config.get("path", ""),
                "raw_response_size": len(str(response_data)),
            },
        )


class AlphaVantageCollector(GenericAPICollector):
    def __init__(self, api_key: str):
        api_config = {
            "base_url": "https://www.alphavantage.co/query",
            "api_key": api_key,
            "rate_limit": {"limit": 5, "period": 60, "adaptive": True},
            "endpoints": {
                "price": {
                    "path": "",
                    "params": {"function": "GLOBAL_QUOTE"},
                    "symbol_param": "symbol",
                    "api_key_param": "apikey",
                    "parser": self._parse_price_data,
                },
                "fundamentals": {
                    "path": "",
                    "params": {"function": "OVERVIEW"},
                    "symbol_param": "symbol",
                    "api_key_param": "apikey",
                    "parser": self._parse_fundamentals_data,
                },
            },
        }

        collector_config = CollectorConfig(name="alpha_vantage", source=DataSource.API, timeout=30)

        super().__init__(collector_config, api_config)

    def _parse_price_data(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Alpha Vantage price data."""
        global_quote = response_data.get("Global Quote", {})

        return {
            "price": float(global_quote.get("05. price", 0)),
            "change": float(global_quote.get("09. change", 0)),
            "change_percent": global_quote.get("10. change percent", "0%"),
            "volume": int(global_quote.get("06. volume", 0)),
            "previous_close": float(global_quote.get("08. previous close", 0)),
            "open": float(global_quote.get("02. open", 0)),
            "high": float(global_quote.get("03. high", 0)),
            "low": float(global_quote.get("04. low", 0)),
        }

    def _parse_fundamentals_data(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Alpha Vantage fundamentals data."""
        return {
            "market_cap": response_data.get("MarketCapitalization"),
            "pe_ratio": response_data.get("PERatio"),
            "peg_ratio": response_data.get("PEGRatio"),
            "dividend_yield": response_data.get("DividendYield"),
            "eps": response_data.get("EPS"),
            "book_value": response_data.get("BookValue"),
            "sector": response_data.get("Sector"),
            "industry": response_data.get("Industry"),
            "description": response_data.get("Description"),
        }


class FinnhubCollector(GenericAPICollector):
    """Finnhub API collector for stock data."""

    def __init__(self, api_key: str):
        """Initialize Finnhub collector."""
        api_config = {
            "base_url": "https://finnhub.io/api/v1",
            "api_key": api_key,
            "rate_limit": {"limit": 60, "period": 60, "adaptive": True},  # 60 calls per minute for free tier
            "endpoints": {
                "price": {
                    "path": "/quote",
                    "symbol_param": "symbol",
                    "api_key_param": "token",
                    "parser": self._parse_price_data,
                },
                "news": {
                    "path": "/company-news",
                    "symbol_param": "symbol",
                    "api_key_param": "token",
                    "params": {"from": "2023-01-01", "to": "2023-12-31"},
                    "parser": self._parse_news_data,
                },
            },
        }

        collector_config = CollectorConfig(name="finnhub", source=DataSource.API, timeout=30)

        super().__init__(collector_config, api_config)

    def _parse_price_data(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse Finnhub price data."""
        return {
            "price": response_data.get("c", 0),  # Current price
            "change": response_data.get("d", 0),  # Change
            "change_percent": response_data.get("dp", 0),  # Percent change
            "high": response_data.get("h", 0),  # High price of the day
            "low": response_data.get("l", 0),  # Low price of the day
            "open": response_data.get("o", 0),  # Open price of the day
            "previous_close": response_data.get("pc", 0),  # Previous close price
        }

    def _parse_news_data(self, response_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Parse Finnhub news data."""
        return {
            "articles": [
                {
                    "headline": article.get("headline"),
                    "summary": article.get("summary"),
                    "url": article.get("url"),
                    "datetime": article.get("datetime"),
                    "source": article.get("source"),
                }
                for article in response_data[:10]  # Limit to 10 most recent articles
            ]
        }


class BatchAPICollector:
    """Collector that manages multiple API collectors and coordinates data collection."""

    def __init__(self):
        """Initialize batch API collector."""
        self.collectors: Dict[str, BaseAPICollector] = {}
        self.logger = logger.bind(component="batch_api_collector")

    def add_collector(self, name: str, collector: BaseAPICollector):
        """Add a collector to the batch."""
        self.collectors[name] = collector
        self.logger.info("Added collector", name=name, type=type(collector).__name__)

    async def collect_all(
        self, symbols: List[str], data_types: Optional[List[DataType]] = None, **kwargs
    ) -> Dict[str, CollectorResult]:
        """
        Collect data from all configured collectors.

        Args:
            symbols: List of symbols to collect data for
            data_types: Optional list of data types to collect
            **kwargs: Additional parameters for collectors

        Returns:
            Dictionary mapping collector names to their results
        """
        if not self.collectors:
            self.logger.warning("No collectors configured")
            return {}

        if data_types is None:
            data_types = [DataType.PRICE]

        # Prepare tasks for all collectors
        tasks = {}
        async with concurrency_manager:
            for collector_name, collector in self.collectors.items():
                if not collector.is_enabled():
                    continue

                async with collector:
                    for data_type in data_types:
                        task_id = f"{collector_name}_{data_type.value}"
                        coro = collector.collect(symbols, data_type=data_type, **kwargs)
                        task_id = await concurrency_manager.submit_async_task(
                            coro, task_id=task_id, rate_limit_key=collector_name
                        )
                        tasks[collector_name] = task_id

            # Wait for all tasks to complete
            results = {}
            for collector_name, task_id in tasks.items():
                task_result = await concurrency_manager.wait_for_async_task(task_id, timeout=60)
                if task_result.success:
                    results[collector_name] = task_result.result
                else:
                    self.logger.error("Collector failed", collector=collector_name, error=task_result.error)
                    results[collector_name] = CollectorResult(success=False, error=task_result.error)

        return results
