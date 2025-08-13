"""Python library-based data collectors with threading support."""

import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import structlog

from ..core.base import BaseLibraryCollector
from ..core.models import (
    CollectorConfig,
    CollectorResult,
    DataSource,
    DataType,
    StockData,
)
from ..utils.async_manager import concurrency_manager

logger = structlog.get_logger()


class VnStockCollector(BaseLibraryCollector):
    def __init__(self):
        try:
            self.vn = vn
        except ImportError:
            raise ImportError("yfinance library is required. Install with: pip install yfinance")

        collector_config = CollectorConfig(
            name="yfinance", source=DataSource.LIBRARY, timeout=30, retry_attempts=3
        )

        super().__init__(collector_config)
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.Lock()
        self._cache_ttl = 300  # 5 minutes cache TTL

    def validate_config(self) -> bool:
        """Validate yfinance collector configuration."""
        try:
            # Test basic functionality
            test_ticker = self.yf.Ticker("AAPL")
            return True
        except Exception as e:
            self.logger.error("yfinance validation failed", error=str(e))
            return False

    def fetch_data_sync(
        self, symbol: str, data_type: DataType = DataType.PRICE, period: str = "1d", **kwargs
    ) -> Optional[StockData]:
        """
        Fetch data for a single symbol synchronously.

        Args:
            symbol: Stock symbol to fetch
            data_type: Type of data to fetch
            period: Period for historical data (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            **kwargs: Additional parameters

        Returns:
            StockData object if successful, None otherwise
        """
        try:
            # Check cache first
            cache_key = f"{symbol}_{data_type.value}_{period}"
            cached_data = self._get_cached_data(cache_key)
            if cached_data:
                return cached_data

            ticker = self.yf.Ticker(symbol)

            if data_type == DataType.PRICE:
                data = self._fetch_price_data(ticker, period)
            elif data_type == DataType.FUNDAMENTALS:
                data = self._fetch_fundamentals_data(ticker)
            elif data_type == DataType.TECHNICAL_INDICATORS:
                data = self._fetch_technical_indicators(ticker, period)
            elif data_type == DataType.NEWS:
                data = self._fetch_news_data(ticker)
            else:
                self.logger.warning("Unsupported data type", data_type=data_type.value)
                return None

            if data is None:
                return None

            stock_data = StockData(
                symbol=symbol,
                data_type=data_type,
                source=DataSource.LIBRARY,
                timestamp=datetime.now(),
                data=data,
                metadata={"library": "yfinance", "period": period, "cache_key": cache_key},
            )

            # Cache the result
            self._cache_data(cache_key, stock_data)

            return stock_data

        except Exception as e:
            self.logger.error("Failed to fetch data", symbol=symbol, error=str(e))
            return None

    def _fetch_price_data(self, ticker, period: str) -> Optional[Dict[str, Any]]:
        """Fetch price data using yfinance."""
        try:
            # Get current price info
            info = ticker.info

            # Get historical data
            hist = ticker.history(period=period)

            if hist.empty:
                return None

            latest = hist.iloc[-1]

            return {
                "current_price": info.get("currentPrice", latest.get("Close", 0)),
                "previous_close": info.get("previousClose", 0),
                "open": latest.get("Open", 0),
                "high": latest.get("High", 0),
                "low": latest.get("Low", 0),
                "volume": latest.get("Volume", 0),
                "market_cap": info.get("marketCap"),
                "change": info.get("currentPrice", 0) - info.get("previousClose", 0),
                "change_percent": (
                    (info.get("currentPrice", 0) - info.get("previousClose", 0))
                    / info.get("previousClose", 1)
                )
                * 100,
                "historical_data": {
                    "dates": hist.index.strftime("%Y-%m-%d").tolist(),
                    "prices": hist["Close"].tolist(),
                    "volumes": hist["Volume"].tolist(),
                },
            }

        except Exception as e:
            self.logger.error("Failed to fetch price data", error=str(e))
            return None

    def _fetch_fundamentals_data(self, ticker) -> Optional[Dict[str, Any]]:
        """Fetch fundamental data using yfinance."""
        try:
            info = ticker.info

            return {
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "peg_ratio": info.get("pegRatio"),
                "price_to_book": info.get("priceToBook"),
                "price_to_sales": info.get("priceToSalesTrailing12Months"),
                "enterprise_to_revenue": info.get("enterpriseToRevenue"),
                "enterprise_to_ebitda": info.get("enterpriseToEbitda"),
                "profit_margins": info.get("profitMargins"),
                "operating_margins": info.get("operatingMargins"),
                "return_on_assets": info.get("returnOnAssets"),
                "return_on_equity": info.get("returnOnEquity"),
                "revenue": info.get("totalRevenue"),
                "revenue_per_share": info.get("revenuePerShare"),
                "quarterly_revenue_growth": info.get("quarterlyRevenueGrowth"),
                "gross_profits": info.get("grossProfits"),
                "ebitda": info.get("ebitda"),
                "net_income": info.get("netIncomeToCommon"),
                "diluted_eps": info.get("trailingEps"),
                "quarterly_earnings_growth": info.get("quarterlyEarningsGrowth"),
                "total_cash": info.get("totalCash"),
                "total_cash_per_share": info.get("totalCashPerShare"),
                "total_debt": info.get("totalDebt"),
                "current_ratio": info.get("currentRatio"),
                "book_value": info.get("bookValue"),
                "operating_cash_flow": info.get("operatingCashflow"),
                "levered_free_cash_flow": info.get("freeCashflow"),
                "beta": info.get("beta"),
                "52_week_high": info.get("fiftyTwoWeekHigh"),
                "52_week_low": info.get("fiftyTwoWeekLow"),
                "50_day_average": info.get("fiftyDayAverage"),
                "200_day_average": info.get("twoHundredDayAverage"),
                "dividend_rate": info.get("dividendRate"),
                "dividend_yield": info.get("dividendYield"),
                "payout_ratio": info.get("payoutRatio"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "full_time_employees": info.get("fullTimeEmployees"),
                "business_summary": info.get("longBusinessSummary"),
            }

        except Exception as e:
            self.logger.error("Failed to fetch fundamentals data", error=str(e))
            return None

    def _fetch_technical_indicators(self, ticker, period: str) -> Optional[Dict[str, Any]]:
        """Fetch and calculate technical indicators."""
        try:
            hist = ticker.history(period=period)

            if hist.empty or len(hist) < 20:
                return None

            # Calculate simple moving averages
            hist["SMA_10"] = hist["Close"].rolling(window=10).mean()
            hist["SMA_20"] = hist["Close"].rolling(window=20).mean()
            hist["SMA_50"] = hist["Close"].rolling(window=50).mean()

            # Calculate RSI
            delta = hist["Close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            hist["RSI"] = 100 - (100 / (1 + rs))

            # Calculate Bollinger Bands
            hist["BB_Middle"] = hist["Close"].rolling(window=20).mean()
            bb_std = hist["Close"].rolling(window=20).std()
            hist["BB_Upper"] = hist["BB_Middle"] + (bb_std * 2)
            hist["BB_Lower"] = hist["BB_Middle"] - (bb_std * 2)

            # Calculate MACD
            exp1 = hist["Close"].ewm(span=12).mean()
            exp2 = hist["Close"].ewm(span=26).mean()
            hist["MACD"] = exp1 - exp2
            hist["MACD_Signal"] = hist["MACD"].ewm(span=9).mean()
            hist["MACD_Histogram"] = hist["MACD"] - hist["MACD_Signal"]

            # Get latest values
            latest = hist.iloc[-1]

            return {
                "sma_10": latest.get("SMA_10"),
                "sma_20": latest.get("SMA_20"),
                "sma_50": latest.get("SMA_50"),
                "rsi": latest.get("RSI"),
                "bollinger_upper": latest.get("BB_Upper"),
                "bollinger_middle": latest.get("BB_Middle"),
                "bollinger_lower": latest.get("BB_Lower"),
                "macd": latest.get("MACD"),
                "macd_signal": latest.get("MACD_Signal"),
                "macd_histogram": latest.get("MACD_Histogram"),
                "volume_sma_10": hist["Volume"].rolling(window=10).mean().iloc[-1],
            }

        except Exception as e:
            self.logger.error("Failed to calculate technical indicators", error=str(e))
            return None

    def _fetch_news_data(self, ticker) -> Optional[Dict[str, Any]]:
        """Fetch news data using yfinance."""
        try:
            news = ticker.news

            if not news:
                return None

            return {
                "articles": [
                    {
                        "title": article.get("title"),
                        "link": article.get("link"),
                        "publisher": article.get("publisher"),
                        "published_at": article.get("providerPublishTime"),
                        "type": article.get("type"),
                    }
                    for article in news[:10]  # Limit to 10 articles
                ]
            }

        except Exception as e:
            self.logger.error("Failed to fetch news data", error=str(e))
            return None

    def _get_cached_data(self, cache_key: str) -> Optional[StockData]:
        """Get data from cache if not expired."""
        with self._cache_lock:
            if cache_key in self._cache:
                cached_item = self._cache[cache_key]
                if time.time() - cached_item["timestamp"] < self._cache_ttl:
                    return cached_item["data"]
                else:
                    del self._cache[cache_key]
        return None

    def _cache_data(self, cache_key: str, data: StockData):
        """Cache data with timestamp."""
        with self._cache_lock:
            self._cache[cache_key] = {"data": data, "timestamp": time.time()}

            # Clean old cache entries
            current_time = time.time()
            expired_keys = [
                key
                for key, value in self._cache.items()
                if current_time - value["timestamp"] > self._cache_ttl
            ]
            for key in expired_keys:
                del self._cache[key]


class PandasDataReaderCollector(BaseLibraryCollector):
    """Data collector using pandas-datareader library."""

    def __init__(self):
        """Initialize pandas-datareader collector."""
        try:
            import pandas_datareader as pdr

            self.pdr = pdr
        except ImportError:
            raise ImportError(
                "pandas-datareader library is required. Install with: pip install pandas-datareader"
            )

        collector_config = CollectorConfig(
            name="pandas_datareader", source=DataSource.LIBRARY, timeout=30
        )

        super().__init__(collector_config)

    def validate_config(self) -> bool:
        """Validate pandas-datareader collector configuration."""
        try:
            # Test basic functionality
            end = datetime.now()
            start = end - timedelta(days=5)
            data = self.pdr.get_data_yahoo("AAPL", start=start, end=end)
            return not data.empty
        except Exception as e:
            self.logger.error("pandas-datareader validation failed", error=str(e))
            return False

    def fetch_data_sync(
        self,
        symbol: str,
        data_type: DataType = DataType.PRICE,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs,
    ) -> Optional[StockData]:
        """Fetch data using pandas-datareader."""
        try:
            if end_date is None:
                end_date = datetime.now()
            if start_date is None:
                start_date = end_date - timedelta(days=30)

            if data_type == DataType.PRICE:
                data = self._fetch_yahoo_data(symbol, start_date, end_date)
            else:
                self.logger.warning("Only price data supported", data_type=data_type.value)
                return None

            if data is None:
                return None

            return StockData(
                symbol=symbol,
                data_type=data_type,
                source=DataSource.LIBRARY,
                timestamp=datetime.now(),
                data=data,
                metadata={
                    "library": "pandas-datareader",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
            )

        except Exception as e:
            self.logger.error("Failed to fetch data", symbol=symbol, error=str(e))
            return None

    def _fetch_yahoo_data(
        self, symbol: str, start_date: datetime, end_date: datetime
    ) -> Optional[Dict[str, Any]]:
        """Fetch data from Yahoo Finance via pandas-datareader."""
        try:
            data = self.pdr.get_data_yahoo(symbol, start=start_date, end=end_date)

            if data.empty:
                return None

            latest = data.iloc[-1]

            return {
                "current_price": latest["Close"],
                "open": latest["Open"],
                "high": latest["High"],
                "low": latest["Low"],
                "volume": latest["Volume"],
                "adjusted_close": latest["Adj Close"],
                "historical_data": {
                    "dates": data.index.strftime("%Y-%m-%d").tolist(),
                    "open": data["Open"].tolist(),
                    "high": data["High"].tolist(),
                    "low": data["Low"].tolist(),
                    "close": data["Close"].tolist(),
                    "adj_close": data["Adj Close"].tolist(),
                    "volume": data["Volume"].tolist(),
                },
                "data_points": len(data),
            }

        except Exception as e:
            self.logger.error("Failed to fetch Yahoo data", error=str(e))
            return None


class CustomLibraryCollector(BaseLibraryCollector):
    """Custom library collector that allows plugging in any Python function."""

    def __init__(
        self, name: str, fetch_function: Callable[[str, DataType], Optional[Dict[str, Any]]]
    ):
        """
        Initialize custom library collector.

        Args:
            name: Name of the collector
            fetch_function: Function that takes (symbol, data_type) and returns data dict
        """
        collector_config = CollectorConfig(name=name, source=DataSource.LIBRARY, timeout=30)

        super().__init__(collector_config)
        self.fetch_function = fetch_function

    def validate_config(self) -> bool:
        """Validate custom collector configuration."""
        if not callable(self.fetch_function):
            self.logger.error("Fetch function must be callable")
            return False
        return True

    def fetch_data_sync(
        self, symbol: str, data_type: DataType = DataType.PRICE, **kwargs
    ) -> Optional[StockData]:
        """Fetch data using custom function."""
        try:
            data = self.fetch_function(symbol, data_type, **kwargs)

            if data is None:
                return None

            return StockData(
                symbol=symbol,
                data_type=data_type,
                source=DataSource.LIBRARY,
                timestamp=datetime.now(),
                data=data,
                metadata={"library": "custom", "collector_name": self.config.name},
            )

        except Exception as e:
            self.logger.error("Failed to fetch data", symbol=symbol, error=str(e))
            return None


class BatchLibraryCollector:
    """Collector that manages multiple library collectors and coordinates data collection."""

    def __init__(self):
        """Initialize batch library collector."""
        self.collectors: Dict[str, BaseLibraryCollector] = {}
        self.logger = logger.bind(component="batch_library_collector")

    def add_collector(self, name: str, collector: BaseLibraryCollector):
        """Add a collector to the batch."""
        self.collectors[name] = collector
        self.logger.info("Added collector", name=name, type=type(collector).__name__)

    async def collect_all(
        self, symbols: List[str], data_types: Optional[List[DataType]] = None, **kwargs
    ) -> Dict[str, CollectorResult]:
        """
        Collect data from all configured collectors using thread pool.

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

                with collector:
                    for data_type in data_types:
                        task_id = f"{collector_name}_{data_type.value}"
                        coro = collector.collect(symbols, data_type=data_type, **kwargs)
                        task_id = await concurrency_manager.submit_async_task(coro, task_id=task_id)
                        tasks[collector_name] = task_id

            # Wait for all tasks to complete
            results = {}
            for collector_name, task_id in tasks.items():
                task_result = await concurrency_manager.wait_for_async_task(task_id, timeout=120)
                if task_result.success:
                    results[collector_name] = task_result.result
                else:
                    self.logger.error(
                        "Collector failed", collector=collector_name, error=task_result.error
                    )
                    results[collector_name] = CollectorResult(
                        success=False, error=task_result.error
                    )

        return results
