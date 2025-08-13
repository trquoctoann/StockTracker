"""Web scraping data collectors using Selenium."""

import asyncio
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import structlog
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ..core.base import BaseWebCollector
from ..core.config import config
from ..core.models import (
    CollectorConfig,
    CollectorResult,
    DataSource,
    DataType,
    StockData,
)
from ..utils.async_manager import concurrency_manager

logger = structlog.get_logger()


class GenericWebCollector(BaseWebCollector):
    """Generic web scraper that can be configured for different websites."""

    def __init__(self, collector_config: CollectorConfig, scraping_config: Dict[str, Any]):
        """
        Initialize generic web collector.

        Args:
            collector_config: Base collector configuration
            scraping_config: Web scraping configuration including:
                - base_url: Base URL template
                - selectors: CSS selectors for data extraction
                - wait_conditions: Conditions to wait for
                - rate_limit: Scraping rate limit
        """
        super().__init__(collector_config)
        self.scraping_config = scraping_config
        self.base_url = scraping_config.get("base_url", "")
        self.selectors = scraping_config.get("selectors", {})
        self.wait_conditions = scraping_config.get("wait_conditions", {})
        self.rate_limit = scraping_config.get("rate_limit", 1.0)  # seconds between requests
        self._last_request_time = 0

    def validate_config(self) -> bool:
        """Validate web collector configuration."""
        if not self.base_url:
            self.logger.error("Base URL is required for web collector")
            return False

        if not self.selectors:
            self.logger.error("At least one selector must be configured")
            return False

        return True

    async def scrape_data(self, symbol: str, data_type: DataType = DataType.PRICE, **kwargs) -> Optional[StockData]:
        """
        Scrape data for a single symbol.

        Args:
            symbol: Stock symbol to scrape data for
            data_type: Type of data to scrape
            **kwargs: Additional parameters for scraping

        Returns:
            StockData object if successful, None otherwise
        """
        if not self.driver:
            raise RuntimeError("WebDriver not initialized. Use async context manager.")

        try:
            # Rate limiting
            await self._apply_rate_limit()

            # Build URL
            url = self._build_url(symbol, data_type, **kwargs)

            # Navigate to page
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.driver.get, url)

            # Wait for page to load
            await self._wait_for_page_load(data_type)

            # Extract data
            data = await self._extract_data(symbol, data_type)

            if data is None:
                return None

            return StockData(
                symbol=symbol,
                data_type=data_type,
                source=DataSource.WEB_SCRAPING,
                timestamp=datetime.now(),
                data=data,
                metadata={"scraper": self.config.name, "url": url, "scrape_time": datetime.now().isoformat()},
            )

        except Exception as e:
            self.logger.error("Failed to scrape data", symbol=symbol, error=str(e))
            return None

    async def _apply_rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time

        if time_since_last < self.rate_limit:
            wait_time = self.rate_limit - time_since_last
            await asyncio.sleep(wait_time)

        self._last_request_time = time.time()

    def _build_url(self, symbol: str, data_type: DataType, **kwargs) -> str:
        """Build URL for scraping."""
        url_template = self.base_url

        # Replace placeholders
        url = url_template.replace("{symbol}", symbol)
        url = url.replace("{data_type}", data_type.value)

        # Add additional parameters
        for key, value in kwargs.items():
            url = url.replace(f"{{{key}}}", str(value))

        return url

    async def _wait_for_page_load(self, data_type: DataType):
        """Wait for page elements to load."""
        if data_type.value not in self.wait_conditions:
            return

        wait_config = self.wait_conditions[data_type.value]
        wait_type = wait_config.get("type", "presence")
        selector = wait_config.get("selector")
        timeout = wait_config.get("timeout", config.selenium.timeout)

        if not selector:
            return

        loop = asyncio.get_event_loop()

        def wait_for_element():
            wait = WebDriverWait(self.driver, timeout)

            if wait_type == "presence":
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            elif wait_type == "visible":
                wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, selector)))
            elif wait_type == "clickable":
                wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))

        try:
            await loop.run_in_executor(None, wait_for_element)
        except TimeoutException:
            self.logger.warning("Wait condition timed out", selector=selector, timeout=timeout)

    async def _extract_data(self, symbol: str, data_type: DataType) -> Optional[Dict[str, Any]]:
        """Extract data from the current page."""
        if data_type.value not in self.selectors:
            self.logger.warning("No selectors configured for data type", data_type=data_type.value)
            return None

        selectors_config = self.selectors[data_type.value]
        loop = asyncio.get_event_loop()

        def extract():
            data = {}

            for field_name, selector_config in selectors_config.items():
                try:
                    value = self._extract_field(selector_config)
                    if value is not None:
                        data[field_name] = value
                except Exception as e:
                    self.logger.warning(
                        "Failed to extract field", field=field_name, selector=selector_config, error=str(e)
                    )

            return data if data else None

        return await loop.run_in_executor(None, extract)

    def _extract_field(self, selector_config: Union[str, Dict[str, Any]]) -> Optional[Any]:
        """Extract a single field from the page."""
        if isinstance(selector_config, str):
            selector_config = {"selector": selector_config}

        selector = selector_config["selector"]
        attribute = selector_config.get("attribute", "text")
        data_type = selector_config.get("type", "string")
        regex = selector_config.get("regex")
        default = selector_config.get("default")

        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)

            # Get value based on attribute
            if attribute == "text":
                value = element.text.strip()
            else:
                value = element.get_attribute(attribute)

            if not value and default is not None:
                value = default

            if not value:
                return None

            # Apply regex if specified
            if regex:
                match = re.search(regex, value)
                if match:
                    value = match.group(1) if match.groups() else match.group(0)
                else:
                    return None

            # Convert data type
            return self._convert_data_type(value, data_type)

        except NoSuchElementException:
            self.logger.debug("Element not found", selector=selector)
            return default

    def _convert_data_type(self, value: str, data_type: str) -> Any:
        """Convert extracted string value to specified data type."""
        try:
            if data_type == "float":
                # Remove common formatting characters
                cleaned = re.sub(r"[^\d.-]", "", value)
                return float(cleaned) if cleaned else 0.0
            elif data_type == "int":
                cleaned = re.sub(r"[^\d-]", "", value)
                return int(cleaned) if cleaned else 0
            elif data_type == "percent":
                cleaned = re.sub(r"[^\d.-]", "", value)
                return float(cleaned) if cleaned else 0.0
            else:  # string
                return value
        except ValueError:
            self.logger.warning("Failed to convert data type", value=value, type=data_type)
            return value


class YahooFinanceWebCollector(GenericWebCollector):
    """Yahoo Finance web scraper."""

    def __init__(self):
        """Initialize Yahoo Finance web collector."""
        scraping_config = {
            "base_url": "https://finance.yahoo.com/quote/{symbol}",
            "rate_limit": 2.0,  # 2 seconds between requests to be respectful
            "wait_conditions": {"price": {"type": "presence", "selector": "[data-symbol='{symbol}']", "timeout": 15}},
            "selectors": {
                "price": {
                    "current_price": {
                        "selector": "[data-testid='qsp-price'] [data-field='regularMarketPrice']",
                        "type": "float",
                    },
                    "change": {
                        "selector": "[data-testid='qsp-price'] [data-field='regularMarketChange']",
                        "type": "float",
                    },
                    "change_percent": {
                        "selector": "[data-testid='qsp-price'] [data-field='regularMarketChangePercent']",
                        "type": "percent",
                        "regex": r"([+-]?\d+\.?\d*)%",
                    },
                    "previous_close": {"selector": "td[data-test='PREV_CLOSE-value']", "type": "float"},
                    "open": {"selector": "td[data-test='OPEN-value']", "type": "float"},
                    "volume": {
                        "selector": "td[data-test='TD_VOLUME-value']",
                        "type": "string",  # Keep as string due to formatting (e.g., "1.2M")
                    },
                    "market_cap": {"selector": "td[data-test='MARKET_CAP-value']", "type": "string"},
                }
            },
        }

        collector_config = CollectorConfig(name="yahoo_finance_web", source=DataSource.WEB_SCRAPING, timeout=30)

        super().__init__(collector_config, scraping_config)


class GoogleFinanceWebCollector(GenericWebCollector):
    """Google Finance web scraper."""

    def __init__(self):
        """Initialize Google Finance web collector."""
        scraping_config = {
            "base_url": "https://www.google.com/finance/quote/{symbol}:NASDAQ",
            "rate_limit": 2.0,
            "wait_conditions": {
                "price": {"type": "presence", "selector": "[data-symbol='{symbol}'] .YMlKec", "timeout": 15}
            },
            "selectors": {
                "price": {
                    "current_price": {"selector": ".YMlKec.fxKbKc", "type": "float"},
                    "change": {"selector": ".SEB4yb:first-child", "type": "float"},
                    "change_percent": {
                        "selector": ".SEB4yb:last-child",
                        "type": "percent",
                        "regex": r"\(([+-]?\d+\.?\d*)%\)",
                    },
                }
            },
        }

        collector_config = CollectorConfig(name="google_finance_web", source=DataSource.WEB_SCRAPING, timeout=30)

        super().__init__(collector_config, scraping_config)


class MarketWatchWebCollector(GenericWebCollector):
    """MarketWatch web scraper."""

    def __init__(self):
        """Initialize MarketWatch web collector."""
        scraping_config = {
            "base_url": "https://www.marketwatch.com/investing/stock/{symbol}",
            "rate_limit": 3.0,
            "wait_conditions": {"price": {"type": "presence", "selector": ".value", "timeout": 20}},
            "selectors": {
                "price": {
                    "current_price": {"selector": ".intraday__price .value", "type": "float"},
                    "change": {"selector": ".change--point--q .value", "type": "float"},
                    "change_percent": {
                        "selector": ".change--percent--q .value",
                        "type": "percent",
                        "regex": r"([+-]?\d+\.?\d*)%",
                    },
                },
                "fundamentals": {
                    "market_cap": {"selector": "mw-rangeBar[field='marketcap'] .primary", "type": "string"},
                    "pe_ratio": {"selector": "mw-rangeBar[field='pe'] .primary", "type": "float"},
                    "dividend_yield": {"selector": "mw-rangeBar[field='dividendyield'] .primary", "type": "percent"},
                },
            },
        }

        collector_config = CollectorConfig(name="marketwatch_web", source=DataSource.WEB_SCRAPING, timeout=30)

        super().__init__(collector_config, scraping_config)


class NewsWebCollector(BaseWebCollector):
    """Web scraper specifically for financial news."""

    def __init__(self, news_sites_config: List[Dict[str, Any]]):
        """
        Initialize news web collector.

        Args:
            news_sites_config: List of news site configurations
        """
        collector_config = CollectorConfig(name="news_web_collector", source=DataSource.WEB_SCRAPING, timeout=30)

        super().__init__(collector_config)
        self.news_sites = news_sites_config
        self.rate_limit = 2.0
        self._last_request_time = 0

    def validate_config(self) -> bool:
        """Validate news collector configuration."""
        return bool(self.news_sites)

    async def scrape_data(self, symbol: str, **kwargs) -> Optional[StockData]:
        """Scrape news data for a symbol."""
        try:
            all_articles = []

            for site_config in self.news_sites:
                articles = await self._scrape_site_news(symbol, site_config)
                if articles:
                    all_articles.extend(articles)

            if not all_articles:
                return None

            return StockData(
                symbol=symbol,
                data_type=DataType.NEWS,
                source=DataSource.WEB_SCRAPING,
                timestamp=datetime.now(),
                data={"articles": all_articles},
                metadata={"sites_scraped": len(self.news_sites), "articles_found": len(all_articles)},
            )

        except Exception as e:
            self.logger.error("Failed to scrape news", symbol=symbol, error=str(e))
            return None

    async def _scrape_site_news(self, symbol: str, site_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Scrape news from a specific site."""
        try:
            # Rate limiting
            await self._apply_rate_limit()

            # Build URL
            url = site_config["url_template"].replace("{symbol}", symbol)

            # Navigate to page
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.driver.get, url)

            # Wait for articles to load
            try:
                wait = WebDriverWait(self.driver, 10)
                await loop.run_in_executor(
                    None,
                    lambda: wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, site_config["article_selector"]))
                    ),
                )
            except TimeoutException:
                self.logger.warning("Articles not found", site=site_config["name"], url=url)
                return []

            # Extract articles
            def extract_articles():
                articles = []
                elements = self.driver.find_elements(By.CSS_SELECTOR, site_config["article_selector"])

                for element in elements[: site_config.get("max_articles", 10)]:
                    try:
                        article = {}

                        # Extract title
                        title_element = element.find_element(By.CSS_SELECTOR, site_config["title_selector"])
                        article["title"] = title_element.text.strip()

                        # Extract link
                        if "link_selector" in site_config:
                            link_element = element.find_element(By.CSS_SELECTOR, site_config["link_selector"])
                            article["link"] = link_element.get_attribute("href")
                        else:
                            article["link"] = title_element.get_attribute("href")

                        # Extract date if available
                        if "date_selector" in site_config:
                            try:
                                date_element = element.find_element(By.CSS_SELECTOR, site_config["date_selector"])
                                article["published_at"] = date_element.text.strip()
                            except NoSuchElementException:
                                pass

                        # Extract summary if available
                        if "summary_selector" in site_config:
                            try:
                                summary_element = element.find_element(By.CSS_SELECTOR, site_config["summary_selector"])
                                article["summary"] = summary_element.text.strip()
                            except NoSuchElementException:
                                pass

                        article["source"] = site_config["name"]
                        articles.append(article)

                    except Exception as e:
                        self.logger.debug("Failed to extract article", error=str(e))
                        continue

                return articles

            return await loop.run_in_executor(None, extract_articles)

        except Exception as e:
            self.logger.error("Failed to scrape site", site=site_config["name"], error=str(e))
            return []

    async def _apply_rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time

        if time_since_last < self.rate_limit:
            wait_time = self.rate_limit - time_since_last
            await asyncio.sleep(wait_time)

        self._last_request_time = time.time()


class BatchWebCollector:
    """Collector that manages multiple web collectors and coordinates scraping."""

    def __init__(self):
        """Initialize batch web collector."""
        self.collectors: Dict[str, BaseWebCollector] = {}
        self.logger = logger.bind(component="batch_web_collector")

    def add_collector(self, name: str, collector: BaseWebCollector):
        """Add a collector to the batch."""
        self.collectors[name] = collector
        self.logger.info("Added collector", name=name, type=type(collector).__name__)

    async def collect_all(
        self, symbols: List[str], data_types: Optional[List[DataType]] = None, **kwargs
    ) -> Dict[str, CollectorResult]:
        """
        Collect data from all configured web collectors.

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

        # Web scraping is typically done sequentially to avoid overloading servers
        results = {}

        for collector_name, collector in self.collectors.items():
            if not collector.is_enabled():
                continue

            try:
                async with collector:
                    result = await collector.collect(symbols, **kwargs)
                    results[collector_name] = result

                    self.logger.info(
                        "Collector completed",
                        collector=collector_name,
                        success=result.success,
                        data_count=len(result.data) if result.data else 0,
                    )

            except Exception as e:
                self.logger.error("Collector failed", collector=collector_name, error=str(e))
                results[collector_name] = CollectorResult(success=False, error=f"Collector failed: {str(e)}")

        return results
