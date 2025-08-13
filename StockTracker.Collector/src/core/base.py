import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import structlog

from .config import config
from .models import CollectorConfig, CollectorResult, SenderResult, StockData

logger = structlog.get_logger()


class BaseCollector(ABC):
    def __init__(self, collector_config: CollectorConfig):
        self.config = collector_config
        self.logger = logger.bind(collector=self.config.name)

    @abstractmethod
    async def collect(self, symbols: List[str], **kwargs) -> CollectorResult:
        pass

    @abstractmethod
    def validate_config(self) -> bool:
        pass

    def get_name(self) -> str:
        return self.config.name


# class BaseAPICollector(BaseCollector):
#     def __init__(self, collector_config: CollectorConfig):
#         super().__init__(collector_config)
#         self.session = None
#         self._rate_limiter = None

#     @abstractmethod
#     async def fetch_data(self, symbol: str, **kwargs) -> Optional[StockData]:
#         pass

#     async def __aenter__(self):
#         await self.setup()
#         return self

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.cleanup()

#     async def setup(self):
#         connector = aiohttp.TCPConnector(limit=config.concurrency.max_async_connections)
#         self.session = aiohttp.ClientSession(
#             connector=connector, timeout=aiohttp.ClientTimeout(total=self.config.timeout)
#         )

#     async def cleanup(self):
#         if self.session:
#             await self.session.close()

#     async def collect(self, symbols: List[str], **kwargs) -> CollectorResult:
#         try:
#             tasks = [self.fetch_data(symbol, **kwargs) for symbol in symbols]
#             results = await asyncio.gather(*tasks, return_exceptions=True)

#             data = []
#             errors = []

#             for result in results:
#                 if isinstance(result, Exception):
#                     errors.append(str(result))
#                 elif result is not None:
#                     data.append(result)

#             success = len(errors) == 0 or len(data) > 0
#             error_msg = "; ".join(errors) if errors else None

#             return CollectorResult(
#                 success=success,
#                 data=data,
#                 error=error_msg,
#                 metadata={"total_symbols": len(symbols), "errors_count": len(errors)},
#             )

#         except Exception as e:
#             self.logger.error("Collection failed", error=str(e))
#             return CollectorResult(success=False, error=f"Collection failed: {str(e)}")


class BaseLibraryCollector(BaseCollector):
    def __init__(self, collector_config: CollectorConfig):
        super().__init__(collector_config)
        self._executor = None

    @abstractmethod
    def fetch_data_sync(self, symbol: str, **kwargs) -> Optional[StockData]:
        pass

    def __enter__(self):
        self.setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def setup(self):
        self._executor = ThreadPoolExecutor(max_workers=config.concurrency.max_workers)

    def cleanup(self):
        if self._executor:
            self._executor.shutdown(wait=True)

    async def collect(self, symbols: List[str], **kwargs) -> CollectorResult:
        try:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(self._executor, self.fetch_data_sync, symbol, **kwargs)
                for symbol in symbols
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            data = []
            errors = []

            for result in results:
                if isinstance(result, Exception):
                    errors.append(str(result))
                elif result is not None:
                    data.append(result)

            success = len(errors) == 0 or len(data) > 0
            error_msg = "; ".join(errors) if errors else None

            return CollectorResult(
                success=success,
                data=data,
                error=error_msg,
                metadata={"total_symbols": len(symbols), "errors_count": len(errors)},
            )

        except Exception as e:
            self.logger.error("Collection failed", error=str(e))
            return CollectorResult(success=False, error=f"Collection failed: {str(e)}")


# class BaseWebCollector(BaseCollector):
#     def __init__(self, collector_config: CollectorConfig):
#         super().__init__(collector_config)
#         self.driver = None

#     async def __aenter__(self):
#         await self.setup()
#         return self

#     async def __aexit__(self, exc_type, exc_val, exc_tb):
#         await self.cleanup()

#     async def setup(self):
#         from selenium import webdriver
#         from selenium.webdriver.chrome.options import Options

#         options = Options()
#         if config.selenium.headless:
#             options.add_argument("--headless")
#         options.add_argument("--no-sandbox")
#         options.add_argument("--disable-dev-shm-usage")

#         loop = asyncio.get_event_loop()
#         self.driver = await loop.run_in_executor(None, lambda: webdriver.Chrome(options=options))
#         self.driver.implicitly_wait(config.selenium.timeout)

#     async def cleanup(self):
#         if self.driver:
#             loop = asyncio.get_event_loop()
#             await loop.run_in_executor(None, self.driver.quit)

#     @abstractmethod
#     async def scrape_data(self, symbol: str, **kwargs) -> Optional[StockData]:
#         pass

#     async def collect(self, symbols: List[str], **kwargs) -> CollectorResult:
#         try:
#             data = []
#             errors = []

#             for symbol in symbols:
#                 try:
#                     result = await self.scrape_data(symbol, **kwargs)
#                     if result:
#                         data.append(result)
#                 except Exception as e:
#                     errors.append(f"{symbol}: {str(e)}")

#             success = len(errors) == 0 or len(data) > 0
#             error_msg = "; ".join(errors) if errors else None

#             return CollectorResult(
#                 success=success,
#                 data=data,
#                 error=error_msg,
#                 metadata={"total_symbols": len(symbols), "errors_count": len(errors)},
#             )

#         except Exception as e:
#             self.logger.error("Collection failed", error=str(e))
#             return CollectorResult(success=False, error=f"Collection failed: {str(e)}")


class BaseSender(ABC):
    def __init__(self, name: str):
        self.name = name
        self.logger = logger.bind(sender=name)

    @abstractmethod
    async def send(self, data: List[StockData], **kwargs) -> SenderResult:
        pass

    @abstractmethod
    async def setup(self):
        pass

    @abstractmethod
    async def cleanup(self):
        pass

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()
