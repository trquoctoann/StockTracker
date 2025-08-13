"""Main orchestrator for coordinating data collection and sending."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import structlog

from .collectors.api_collector import (
    AlphaVantageCollector,
    BatchAPICollector,
    FinnhubCollector,
)
from .collectors.library_collector import BatchLibraryCollector, YFinanceCollector
from .collectors.web_collector import BatchWebCollector, YahooFinanceWebCollector
from .core.config import config
from .core.models import (
    CollectorConfig,
    CollectorResult,
    DataType,
    SenderResult,
    StockData,
)
from .senders.api_sender import BatchAPISender, SlackSender, WebhookSender
from .senders.message_broker_sender import (
    BatchMessageBrokerSender,
    RabbitMQSender,
    RedisSender,
)
from .utils.async_manager import concurrency_manager

logger = structlog.get_logger()


class StockDataOrchestrator:
    """Main orchestrator for the stock data collection system."""

    def __init__(self):
        """Initialize the orchestrator."""
        self.logger = logger.bind(component="orchestrator")

        # Batch collectors
        self.api_batch_collector = BatchAPICollector()
        self.library_batch_collector = BatchLibraryCollector()
        self.web_batch_collector = BatchWebCollector()

        # Batch senders
        self.message_broker_batch_sender = BatchMessageBrokerSender()
        self.api_batch_sender = BatchAPISender()

        # Configuration
        self.enabled_collectors: Dict[str, bool] = {}
        self.enabled_senders: Dict[str, bool] = {}

        self.logger.info("Stock Data Orchestrator initialized")

    def setup_api_collectors(self, collectors_config: Dict[str, Dict[str, Any]]):
        """
        Setup API collectors.

        Args:
            collectors_config: Dictionary mapping collector names to their configurations
        """
        for name, config_data in collectors_config.items():
            if name == "alpha_vantage" and config_data.get("enabled", False):
                api_key = config_data.get("api_key") or config.get_api_key("alpha_vantage")
                if api_key:
                    collector = AlphaVantageCollector(api_key)
                    self.api_batch_collector.add_collector(name, collector)
                    self.enabled_collectors[name] = True
                    self.logger.info("Added Alpha Vantage collector")
                else:
                    self.logger.warning("Alpha Vantage API key not found")

            elif name == "finnhub" and config_data.get("enabled", False):
                api_key = config_data.get("api_key") or config.get_api_key("finnhub")
                if api_key:
                    collector = FinnhubCollector(api_key)
                    self.api_batch_collector.add_collector(name, collector)
                    self.enabled_collectors[name] = True
                    self.logger.info("Added Finnhub collector")
                else:
                    self.logger.warning("Finnhub API key not found")

    def setup_library_collectors(self, collectors_config: Dict[str, Dict[str, Any]]):
        """Setup library collectors."""
        for name, config_data in collectors_config.items():
            if name == "yfinance" and config_data.get("enabled", False):
                try:
                    collector = YFinanceCollector()
                    if collector.validate_config():
                        self.library_batch_collector.add_collector(name, collector)
                        self.enabled_collectors[name] = True
                        self.logger.info("Added yfinance collector")
                    else:
                        self.logger.warning("yfinance collector validation failed")
                except ImportError as e:
                    self.logger.warning("yfinance not available", error=str(e))

    def setup_web_collectors(self, collectors_config: Dict[str, Dict[str, Any]]):
        """Setup web collectors."""
        for name, config_data in collectors_config.items():
            if name == "yahoo_finance_web" and config_data.get("enabled", False):
                try:
                    collector = YahooFinanceWebCollector()
                    if collector.validate_config():
                        self.web_batch_collector.add_collector(name, collector)
                        self.enabled_collectors[name] = True
                        self.logger.info("Added Yahoo Finance web collector")
                    else:
                        self.logger.warning("Yahoo Finance web collector validation failed")
                except ImportError as e:
                    self.logger.warning("Selenium not available", error=str(e))

    def setup_message_broker_senders(self, senders_config: Dict[str, Dict[str, Any]]):
        """Setup message broker senders."""
        for name, config_data in senders_config.items():
            if name == "redis" and config_data.get("enabled", False):
                redis_url = config_data.get("url") or config.redis.url
                channel = config_data.get("channel", "stock_data")
                sender = RedisSender(redis_url, channel)
                self.message_broker_batch_sender.add_sender(name, sender)
                self.enabled_senders[name] = True
                self.logger.info("Added Redis sender")

            elif name == "rabbitmq" and config_data.get("enabled", False):
                rabbitmq_url = config_data.get("url") or config.rabbitmq.url
                exchange = config_data.get("exchange", "stock_data")
                routing_key = config_data.get("routing_key", "stock.data")
                sender = RabbitMQSender(rabbitmq_url, exchange, routing_key)
                self.message_broker_batch_sender.add_sender(name, sender)
                self.enabled_senders[name] = True
                self.logger.info("Added RabbitMQ sender")

    def setup_api_senders(self, senders_config: Dict[str, Dict[str, Any]]):
        """Setup API senders."""
        for name, config_data in senders_config.items():
            if name == "webhook" and config_data.get("enabled", False):
                webhook_url = config_data.get("url")
                secret = config_data.get("secret")
                if webhook_url:
                    sender = WebhookSender(webhook_url, secret)
                    self.api_batch_sender.add_sender(name, sender)
                    self.enabled_senders[name] = True
                    self.logger.info("Added webhook sender")
                else:
                    self.logger.warning("Webhook URL not configured")

            elif name == "slack" and config_data.get("enabled", False):
                webhook_url = config_data.get("webhook_url")
                channel = config_data.get("channel", "#general")
                if webhook_url:
                    sender = SlackSender(webhook_url, channel)
                    self.api_batch_sender.add_sender(name, sender)
                    self.enabled_senders[name] = True
                    self.logger.info("Added Slack sender")
                else:
                    self.logger.warning("Slack webhook URL not configured")

    async def collect_data(
        self,
        symbols: List[str],
        data_types: Optional[List[DataType]] = None,
        collector_types: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Dict[str, CollectorResult]]:
        """
        Collect data from all enabled collectors.

        Args:
            symbols: List of stock symbols to collect
            data_types: List of data types to collect
            collector_types: List of collector types to use ('api', 'library', 'web')
            **kwargs: Additional parameters for collectors

        Returns:
            Dictionary mapping collector types to their results
        """
        if data_types is None:
            data_types = [DataType.PRICE]

        if collector_types is None:
            collector_types = ["api", "library", "web"]

        self.logger.info(
            "Starting data collection",
            symbols=symbols,
            data_types=[dt.value for dt in data_types],
            collector_types=collector_types,
        )

        results = {}
        tasks = []
        task_names = []

        # Collect from API collectors
        if "api" in collector_types and self.api_batch_collector.collectors:
            task = asyncio.create_task(self.api_batch_collector.collect_all(symbols, data_types, **kwargs))
            tasks.append(task)
            task_names.append("api")

        # Collect from library collectors
        if "library" in collector_types and self.library_batch_collector.collectors:
            task = asyncio.create_task(self.library_batch_collector.collect_all(symbols, data_types, **kwargs))
            tasks.append(task)
            task_names.append("library")

        # Collect from web collectors
        if "web" in collector_types and self.web_batch_collector.collectors:
            task = asyncio.create_task(self.web_batch_collector.collect_all(symbols, data_types, **kwargs))
            tasks.append(task)
            task_names.append("web")

        if not tasks:
            self.logger.warning("No collectors available for data collection")
            return {}

        # Wait for all collection tasks to complete
        collection_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for task_name, result in zip(task_names, collection_results):
            if isinstance(result, Exception):
                self.logger.error("Collection failed", collector_type=task_name, error=str(result))
                results[task_name] = {}
            else:
                results[task_name] = result

                # Log collection summary
                total_collected = sum(
                    len(collector_result.data) if collector_result.data else 0 for collector_result in result.values()
                )
                self.logger.info(
                    "Collection completed",
                    collector_type=task_name,
                    collectors_used=len(result),
                    total_data_points=total_collected,
                )

        return results

    async def send_data(
        self,
        data: List[StockData],
        sender_types: Optional[List[str]] = None,
        specific_senders: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Dict[str, SenderResult]]:
        """
        Send data to all enabled senders.

        Args:
            data: List of StockData to send
            sender_types: List of sender types to use ('message_broker', 'api')
            specific_senders: List of specific sender names to use
            **kwargs: Additional parameters for senders

        Returns:
            Dictionary mapping sender types to their results
        """
        if not data:
            self.logger.warning("No data to send")
            return {}

        if sender_types is None:
            sender_types = ["message_broker", "api"]

        self.logger.info(
            "Starting data sending", data_count=len(data), sender_types=sender_types, specific_senders=specific_senders
        )

        results = {}
        tasks = []
        task_names = []

        # Send to message broker senders
        if "message_broker" in sender_types and self.message_broker_batch_sender.senders:
            if specific_senders:
                mb_senders = [s for s in specific_senders if s in self.message_broker_batch_sender.senders]
                if mb_senders:
                    task = asyncio.create_task(
                        self.message_broker_batch_sender.send_selective(data, mb_senders, **kwargs)
                    )
                    tasks.append(task)
                    task_names.append("message_broker")
            else:
                task = asyncio.create_task(self.message_broker_batch_sender.send_to_all(data, **kwargs))
                tasks.append(task)
                task_names.append("message_broker")

        # Send to API senders
        if "api" in sender_types and self.api_batch_sender.senders:
            if specific_senders:
                api_senders = [s for s in specific_senders if s in self.api_batch_sender.senders]
                if api_senders:
                    # API senders don't have send_selective method, so we'll handle it differently
                    task = asyncio.create_task(self.api_batch_sender.send_to_all(data, **kwargs))
                    tasks.append(task)
                    task_names.append("api")
            else:
                task = asyncio.create_task(self.api_batch_sender.send_to_all(data, **kwargs))
                tasks.append(task)
                task_names.append("api")

        if not tasks:
            self.logger.warning("No senders available for data sending")
            return {}

        # Wait for all sending tasks to complete
        sending_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for task_name, result in zip(task_names, sending_results):
            if isinstance(result, Exception):
                self.logger.error("Sending failed", sender_type=task_name, error=str(result))
                results[task_name] = {}
            else:
                results[task_name] = result

                # Log sending summary
                total_sent = sum(sender_result.sent_count for sender_result in result.values())
                self.logger.info(
                    "Sending completed", sender_type=task_name, senders_used=len(result), total_sent=total_sent
                )

        return results

    async def collect_and_send(
        self,
        symbols: List[str],
        data_types: Optional[List[DataType]] = None,
        collector_types: Optional[List[str]] = None,
        sender_types: Optional[List[str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Collect data and immediately send it.

        Args:
            symbols: List of stock symbols to collect
            data_types: List of data types to collect
            collector_types: List of collector types to use
            sender_types: List of sender types to use
            **kwargs: Additional parameters

        Returns:
            Dictionary with collection and sending results
        """
        self.logger.info(
            "Starting collect and send operation",
            symbols=symbols,
            data_types=[dt.value for dt in data_types] if data_types else ["price"],
        )

        # Collect data
        collection_results = await self.collect_data(symbols, data_types, collector_types, **kwargs)

        # Aggregate all collected data
        all_data = []
        for collector_type, type_results in collection_results.items():
            for collector_name, collector_result in type_results.items():
                if collector_result.success and collector_result.data:
                    all_data.extend(collector_result.data)

        if not all_data:
            self.logger.warning("No data collected, skipping send operation")
            return {
                "collection_results": collection_results,
                "sending_results": {},
                "summary": {"collected_count": 0, "sent_count": 0, "timestamp": datetime.now().isoformat()},
            }

        # Send data
        sending_results = await self.send_data(all_data, sender_types, **kwargs)

        # Calculate summary
        total_sent = 0
        for sender_type, type_results in sending_results.items():
            for sender_name, sender_result in type_results.items():
                total_sent += sender_result.sent_count

        summary = {"collected_count": len(all_data), "sent_count": total_sent, "timestamp": datetime.now().isoformat()}

        self.logger.info("Collect and send operation completed", **summary)

        return {"collection_results": collection_results, "sending_results": sending_results, "summary": summary}

    def get_status(self) -> Dict[str, Any]:
        """Get status of all collectors and senders."""
        return {
            "collectors": {
                "api": {
                    "count": len(self.api_batch_collector.collectors),
                    "names": list(self.api_batch_collector.collectors.keys()),
                },
                "library": {
                    "count": len(self.library_batch_collector.collectors),
                    "names": list(self.library_batch_collector.collectors.keys()),
                },
                "web": {
                    "count": len(self.web_batch_collector.collectors),
                    "names": list(self.web_batch_collector.collectors.keys()),
                },
            },
            "senders": {
                "message_broker": {
                    "count": len(self.message_broker_batch_sender.senders),
                    "names": list(self.message_broker_batch_sender.senders.keys()),
                },
                "api": {
                    "count": len(self.api_batch_sender.senders),
                    "names": list(self.api_batch_sender.senders.keys()),
                },
            },
            "enabled_collectors": self.enabled_collectors,
            "enabled_senders": self.enabled_senders,
            "concurrency_stats": concurrency_manager.get_stats(),
        }
