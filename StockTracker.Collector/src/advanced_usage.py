import asyncio
import time
from datetime import datetime
from typing import Any, Dict, Optional

import structlog
from src.collectors.library_collector import CustomLibraryCollector
from src.core.models import DataSource, DataType, StockData
from src.orchestrator import StockDataOrchestrator
from src.senders.api_sender import HTTPAPISender
from src.utils.async_manager import concurrency_manager
from src.utils.rate_limiter import RateLimitRule, rate_limiter

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.LoggingAdapter,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def advanced_rate_limiting_example():
    logger.info("=== RATE LIMITING ===")

    premium_api_rule = RateLimitRule(
        limit=1000,
        period=3600,
        burst_limit=50,
        burst_period=60,
    )

    free_api_rule = RateLimitRule(
        limit=100,
        period=3600,
        burst_limit=5,
        burst_period=60,
    )

    rate_limiter.add_rule("premium_api", premium_api_rule, adaptive=True)
    rate_limiter.add_rule("free_api", free_api_rule, adaptive=False)

    logger.info("Testing rate limiting...")

    for i in range(10):
        allowed = await rate_limiter.is_allowed("free_api", "user_123")
        if allowed:
            logger.info("Request allowed", request_number=i + 1)
            response_time = 0.5 + (i * 0.1)
            rate_limiter.record_response_time("premium_api", response_time)
        else:
            logger.warning("Request denied by rate limiter", request_number=i + 1)
            await rate_limiter.wait_if_needed("free_api", "user_123", max_wait=5.0)


async def custom_collector_example():
    logger.info("=== CUSTOM COLLECTOR ===")

    def crypto_data_fetcher(symbol: str, data_type: DataType, **kwargs) -> Optional[Dict[str, Any]]:
        time.sleep(0.1)

        if data_type == DataType.PRICE:
            return {
                "price": 50000.0 + (hash(symbol) % 10000),
                "volume": 1000000 + (hash(symbol) % 500000),
                "change_24h": -2.5 + (hash(symbol) % 5),
                "market_cap": 1000000000,
                "source": "crypto_exchange_api",
            }
        elif data_type == DataType.NEWS:
            return {
                "articles": [
                    {
                        "title": f"Latest news about {symbol}",
                        "summary": f"Important developments in {symbol} market",
                        "source": "crypto_news",
                        "timestamp": datetime.now().isoformat(),
                    }
                ]
            }

        return None

    crypto_collector = CustomLibraryCollector(
        name="crypto_collector", fetch_function=crypto_data_fetcher
    )

    crypto_symbols = ["BTC", "ETH", "ADA", "DOT", "LINK"]

    with crypto_collector:
        result = await crypto_collector.collect(symbols=crypto_symbols, data_type=DataType.PRICE)

        if result.success and result.data:
            logger.info(
                "Custom collector successful",
                collected_count=len(result.data),
                symbols=crypto_symbols,
            )

            for data in result.data:
                logger.info("Crypto data", **data.to_dict())
        else:
            logger.error("Custom collector failed", error=result.error)


async def custom_sender_example():
    logger.info("=== CUSTOM SENDER ===")

    custom_sender = HTTPAPISender(
        name="custom_api_sender",
        endpoint_url="https://httpbin.org/post",
        method="POST",
        headers={"Content-Type": "application/json"},
        rate_limit_config={"limit": 10, "period": 60, "adaptive": True},
    )

    mock_data = [
        StockData(
            symbol="TEST1",
            data_type=DataType.PRICE,
            source=DataSource.API,
            data={"price": 100.0, "volume": 1000},
            timestamp=datetime.now(),
        ),
        StockData(
            symbol="TEST2",
            data_type=DataType.PRICE,
            source=DataSource.API,
            data={"price": 200.0, "volume": 2000},
            timestamp=datetime.now(),
        ),
    ]

    async with custom_sender:
        result = await custom_sender.send(mock_data, batch_size=1)

        if result.success:
            logger.info(
                "Custom sender successful",
                sent_count=result.sent_count,
                endpoint=custom_sender.endpoint_url,
            )
        else:
            logger.error("Custom sender failed", error=result.error)


async def concurrency_management_example():
    logger.info("=== CONCURRENCY MANAGEMENT ===")

    async def mock_async_task(task_id: str, duration: float):
        """Mock async task."""
        logger.info("Task started", task_id=task_id, duration=duration)
        await asyncio.sleep(duration)
        logger.info("Task completed", task_id=task_id)
        return f"Result from {task_id}"

    task_ids = []
    async with concurrency_manager:
        for i in range(5):
            task_id = await concurrency_manager.submit_async_task(
                mock_async_task(f"task_{i}", 0.5 + i * 0.2),
                task_id=f"async_task_{i}",
                rate_limit_key="test_tasks",
            )
            task_ids.append(task_id)

        logger.info("Waiting for async tasks to complete...")
        for task_id in task_ids:
            result = await concurrency_manager.wait_for_async_task(task_id, timeout=10)
            logger.info(
                "Task result",
                task_id=task_id,
                success=result.success,
                result=result.result,
                duration=result.duration,
            )

        stats = concurrency_manager.get_stats()
        logger.info("Concurrency stats", **stats)


async def batch_processing_with_error_handling():
    logger.info("=== BATCH PROCESSING WITH ERROR HANDLING ===")

    orchestrator = StockDataOrchestrator()

    orchestrator.setup_library_collectors({"yfinance": {"enabled": True}})

    symbols = ["AAPL", "GOOGL", "INVALID_SYMBOL", "MSFT", "ANOTHER_INVALID"]

    try:
        collection_results = await orchestrator.collect_data(
            symbols=symbols, data_types=[DataType.PRICE], collector_types=["library"]
        )

        total_collected = 0
        total_errors = 0

        for collector_type, type_results in collection_results.items():
            for collector_name, collector_result in type_results.items():
                if collector_result.success:
                    data_count = len(collector_result.data) if collector_result.data else 0
                    total_collected += data_count
                    logger.info(
                        "Collector success",
                        collector=collector_name,
                        type=collector_type,
                        collected=data_count,
                    )
                else:
                    total_errors += 1
                    logger.error(
                        "Collector failed",
                        collector=collector_name,
                        type=collector_type,
                        error=collector_result.error,
                    )

        logger.info(
            "Batch processing summary",
            total_symbols=len(symbols),
            total_collected=total_collected,
            total_errors=total_errors,
            success_rate=f"{((len(symbols) - total_errors) / len(symbols)) * 100:.1f}%",
        )

    except Exception as e:
        logger.error("Batch processing failed", error=str(e))


async def monitoring_and_alerting_example():
    logger.info("=== MONITORING & ALERTING ===")

    orchestrator = StockDataOrchestrator()

    orchestrator.setup_library_collectors({"yfinance": {"enabled": True}})

    start_time = time.time()
    symbols = ["AAPL", "GOOGL", "MSFT"]

    results = await orchestrator.collect_data(symbols=symbols, data_types=[DataType.PRICE])

    end_time = time.time()
    duration = end_time - start_time

    total_data_points = 0
    for collector_results in results.values():
        for collector_result in collector_results.values():
            if collector_result.success and collector_result.data:
                total_data_points += len(collector_result.data)

    throughput = total_data_points / duration if duration > 0 else 0

    logger.info(
        "Performance metrics",
        duration_seconds=duration,
        total_data_points=total_data_points,
        throughput_per_second=throughput,
        symbols_processed=len(symbols),
    )

    if duration > 30:
        logger.warning("ALERT: Collection duration exceeded threshold", duration=duration)

    if throughput < 1:
        logger.warning("ALERT: Low throughput detected", throughput=throughput)

    if total_data_points == 0:
        logger.error("ALERT: No data collected")


async def main():
    logger.info("Starting advanced examples...")

    examples = [
        ("Rate Limiting", advanced_rate_limiting_example),
        ("Custom Collector", custom_collector_example),
        ("Custom Sender", custom_sender_example),
        ("Concurrency Management", concurrency_management_example),
        ("Batch Processing", batch_processing_with_error_handling),
        ("Monitoring & Alerting", monitoring_and_alerting_example),
    ]

    for name, example_func in examples:
        try:
            logger.info(f"Running example: {name}")
            await example_func()
            logger.info(f"Completed example: {name}")
            print()

        except Exception as e:
            logger.error(f"Example failed: {name}", error=str(e))

    logger.info("All advanced examples completed!")


if __name__ == "__main__":
    asyncio.run(main())
