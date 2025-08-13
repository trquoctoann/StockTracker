"""Message broker senders for sending data to various message queues."""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional

import structlog

from ..core.base import BaseSender
from ..core.config import config
from ..core.models import SenderResult, StockData

logger = structlog.get_logger()


class RedisSender(BaseSender):
    """Sender for Redis pub/sub and streams."""

    def __init__(self, redis_url: Optional[str] = None, channel: str = "stock_data"):
        """
        Initialize Redis sender.

        Args:
            redis_url: Redis connection URL
            channel: Redis channel or stream name
        """
        super().__init__("redis_sender")
        self.redis_url = redis_url or config.redis.url
        self.channel = channel
        self.redis_client = None
        self.connection_pool = None

    async def setup(self):
        """Setup Redis connection."""
        try:
            import redis.asyncio as redis

            self.connection_pool = redis.ConnectionPool.from_url(
                self.redis_url, max_connections=20, decode_responses=True
            )

            self.redis_client = redis.Redis(connection_pool=self.connection_pool)

            await self.redis_client.ping()

            self.logger.info("Redis connection established", url=self.redis_url)

        except ImportError:
            raise ImportError("redis library is required. Install with: pip install redis")
        except Exception as e:
            self.logger.error("Failed to connect to Redis", error=str(e))
            raise

    async def cleanup(self):
        """Cleanup Redis resources."""
        if self.redis_client:
            await self.redis_client.close()
        if self.connection_pool:
            await self.connection_pool.disconnect()

    async def send(self, data: List[StockData], method: str = "publish", **kwargs) -> SenderResult:
        """
        Send data to Redis.

        Args:
            data: List of StockData to send
            method: Method to use ('publish', 'stream', 'list')
            **kwargs: Additional parameters

        Returns:
            SenderResult with send status
        """
        if not self.redis_client:
            return SenderResult(success=False, error="Redis client not initialized")

        try:
            sent_count = 0

            for stock_data in data:
                message = stock_data.to_json()

                if method == "publish":
                    await self.redis_client.publish(self.channel, message)
                elif method == "stream":
                    stream_key = kwargs.get("stream_key", f"stream:{self.channel}")
                    await self.redis_client.xadd(stream_key, {"data": message})
                elif method == "list":
                    list_key = kwargs.get("list_key", f"list:{self.channel}")
                    await self.redis_client.lpush(list_key, message)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                sent_count += 1

            self.logger.info("Data sent to Redis", count=sent_count, method=method, channel=self.channel)

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={"method": method, "channel": self.channel, "timestamp": datetime.now().isoformat()},
            )

        except Exception as e:
            self.logger.error("Failed to send data to Redis", error=str(e))
            return SenderResult(success=False, error=f"Redis send failed: {str(e)}")


class RabbitMQSender(BaseSender):
    """Sender for RabbitMQ message queue."""

    def __init__(
        self, rabbitmq_url: Optional[str] = None, exchange: str = "stock_data", routing_key: str = "stock.data"
    ):
        """
        Initialize RabbitMQ sender.

        Args:
            rabbitmq_url: RabbitMQ connection URL
            exchange: Exchange name
            routing_key: Routing key for messages
        """
        super().__init__("rabbitmq_sender")
        self.rabbitmq_url = rabbitmq_url or config.rabbitmq.url
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = None
        self.channel = None

    async def setup(self):
        """Setup RabbitMQ connection."""
        try:
            import aio_pika

            self.connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.channel = await self.connection.channel()

            await self.channel.declare_exchange(self.exchange, type=aio_pika.ExchangeType.TOPIC, durable=True)

            self.logger.info("RabbitMQ connection established", exchange=self.exchange, routing_key=self.routing_key)

        except ImportError:
            raise ImportError("aio-pika library is required. Install with: pip install aio-pika")
        except Exception as e:
            self.logger.error("Failed to connect to RabbitMQ", error=str(e))
            raise

    async def cleanup(self):
        """Cleanup RabbitMQ resources."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()

    async def send(self, data: List[StockData], routing_key: Optional[str] = None, **kwargs) -> SenderResult:
        """
        Send data to RabbitMQ.

        Args:
            data: List of StockData to send
            routing_key: Override default routing key
            **kwargs: Additional parameters

        Returns:
            SenderResult with send status
        """
        if not self.channel:
            return SenderResult(success=False, error="RabbitMQ channel not initialized")

        try:
            import aio_pika

            routing_key = routing_key or self.routing_key
            sent_count = 0

            for stock_data in data:
                message_body = stock_data.to_json()

                message = aio_pika.Message(
                    message_body.encode("utf-8"),
                    headers={
                        "symbol": stock_data.symbol,
                        "data_type": stock_data.data_type.value,
                        "source": stock_data.source.value,
                        "timestamp": stock_data.timestamp.isoformat(),
                    },
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                )

                exchange = await self.channel.get_exchange(self.exchange)
                await exchange.publish(message, routing_key=routing_key)

                sent_count += 1

            self.logger.info("Data sent to RabbitMQ", count=sent_count, exchange=self.exchange, routing_key=routing_key)

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={
                    "exchange": self.exchange,
                    "routing_key": routing_key,
                    "timestamp": datetime.now().isoformat(),
                },
            )

        except Exception as e:
            self.logger.error("Failed to send data to RabbitMQ", error=str(e))
            return SenderResult(success=False, error=f"RabbitMQ send failed: {str(e)}")


class KafkaSender(BaseSender):
    """Sender for Apache Kafka."""

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "stock_data"):
        """
        Initialize Kafka sender.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
        """
        super().__init__("kafka_sender")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    async def setup(self):
        """Setup Kafka producer."""
        try:
            from aiokafka import AIOKafkaProducer

            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                key_serializer=lambda x: x.encode("utf-8") if x else None,
            )

            await self.producer.start()

            self.logger.info("Kafka producer started", servers=self.bootstrap_servers, topic=self.topic)

        except ImportError:
            raise ImportError("aiokafka library is required. Install with: pip install aiokafka")
        except Exception as e:
            self.logger.error("Failed to start Kafka producer", error=str(e))
            raise

    async def cleanup(self):
        """Cleanup Kafka resources."""
        if self.producer:
            await self.producer.stop()

    async def send(self, data: List[StockData], topic: Optional[str] = None, **kwargs) -> SenderResult:
        """
        Send data to Kafka.

        Args:
            data: List of StockData to send
            topic: Override default topic
            **kwargs: Additional parameters

        Returns:
            SenderResult with send status
        """
        if not self.producer:
            return SenderResult(success=False, error="Kafka producer not initialized")

        try:
            topic = topic or self.topic
            sent_count = 0

            for stock_data in data:
                key = stock_data.symbol
                value = stock_data.to_dict()

                await self.producer.send_and_wait(topic, value=value, key=key)
                sent_count += 1

            self.logger.info("Data sent to Kafka", count=sent_count, topic=topic)

            return SenderResult(
                success=True, sent_count=sent_count, metadata={"topic": topic, "timestamp": datetime.now().isoformat()}
            )

        except Exception as e:
            self.logger.error("Failed to send data to Kafka", error=str(e))
            return SenderResult(success=False, error=f"Kafka send failed: {str(e)}")


class WebSocketSender(BaseSender):
    """Sender for WebSocket connections."""

    def __init__(self, websocket_url: str):
        """
        Initialize WebSocket sender.

        Args:
            websocket_url: WebSocket server URL
        """
        super().__init__("websocket_sender")
        self.websocket_url = websocket_url
        self.websocket = None

    async def setup(self):
        """Setup WebSocket connection."""
        try:
            import websockets

            self.websocket = await websockets.connect(self.websocket_url)

            self.logger.info("WebSocket connection established", url=self.websocket_url)

        except ImportError:
            raise ImportError("websockets library is required. Install with: pip install websockets")
        except Exception as e:
            self.logger.error("Failed to connect to WebSocket", error=str(e))
            raise

    async def cleanup(self):
        """Cleanup WebSocket connection."""
        if self.websocket:
            await self.websocket.close()

    async def send(self, data: List[StockData], **kwargs) -> SenderResult:
        """
        Send data via WebSocket.

        Args:
            data: List of StockData to send
            **kwargs: Additional parameters

        Returns:
            SenderResult with send status
        """
        if not self.websocket:
            return SenderResult(success=False, error="WebSocket not connected")

        try:
            sent_count = 0

            for stock_data in data:
                message = stock_data.to_json()
                await self.websocket.send(message)
                sent_count += 1

            self.logger.info("Data sent via WebSocket", count=sent_count)

            return SenderResult(
                success=True,
                sent_count=sent_count,
                metadata={"websocket_url": self.websocket_url, "timestamp": datetime.now().isoformat()},
            )

        except Exception as e:
            self.logger.error("Failed to send data via WebSocket", error=str(e))
            return SenderResult(success=False, error=f"WebSocket send failed: {str(e)}")


class BatchMessageBrokerSender:
    """Batch sender that coordinates multiple message broker senders."""

    def __init__(self):
        """Initialize batch message broker sender."""
        self.senders: Dict[str, BaseSender] = {}
        self.logger = logger.bind(component="batch_message_broker_sender")

    def add_sender(self, name: str, sender: BaseSender):
        """Add a sender to the batch."""
        self.senders[name] = sender
        self.logger.info("Added sender", name=name, type=type(sender).__name__)

    async def send_to_all(self, data: List[StockData], **kwargs) -> Dict[str, SenderResult]:
        """
        Send data to all configured senders.

        Args:
            data: List of StockData to send
            **kwargs: Additional parameters for senders

        Returns:
            Dictionary mapping sender names to their results
        """
        if not self.senders:
            self.logger.warning("No senders configured")
            return {}

        results = {}

        tasks = []
        sender_names = []

        for sender_name, sender in self.senders.items():

            async def send_data(s, d):
                async with s:
                    return await s.send(d, **kwargs)

            task = asyncio.create_task(send_data(sender, data))
            tasks.append(task)
            sender_names.append(sender_name)

        send_results = await asyncio.gather(*tasks, return_exceptions=True)

        for sender_name, result in zip(sender_names, send_results):
            if isinstance(result, Exception):
                self.logger.error("Sender failed", sender=sender_name, error=str(result))
                results[sender_name] = SenderResult(success=False, error=f"Sender failed: {str(result)}")
            else:
                results[sender_name] = result

                self.logger.info(
                    "Sender completed", sender=sender_name, success=result.success, sent_count=result.sent_count
                )

        return results

    async def send_selective(self, data: List[StockData], sender_names: List[str], **kwargs) -> Dict[str, SenderResult]:
        """
        Send data to specific senders only.

        Args:
            data: List of StockData to send
            sender_names: List of sender names to use
            **kwargs: Additional parameters for senders

        Returns:
            Dictionary mapping sender names to their results
        """
        selected_senders = {name: sender for name, sender in self.senders.items() if name in sender_names}

        if not selected_senders:
            self.logger.warning("No valid senders selected", requested=sender_names)
            return {}

        results = {}
    
        for sender_name, sender in selected_senders.items():
            try:
                async with sender:
                    result = await sender.send(data, **kwargs)
                    results[sender_name] = result

                    self.logger.info(
                        "Selective send completed",
                        sender=sender_name,
                        success=result.success,
                        sent_count=result.sent_count,
                    )

            except Exception as e:
                self.logger.error("Selective send failed", sender=sender_name, error=str(e))
                results[sender_name] = SenderResult(success=False, error=f"Sender failed: {str(e)}")

        return results
