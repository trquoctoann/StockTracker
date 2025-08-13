"""API senders for sending data to external APIs and webhooks."""

import asyncio
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import aiohttp
import structlog

from ..core.base import BaseSender
from ..core.models import SenderResult, StockData
from ..utils.rate_limiter import RateLimitRule, rate_limiter

logger = structlog.get_logger()


class HTTPAPISender(BaseSender):
    """Generic HTTP API sender for sending data to REST endpoints."""

    def __init__(
        self,
        name: str,
        endpoint_url: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[Dict[str, str]] = None,
        rate_limit_config: Optional[Dict[str, Union[int, float]]] = None,
    ):
        """
        Initialize HTTP API sender.

        Args:
            name: Sender name
            endpoint_url: API endpoint URL
            method: HTTP method (GET, POST, PUT, PATCH)
            headers: Additional HTTP headers
            auth: Authentication configuration
            rate_limit_config: Rate limiting configuration
        """
        super().__init__(name)
        self.endpoint_url = endpoint_url
        self.method = method.upper()
        self.headers = headers or {}
        self.auth = auth or {}
        self.session = None

        # Setup rate limiting
        if rate_limit_config:
            rule = RateLimitRule(
                limit=rate_limit_config.get("limit", 100),
                period=rate_limit_config.get("period", 60),
                burst_limit=rate_limit_config.get("burst_limit"),
                burst_period=rate_limit_config.get("burst_period"),
            )
            rate_limiter.add_rule(name, rule, adaptive=rate_limit_config.get("adaptive", False))

    async def setup(self):
        """Setup HTTP session."""
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=20)

        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=self._build_auth_headers())

        self.logger.info("HTTP API sender initialized", endpoint=self.endpoint_url)

    async def cleanup(self):
        """Cleanup HTTP session."""
        if self.session:
            await self.session.close()

    def _build_auth_headers(self) -> Dict[str, str]:
        """Build authentication headers."""
        auth_headers = self.headers.copy()

        if "api_key" in self.auth:
            key_name = self.auth.get("api_key_header", "X-API-Key")
            auth_headers[key_name] = self.auth["api_key"]
        elif "bearer_token" in self.auth:
            auth_headers["Authorization"] = f"Bearer {self.auth['bearer_token']}"
        elif "basic_auth" in self.auth:
            import base64

            credentials = f"{self.auth['username']}:{self.auth['password']}"
            encoded = base64.b64encode(credentials.encode()).decode()
            auth_headers["Authorization"] = f"Basic {encoded}"

        return auth_headers

    async def send(
        self, data: List[StockData], batch_size: int = 100, format_function: Optional[callable] = None, **kwargs
    ) -> SenderResult:
        """
        Send data to HTTP API.

        Args:
            data: List of StockData to send
            batch_size: Number of records to send per request
            format_function: Custom function to format data before sending
            **kwargs: Additional parameters

        Returns:
            SenderResult with send status
        """
        if not self.session:
            return SenderResult(success=False, error="HTTP session not initialized")

        try:
            sent_count = 0

            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]

                await rate_limiter.wait_if_needed(self.name, "api_request")

                if format_function:
                    payload = format_function(batch)
                else:
                    payload = self._default_format(batch)

                success = await self._send_batch(payload)

                if success:
                    sent_count += len(batch)
                else:
                    self.logger.warning("Batch send failed", batch_size=len(batch))

            success = sent_count > 0

            self.logger.info("HTTP API send completed", total_sent=sent_count, total_data=len(data), success=success)

            return SenderResult(
                success=success,
                sent_count=sent_count,
                metadata={
                    "endpoint": self.endpoint_url,
                    "method": self.method,
                    "batches_sent": (len(data) + batch_size - 1) // batch_size,
                    "timestamp": datetime.now().isoformat(),
                },
            )

        except Exception as e:
            self.logger.error("HTTP API send failed", error=str(e))
            return SenderResult(success=False, error=f"HTTP API send failed: {str(e)}")

    def _default_format(self, batch: List[StockData]) -> Dict[str, Any]:
        """Default data formatting."""
        return {
            "data": [stock_data.to_dict() for stock_data in batch],
            "timestamp": datetime.now().isoformat(),
            "count": len(batch),
        }

    async def _send_batch(self, payload: Dict[str, Any]) -> bool:
        """Send a single batch to the API."""
        try:
            async with self.session.request(
                self.method, self.endpoint_url, json=payload, headers=self.headers
            ) as response:

                if response.status < 400:
                    return True
                else:
                    error_text = await response.text()
                    self.logger.error("API request failed", status=response.status, error=error_text)
                    return False

        except Exception as e:
            self.logger.error("Failed to send batch", error=str(e))
            return False


class WebhookSender(HTTPAPISender):
    """Webhook sender for sending data to webhook endpoints."""

    def __init__(self, webhook_url: str, secret: Optional[str] = None, headers: Optional[Dict[str, str]] = None):
        """
        Initialize webhook sender.

        Args:
            webhook_url: Webhook URL
            secret: Optional webhook secret for signature
            headers: Additional headers
        """
        super().__init__(name="webhook_sender", endpoint_url=webhook_url, method="POST", headers=headers)
        self.secret = secret

    async def send(self, data: List[StockData], **kwargs) -> SenderResult:
        """Send data to webhook with optional signature."""
        if self.secret:
            payload = self._default_format(data)
            signature = self._generate_signature(json.dumps(payload))
            self.headers["X-Signature"] = signature

        return await super().send(data, **kwargs)

    def _generate_signature(self, payload: str) -> str:
        """Generate HMAC signature for webhook security."""
        import hashlib
        import hmac

        signature = hmac.new(self.secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

        return f"sha256={signature}"


class DatabaseAPISender(HTTPAPISender):
    """Sender for database APIs (e.g., InfluxDB, TimescaleDB REST APIs)."""

    def __init__(self, api_url: str, database: str, table: str, api_key: Optional[str] = None):
        """
        Initialize database API sender.

        Args:
            api_url: Database API URL
            database: Database name
            table: Table/measurement name
            api_key: API key for authentication
        """
        auth = {"api_key": api_key} if api_key else {}

        super().__init__(name="database_api_sender", endpoint_url=f"{api_url}/write", method="POST", auth=auth)
        self.database = database
        self.table = table

    def _default_format(self, batch: List[StockData]) -> Dict[str, Any]:
        """Format data for database API."""
        points = []

        for stock_data in batch:
            point = {
                "measurement": self.table,
                "tags": {
                    "symbol": stock_data.symbol,
                    "data_type": stock_data.data_type.value,
                    "source": stock_data.source.value,
                },
                "fields": stock_data.data,
                "time": stock_data.timestamp.isoformat(),
            }
            points.append(point)

        return {"database": self.database, "points": points}


class SlackSender(HTTPAPISender):
    """Sender for Slack notifications."""

    def __init__(self, webhook_url: str, channel: str = "#general"):
        """
        Initialize Slack sender.

        Args:
            webhook_url: Slack webhook URL
            channel: Slack channel to send to
        """
        super().__init__(name="slack_sender", endpoint_url=webhook_url, method="POST")
        self.channel = channel

    def _default_format(self, batch: List[StockData]) -> Dict[str, Any]:
        """Format data for Slack message."""
        symbols = list(set(stock.symbol for stock in batch))

        text = f"📊 Stock data update for {len(symbols)} symbols: {', '.join(symbols[:5])}"
        if len(symbols) > 5:
            text += f" and {len(symbols) - 5} more"

        return {
            "channel": self.channel,
            "text": text,
            "attachments": [
                {
                    "color": "good",
                    "fields": [
                        {"title": "Data Points", "value": str(len(batch)), "short": True},
                        {"title": "Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "short": True},
                    ],
                }
            ],
        }


class DiscordSender(HTTPAPISender):
    """Sender for Discord notifications."""

    def __init__(self, webhook_url: str):
        """
        Initialize Discord sender.

        Args:
            webhook_url: Discord webhook URL
        """
        super().__init__(name="discord_sender", endpoint_url=webhook_url, method="POST")

    def _default_format(self, batch: List[StockData]) -> Dict[str, Any]:
        """Format data for Discord message."""
        symbols = list(set(stock.symbol for stock in batch))

        embed = {
            "title": "📈 Stock Data Update",
            "color": 0x00FF00,
            "fields": [
                {"name": "Symbols", "value": ", ".join(symbols[:10]), "inline": True},
                {"name": "Data Points", "value": str(len(batch)), "inline": True},
                {"name": "Timestamp", "value": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "inline": True},
            ],
            "timestamp": datetime.now().isoformat(),
        }

        if len(symbols) > 10:
            embed["fields"][0]["value"] += f" and {len(symbols) - 10} more"

        return {"embeds": [embed]}


class ElasticsearchSender(HTTPAPISender):
    """Sender for Elasticsearch."""

    def __init__(
        self, elasticsearch_url: str, index: str, username: Optional[str] = None, password: Optional[str] = None
    ):
        """
        Initialize Elasticsearch sender.

        Args:
            elasticsearch_url: Elasticsearch URL
            index: Index name
            username: Optional username for auth
            password: Optional password for auth
        """
        auth = {}
        if username and password:
            auth = {"username": username, "password": password, "basic_auth": True}

        super().__init__(
            name="elasticsearch_sender",
            endpoint_url=f"{elasticsearch_url}/{index}/_bulk",
            method="POST",
            headers={"Content-Type": "application/x-ndjson"},
            auth=auth,
        )
        self.index = index

    def _default_format(self, batch: List[StockData]) -> str:
        """Format data for Elasticsearch bulk API."""
        lines = []

        for stock_data in batch:
            action = {"index": {"_index": self.index}}
            lines.append(json.dumps(action))

            doc = stock_data.to_dict()
            lines.append(json.dumps(doc))

        return "\n".join(lines) + "\n"

    async def _send_batch(self, payload: str) -> bool:
        """Send batch to Elasticsearch using ndjson format."""
        try:
            async with self.session.post(self.endpoint_url, data=payload, headers=self.headers) as response:

                if response.status < 400:
                    result = await response.json()
                    if result.get("errors"):
                        self.logger.warning("Some documents failed to index", errors=result["errors"])
                    return True
                else:
                    error_text = await response.text()
                    self.logger.error("Elasticsearch request failed", status=response.status, error=error_text)
                    return False

        except Exception as e:
            self.logger.error("Failed to send to Elasticsearch", error=str(e))
            return False


class BatchAPISender:
    """Batch sender that coordinates multiple API senders."""

    def __init__(self):
        """Initialize batch API sender."""
        self.senders: Dict[str, BaseSender] = {}
        self.logger = logger.bind(component="batch_api_sender")

    def add_sender(self, name: str, sender: BaseSender):
        """Add a sender to the batch."""
        self.senders[name] = sender
        self.logger.info("Added sender", name=name, type=type(sender).__name__)

    async def send_to_all(self, data: List[StockData], **kwargs) -> Dict[str, SenderResult]:
        """Send data to all configured API senders."""
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
                self.logger.error("API sender failed", sender=sender_name, error=str(result))
                results[sender_name] = SenderResult(success=False, error=f"Sender failed: {str(result)}")
            else:
                results[sender_name] = result

                self.logger.info(
                    "API sender completed", sender=sender_name, success=result.success, sent_count=result.sent_count
                )

        return results
