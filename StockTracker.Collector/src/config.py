"""Configuration management for StockTracker Collector."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    env: str = Field(default="development", description="Environment")
    api_base_url: str = Field(default="http://localhost:8080", description="API base URL")
    redis_addr: str = Field(default="redis://localhost:6379", description="Redis address")
    log_level: str = Field(default="INFO", description="Log level")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
