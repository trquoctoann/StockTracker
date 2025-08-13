"""Smoke tests."""

from stocktracker_collector import __version__
from stocktracker_collector.config import settings


def test_smoke():
    """Basic smoke test."""
    assert True


def test_version():
    """Test version is available."""
    assert __version__ == "0.1.0"


def test_settings():
    """Test settings can be loaded."""
    assert settings.env is not None
    assert settings.api_base_url is not None


