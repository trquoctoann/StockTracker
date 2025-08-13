"""Main CLI application for StockTracker Collector."""

from __future__ import annotations

import typer

from .config import settings
from .logging import configure_logging, get_logger

app = typer.Typer(add_completion=False)

# Configure logging at module level
configure_logging()
logger = get_logger(__name__)


@app.command()
def health() -> None:
    """Health check command."""
    logger.info("Health check requested", env=settings.env)
    typer.echo("ok")


@app.command()
def version() -> None:
    """Show version information."""
    from . import __version__
    logger.info("Version requested", version=__version__)
    typer.echo(f"stocktracker-collector {__version__}")


def main() -> None:
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()


