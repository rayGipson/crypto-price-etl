import logging
import structlog
from structlog.typing import FilteringBoundLogger
from .config import config

def configure_logging() -> FilteringBoundLogger:
    """
    Configure structured logging with JSON output
    
    Returns:
        Configured structlog logger
    """
    # Configure standard library logging first
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, config.logging.level.upper())
    )

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Return a structlog logger
    return structlog.get_logger()

# Create a global logger instance
logger = configure_logging()