from loguru import logger

dev = logger.add(
    "logs/log_dev.log",
    rotation="10MB",
    retention="10 days",
    compression="zip",
    level="TRACE",
    diagnose=True,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)

prod = logger.add(
    "logs/log_prod.log",
    rotation="10MB",
    retention="10 days",
    compression="zip",
    level="INFO",
    diagnose=False,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)

__all__ = ["logger"]
