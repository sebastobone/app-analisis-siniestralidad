from loguru import logger

logger.add(
    "logs/plantilla_seguimiento.log",
    rotation="10MB",
    retention="10 days",
    compression="zip",
    level="INFO",
    format="{time} | {level} | {message}",
)

__all__ = ["logger"]