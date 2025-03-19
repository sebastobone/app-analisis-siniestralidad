import uvicorn
from src.logger_config import logger

if __name__ == "__main__":

    def main(dev: bool):
        try:
            uvicorn.run("src.app:app", reload=dev)
        except Exception as e:
            logger.exception(str(e))
            raise

    main(False)
