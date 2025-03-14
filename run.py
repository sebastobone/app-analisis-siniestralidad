import uvicorn
from src.logger_config import logger

if __name__ == "__main__":

    @logger.catch
    def main(dev: bool):
        uvicorn.run("src.app:app", reload=dev)

    main(False)
