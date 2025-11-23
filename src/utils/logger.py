"""
File to handle logging for the project
"""

import logging
import sys
import os

from dotenv import load_dotenv
from pathlib import Path

# region ------------ Load env variables ------------
load_dotenv()

# default to INFO if not set
LOGGER_LEVEL: int = int(os.getenv("LOGGER_LEVEL", logging.INFO))
# endregion


# region ------------ Folder Setup ------------
LOG_DIR: Path = Path(__file__).parent.parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
# endregion


# region ------------ Logger Setup ------------
def setup_logger(name: str) -> logging.Logger:
    """
    Sets up the logger based on the name provided (best if it is the module name
    e.g. __name__)
    The logs go to both console and the log file

    ARGS:
        name (str): The name of the module

    RETURNS:
        logging.Logger: The logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOGGER_LEVEL)

    if not logger.handlers:
        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] %(name)s: %(message)s"
        )

        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File handler
        log_file = LOG_DIR / f"{name.replace('.', '_')}.log"
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


# endregion
