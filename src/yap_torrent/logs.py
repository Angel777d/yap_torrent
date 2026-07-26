import logging
import sys
from pathlib import Path

_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def setup_logger(path: Path, logger, use_file=True, level=logging.DEBUG):
	logger.setLevel(level)
	if use_file:
		path.parent.mkdir(parents=True, exist_ok=True)
		handler = logging.FileHandler(path, mode='w')
	else:
		handler = logging.StreamHandler(sys.stdout)
	handler.setFormatter(logging.Formatter(_LOG_FORMAT))
	handler.setLevel(level)
	logger.addHandler(handler)
