import logging

_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


def setup_logger(logger, use_file=True, level=logging.DEBUG):
	logger.setLevel(level)

	handler = logging.FileHandler('torrent.log', mode='w') if use_file else logging.StreamHandler()
	handler.setFormatter(logging.Formatter(_LOG_FORMAT))
	handler.setLevel(level)
	logger.addHandler(handler)
