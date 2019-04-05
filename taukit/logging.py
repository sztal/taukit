"""Logging utilities."""


formatter_default = {
    'format': "[%(asctime)s] %(name)s | %(levelname)s | %(funcName)s | %(message)s",
    'datefmt': "%Y-%m-%d %H:%M:%S"
}

message_handler = {
    'class': 'logging.StreamHandler',
    'level': 'INFO',
    'formatter': 'default',
    'stream': 'ext://sys.stdout'
}

rotating_file_handler = {
    'class': 'logging.handlers.RotatingFileHandler',
    'level': 'INFO',
    'formatter': 'default',
    'filename': None,
    'maxBytes': 1048576,
    'backupCount': 3
}

timed_rotating_file_handler = {
    'class': 'logging.handlers.TimedRotatingFileHandler',
    'level': 'INFO',
    'formatter': 'default',
    'filename': None,
    'when': 'D',
    'interval': 30,
    'backupCount': 3
}
