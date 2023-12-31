# -*- coding: utf-8 -*-

import sys
import os
import logging.config
from traceback import format_exception


Silent = False
Debug = False


class MaybeSilentHandler(logging.StreamHandler):
    def emit(self, record):
        if record.levelno == logging.INFO and Silent:
            return
        if record.levelno == logging.DEBUG and not Debug:
            return
        if record.levelno == logging.ERROR:
            etype, evalue, etraceback = sys.exc_info()
            if etype is not None:
                record.msg += '\n'.join([''] + format_exception(etype, evalue, etraceback))
        super().emit(record)


# pylint: disable=protected-access
class Formatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            # pylint: disable=protected-access
            self._style._fmt = "%(module)s:: %(message)s"
        else:
            self._style._fmt = '%(levelname)s::%(module)s|%(lineno)s:: %(message)s'
        return super().format(record)


def configure_logging(debug: bool = False, silent: bool = False, logs_dir: str = 'logs'):
    global Debug
    Debug = debug

    global Silent
    Silent = silent

    log_file_path = ''

    # Check if the application is running as a Lambda function. If so
    # the application does not have access to a file system, and logs
    # are instead sent to CloudWatch.
    is_lambda = os.getenv('AWS_EXECUTION_ENV', '').startswith('AWS_Lambda_')
    if not is_lambda:
        # Create a logs directory if it doesn't already exist
        os.makedirs(logs_dir, exist_ok=True)
        log_file_path = os.path.join(logs_dir, 'aword.log')

    LOGGING_CONFIG = {
        'version': 1,
        'loggers': {
            'aword': {
                'level': 'DEBUG',
                'propagate': False,
                'handlers': ['maybe_console_handler']
                + (['rotating_file_handler'] if log_file_path else []),
            }
        },
        'handlers': {
            'maybe_console_handler': {
                'level': 'DEBUG',
                'formatter': 'console',
                'class': 'aword.logger.MaybeSilentHandler',
                'stream': 'ext://sys.stdout',
            },
            'rotating_file_handler': {
                'level': 'DEBUG',
                'formatter': 'file',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': log_file_path,
                'mode': 'a',
            },
        },
        'formatters': {
            'console': {'()': Formatter},
            'file': {
                'format': ('%(asctime)s::%(levelname)s::' '%(module)s|%(lineno)s:: %(message)s'),
                'datefmt': '%Y-%m-%dT%H:%M:%S',
            },
            'simple_file': {'format': '%(asctime)s:: %(message)s', 'datefmt': '%Y-%m-%dT%H:%M:%S'},
        },
    }

    logging.config.dictConfig(LOGGING_CONFIG)
