# encoding: utf8
#!/usr/bin/env python3
import os
import logging.config
from lib.settings import LOG_PATH


class Logger(object):
    """
    Handle Logger class, include get_logger get_logging_config function
    and include logger write log to file function, for example: info,
    debug, warning, error and critical etc write function.
    """

    def __init__(self, log_level):
        """
        setting logger, logger 默认一共设有 5 个日志等级
        1. DEBUG
        2. INFO
        4. WARNING
        5. ERROR
        6. CRITICAL

        DEBUG < INFO < WARNING < ERROR < CRITICAL

        如果设置了等级为 WARNING, 那么调用 debug info 写的日志便不会出现在日志文件中

        使用方法:
        '''
        先创建Logger对象, 例如 logger = Logger(log_level='DEBUG'), 然后
        获取 logger: debug_logger = logger.get_logger(logger_name='debug_logger')
        然后就可以使用 debug_logger 了: debug_logger.info('this is INFO information')
        '''

        :param log_level: 设置入 log 文件的等级
        """

        self.log_level = log_level
        self.logger = ''
        self.logger_init()

    def get_logging_config(self):
        """get logging dict config, return a dict object"""
        # logging dict config
        logging_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'loggers': {
                # custom logger
                'debug_logger': {
                    'level': self.log_level,
                    'handlers': ['debug_handler'],
                    'formatter': 'standard',
                    'propagate': False
                },
                'error_logger': {
                    'level': 'ERROR',
                    'handlers': ['error_handler'],
                    'formatter': 'standard',
                    'propagate': False
                },
            },
            'handlers': {
                # be used for file output
                'debug_handler': {
                    # 只处理 DEBUG 级别以上的
                    'level': 'DEBUG',
                    # 使用轮循日志
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(LOG_PATH, 'debug.log'),
                    'maxBytes': 1024*1024*500,
                    'formatter': 'standard',
                },
                'error_handler': {
                    # 只处理 DEBUG 级别以上的
                    'level': 'DEBUG',
                    # 使用轮循日志
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': os.path.join(LOG_PATH, 'error.log'),
                    'maxBytes': 1024*1024*500,
                    'formatter': 'standard',
                },
            },
            'formatters': {
                'standard': {
                    'format': '%(asctime)s [%(levelname)s] %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
                },
            }
        }
        return logging_config

    def get_logger(self, logger_name):
        """
        get logger for further using, for example:
        Logger_object.get_logger(logger_name=LOGGER_NAME)
        """
        self.logger = logging.getLogger(logger_name)
        return self.logger

    def logger_init(self):
        """
        logger initialization, get logging_config and use logging_config,
        no return
        """
        if not os.path.exists(LOG_PATH):
            os.mkdir(LOG_PATH)
        logging_config = self.get_logging_config()
        logging.config.dictConfig(logging_config)

    def debug(self, string):
        """logging debug function"""
        self.logger.debug(string)

    def info(self, string):
        """logging info function"""
        self.logger.info(string)

    def warning(self, string):
        """logging warning function"""
        self.logger.warning(string)

    def error(self, string):
        """logging error function"""
        self.logger.error(string)

    def critical(self, string):
        """logging critical function"""
        self.logger.critical(string)
