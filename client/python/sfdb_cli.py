import os
import sys
import logging

from driver.python.connection import connect

LOG_LEVEL_MAP = {
    'ERROR': logging.ERROR,
    'WARN': logging.WARN,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
}


class SfdbCli(object):

    default_prompt = r'\d'

    def __init__(self, options):
        self.init_logging(options.log_level, options.log_file)
        self.logger = logging.getLogger("sfdbcli.SfdbCli")
        self.server = options.server
        self.query = options.query

    def init_logging(self, log_level, log_file=None):
        formatter = logging.Formatter(
            '%(asctime)s (%(process)d/%(threadName)s) '
            '%(name)s %(levelname)s - %(message)s')
        root_logger = logging.getLogger('')
        root_logger.setLevel(LOG_LEVEL_MAP[log_level])

        console_handler = logging.StreamHandler()
        console_handler.setLevel(LOG_LEVEL_MAP[log_level])
        console_handler.setFormatter(formatter)

        root_logger.addHandler(console_handler)
        if log_file:
            logging_path, log_filename = os.path.split(log_file)
            if os.path.isdir(logging_path) and log_filename:
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(LOG_LEVEL_MAP[log_level])
                file_handler.setFormatter(formatter)

                root_logger.addHandler(file_handler)
                root_logger.info(f'Initalized logging in: {log_file}')
            else:
                root_logger.error(f'Invalid logging path: {logging_path}')
        root_logger.debug('Initialized sfdbcli logging.')

    def connect_to_database(self):
        self.logger.debug("Connecting to database...")
        self.conn = connect('/'.join([self.server, 'Test']))

    def shutdown(self):
        self.logger.debug("Shutting down...")
        self.conn = None

    def execute_query(self, query_text):
        """Process a query string and outputs to STDOUT/file"""
        self.logger.debug(f'Executing query: \"{query_text}\"')
        cursor = self.conn.cursor()
        cursor.execute(query_text)
        return cursor
