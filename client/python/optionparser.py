import argparse
import os

from client.python import __version__
from sfdb_cli import LOG_LEVEL_MAP

SFDB_CLI_SERVER = u'SFDB_CLI_SERVER'


def create_parser():
    args_parser = argparse.ArgumentParser(
        prog=u'sfdb-cli',
        description=u'SFDB CLI. v.{}'.format(__version__)
    )

    args_parser.add_argument(
        u'-S', u'--server',
        dest='server',
        default=os.environ.get(SFDB_CLI_SERVER, None),
        metavar='',
        help=u'server:port instance to connect e.g. -S \'localhost:27910\''
    )

    args_parser.add_argument(
        u'-Q', u'--query',
        dest='query',
        default=False,
        required=True,
        metavar='',
        help=u'Executes a query outputting results to STDOUT and exits.'
    )

    args_parser.add_argument(
        u'--log_level',
        dest='log_level',
        default='INFO',
        metavar='',
        choices=list(LOG_LEVEL_MAP.keys()),
        help=u'Log Level.'
    )

    args_parser.add_argument(
        u'--log_file',
        dest='log_file',
        default=None,
        metavar='',
        help='Path to file to save logs.'
    )

    return args_parser
