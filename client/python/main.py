import sys
from client.python.optionparser import create_parser
from client.python.sfdb_cli import SfdbCli


def run_cli_with(options):
    if options.query:
        options.interactive_mode = False
    sfdbcli = SfdbCli(options)
    try:
        sfdbcli.connect_to_database()
        cursor = sfdbcli.execute_query(str(options.query))
        if cursor.json:
            print(cursor.json)
        print(f'Rows affected: {cursor.rowcount}', file=sys.stderr)
    finally:
        sfdbcli.shutdown()


def main():
    sfdbcli_options_parser = create_parser()
    sfdbcli_options = sfdbcli_options_parser.parse_args(sys.argv[1:])
    run_cli_with(sfdbcli_options)


if __name__ == '__main__':
    main()
