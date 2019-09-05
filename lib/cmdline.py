# encoding: utf8
import sys
from argparse import ArgumentParser

class CmdLineParser(ArgumentParser):
    """handle command line class"""
    def __init__(self):
        super(CmdLineParser, self).__init__()

    @staticmethod
    def cmd_parser(get_help=False):
        parser = ArgumentParser(prog=sys.argv[0], add_help=False)

        # Helper argument
        helper = parser.add_argument_group('helper arguments')
        helper.add_argument('-h', '--help', help='show this help message and exit', action='help')

        # Mandatory argument
        mandatory = parser.add_argument_group("Mandatory module",
                                              "arguments that have to be passed for the program to run")
        mandatory.add_argument('-i', '--csv-to-database', metavar='CSV-FILE-PATH', dest='csv_to_database',
                               help='insert data that extracted from csv file to MYSQL database')
        mandatory.add_argument('-t', '--txt-to-database', metavar='TXT-FOLDER-PATH', dest='txt_to_database',
                               help='insert data that extracted from txt file or contain txt files folder to MYSQL '
                                    'database')

        # Options module
        options = parser.add_argument_group("Options module", 'set up parameter to control this program or control log '
                                                              'or more details output')
        options.add_argument('-f', '--fast', action='store_true', dest='fast', default=False,
                             help="hit -f or --fast option to toggle fast mode with multiprocessing to handle big csv "
                                  "data or others big file, multiprocessing amount is that according to your computer "
                                  "CPU core amount. for example intel i7 CPU is 4 core so multiprocessing amount is 4")
        options.add_argument('-F', '--field-separator', metavar='FS', dest='separator', default=' ',
                             help='bond for -t options, use fs for the input field separator(the value of the FS '
                                  'predefined variable), this option like awk -F option to separate a line with the '
                                  'FS string you specified')
        options.add_argument('-c', '--column-list', nargs='+', metavar='COLUMN-LIST', dest='column_list',
                             help="bond for -t options, provide database table column name list, this option will "
                                  "create table in database and insert data into database according this column list")
        options.add_argument('--log-level', dest='log_level', metavar='INT', type=int, default=1,
                             help='set output log level, default level is 1. for high level can record more details log '
                                  'information and the biggest level is 3')
        options.add_argument("--skip-error", action='store_true', dest="skip_error",
                             help="toggle skip error mode that skip unimportant error information, for example insert "
                                  "error information. and if you want to look through more details information and "
                                  "more error logs, please go to logs/ folder and check it out.")

        # Database module
        database = parser.add_argument_group("Database module",
                                             "database operation option and whether turn on verbose mode to output more"
                                             " details info")
        database.add_argument('-D', '--database', metavar='DATABASE', dest='database_name',
                              help='designated database name custom in mysql database to storage data, default database '
                                   'name in lib/settings.py file without this option')
        database.add_argument('-T', '--table-name', metavar='TABLE-NAME', dest='table_name',
                             help='designated table name in mysql database to storage data, default table name in '
                                  'lib/settings.py file without this option')
        database.add_argument('-m', '--mysql-command', metavar='DATABASE-CMD', dest='mysql_command',
                              help='providing a database interface, it will show command result when you type database '
                                   'command with -m option, for example: -m "show databases"')
        database.add_argument("--drop-table", dest="drop_table", metavar='TABLE-NAME',
                              help="input table name you want to drop and this option is very dangerous so more "
                                   "carefully to take this option")
        database.add_argument('-s', '--show-config', action='store_true',
                              help="show database config from lib/settings.py file and you can modify this config for "
                                   "yourself database")

        # Clean module
        clean_cache = parser.add_argument_group('Clean module', 'clean redundant cache')
        clean_cache.add_argument('--clean-cache', action='store_true', dest='clean_cache', help='clean log files')

        opts = parser.parse_args()

        if get_help:
            return parser.print_help()
        else:
            return opts