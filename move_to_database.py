# encoding: utf8
#!/usr/bin/env python3
import csv
import os
import sys
import time
import pymysql
from termcolor import colored
from argparse import ArgumentParser
from lib.settings import DATABASE, DATABASE_USER, DATABASE_PASSWORD, LOG_PATH, SPLIT_PATH, TABLE_NAME
from lib.Logger import Logger
from lib.progress_bar import ProgressBar
from lib.print_formatter import ColorFormatter
from lib.split_file import split_file
import subprocess
import platform
from multiprocessing import Process


class Database(object):
    def __init__(self, host, database, logger):
        self.host = host
        self.database=database
        self.logger = logger

        self.conn = pymysql.connect(host=self.host, user=DATABASE_USER, password=DATABASE_PASSWORD,
                                    database=self.database, charset='utf8')
        # logger
        self.debug_logger = self.logger.get_logger('debug_logger')
        self.error_logger = self.logger.get_logger('error_logger')
        # 初始化数据库, 返回 cursor 指针
        self.cursor = self.init_database()
        # calculate successful insert sql count
        self.insert_success_count = 0
        # calculate failed insert sql count
        self.insert_failed_count = 0
        # calculate all of insert sql count
        self.insert_total_count = 0

    def init_database(self):
        """Return cursor init database"""
        cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
        self.debug_logger.debug('init database')
        return cursor

    def create_table(self, table_name, table_column_list):
        try:
            columns = ["{} {} {} {}".format(col_name, 'varchar(200)', 'null', 'default NULL') for col_name in table_column_list]
            # columes_str data like this ->
            # id varchar(200) null default NULL, name varchar(200) null default NULL, age varchar(200) null default NULL
            columns_str = ", ".join(columns)

            # write data in debug log
            self.debug_logger.debug('create table {} ({})'.format(table_name, columns_str))
            # 创建一个自增长的表
            # create_table_sql = 'create table {} (id int PRIMARY KEY AUTO_INCREMENT, {})'.format(table_name, columns_str)
            # 不使用自增长索引
            create_table_sql = 'create table {} ({})'.format(table_name, columns_str)
            self.cursor.execute(create_table_sql)
            # 执行事务
            commit_result = self.execute_commit()
            if commit_result:
                self.debug_logger.info('create table {} ({}) successful'.format(table_name, columns_str))
                return 'True'
            else:
                self.debug_logger.error('create table {} ({}) failed'.format(table_name, columns_str))
                self.error_logger.error('create table {} ({}) failed'.format(table_name, columns_str))
                return 'False'

        except pymysql.err.ProgrammingError as e:
            ColorFormatter.error('ProgrammingError: {}'.format(e))
            self.debug_logger.error('ProgrammingError: {}'.format(e))
            self.error_logger.error('ProgrammingError: {}'.format(e))
            return 'False'

        except pymysql.err.InternalError as e:
            ColorFormatter.error('InternalError: {}'.format(e))
            self.debug_logger.error('InternalError: {}'.format(e))
            self.error_logger.error('InternalError: {}'.format(e))
            return str(e)

        except Exception as e:
            ColorFormatter.error('Unknow error: {}'.format(e))
            ColorFormatter.fatal('Please contact author}')
            self.debug_logger.error('Unknow error: {}'.format(e))
            self.error_logger.error('Unknow error: {}'.format(e))
            return 'False'

    def insert_data(self, table_name, column_list, data_list, skip_error=False):
        """
        insert data into table 这里只执行 sql 插入语句, 但是不提交事务, 提交事务需要 调用 self.execute_commit()

        :param table_name: table name
        :param column_list: all of database table column
        :param data_list: data list
        :param skip_error: skip unimportant insert error information, default False
        :return:
        """

        insert_data_sql = 'insert into {} ({}) values ({})'.format(table_name, column_list, data_list)
        try:
            self.cursor.execute(insert_data_sql)
            self.insert_success_count += 1

        except pymysql.err.ProgrammingError as e:
            if skip_error:
                ColorFormatter.error('ProgrammingError: {}'.format(e))
            self.debug_logger.error('ProgrammingError: {}'.format(e))
            self.error_logger.error('ProgrammingError: {}'.format(e))
            self.insert_failed_count += 1

        except pymysql.err.InternalError as e:
            if skip_error:
                # 数据插入 内部错误, 将写入 error_log file
                ColorFormatter.error('InternalError: {}'.format(e))
            self.debug_logger.error("InternalError: insert data sql is: {}".format(insert_data_sql))
            self.error_logger.error("InternalError: insert data sql is: {}".format(insert_data_sql))
            self.insert_failed_count += 1

        except Exception as e:
            ColorFormatter.error('Unknow error: {}'.format(e))
            ColorFormatter.fatal('Please contact author')
            # 写入日志文件
            self.debug_logger.error("Unknow error: {}".format(e))
            self.error_logger.error("Unknow error: {}".format(e))
            self.insert_failed_count += 1

        self.insert_total_count += 1

    def execute_commit(self):
        # 执行事务
        try:
            self.conn.commit()
            self.debug_logger.debug('execute sql commit')
            return True
        except:
            self.debug_logger.error('execute sql commit failed')
            self.error_logger.error('execute sql commit failed')
            return False

    def drop_table(self, table_name):
        """for damage database, more dangerous operation, be carefully!"""

        try:
            drop_table_sql = "drop table {}".format(table_name)
            self.cursor.execute(drop_table_sql)
            self.conn.commit()
            return True

        except pymysql.err.InternalError as e:
            ColorFormatter.error('InternalError: {}'.format(e))
            self.debug_logger.error('InternalError: {}'.format(e))
            self.error_logger.error('InternalError: {}'.format(e))
            return False

        except Exception as e:
            ColorFormatter.error('Unknow error: {}'.format(e))
            ColorFormatter.fatal('Please contact author')
            self.debug_logger.error('Unknow error: {}'.format(e))
            self.error_logger.error('Unknow error: {}'.format(e))
            return True


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

        # Options module
        options = parser.add_argument_group("Options module", 'set up parameter to control this program or control log '
                                                              'or more details output')
        options.add_argument('-f', '--fast', action='store_true', dest='fast', default=False,
                             help="hit -f or --fast option to toggle fast mode with multiprocessing to handle big csv "
                                  "data or others big file, multiprocessing amount is that according to your computer "
                                  "CPU core amount. for example intel i7 CPU is 4 core so multiprocessing amount is 4")
        options.add_argument('--log-level', dest='log_level', metavar='INT', type=int, default=1,
                             help='set output log level, default level is 1. for high level can record more details log '
                                  'information and the biggest level is 3')
        options.add_argument("--skip-error", action='store_false', dest="skip_error",
                             help="toggle skip error mode that skip unimportant error information, for example insert "
                                  "error information. and if you want to look through more details information and "
                                  "more error logs, please go to logs/ folder and check it out.")

        # Database module
        database = parser.add_argument_group("Database module",
                                             "database operation option and whether turn on verbose mode to output more"
                                             " details info")
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


def banner():
    print(colored("""
                                     ______        ____
     /'\_/`\                        /\__  _\      /\  _`\\
    /\      \    ___   __  __     __\/_/\ \/   ___\ \ \/\ \\
    \ \ \__\ \  / __`\/\ \/\ \  /'__`\ \ \ \  / __`\ \ \ \ \\
     \ \ \_/\ \/\ \L\ \ \ \_/ |/\  __/  \ \ \/\ \L\ \ \ \_\ \\
      \ \_\\\\ \_\ \____/\ \___/ \ \____\  \ \_\ \____/\ \____/
       \/_/ \/_/\/___/  \/__/   \/____/   \/_/\/___/  \/___/
            __             __
           /\ \__         /\ \\
       __  \ \ ,_\    __  \ \ \____     __      ____     __
     /'__`\ \ \ \/  /'__`\ \ \ '__`\  /'__`\   /',__\  /'__`\\
    /\ \L\.\_\ \ \_/\ \L\.\_\ \ \L\ \/\ \L\.\_/\__, `\/\  __/
    \ \__/.\_\\\\ \__\ \__/.\_\\\\ \_,__/\ \__/.\_\/\____/\ \____\\
     \/__/\/_/ \/__/\/__/\/_/ \/___/  \/__/\/_/\/___/  \/____/

                                             --Author: Lunpopo
    """, 'cyan'))


def handle_data_to_database(suffix_name, database_obj, table_name, debug_logger, error_logger, skip_error=False):
    one_split_file = os.path.join(SPLIT_PATH, 'split_file_{}.txt'.format(str(suffix_name)))
    record_line_file = os.path.join(SPLIT_PATH, 'record_line_{}.txt'.format(str(suffix_name)))
    with open(one_split_file, 'r') as r:
        line = csv.reader(r)
        columns_str = ''

        # 计算 csv 文件的行
        cmd = "wc -l %s | awk '{print $1}'" % one_split_file
        debug_logger.debug('开始运行 {} 命令'.format(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        (stdout, stderr) = p.communicate()
        return_code = p.returncode
        if return_code != 0:
            ColorFormatter.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            debug_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            error_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            # 异常退出
            sys.exit(1)
        else:
            ColorFormatter.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(one_split_file, str(stdout.strip())))
            debug_logger.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(one_split_file, str(stdout.strip())))

        count = 1
        for all_cols in line:
            if suffix_name == 1:
                if count == 1:
                    # 第一个分割文件的第一行，获取所有数据的列名, 然后创建数据库表
                    columns_str =  ", ".join(all_cols)

                    debug_logger.debug('create table {}'.format(table_name))
                    create_result = database_obj.create_table(table_name=table_name, table_column_list=all_cols)
                    if create_result == 'True':
                        debug_logger.info('create table {} successful'.format(table_name))
                        ColorFormatter.success('创建表 "{}" 成功'.format(table_name))
                    elif 'already exists' or 'Duplicate column name' in create_result:
                        debug_logger.warning('table "{}" has been existed'.format(table_name))
                        ColorFormatter.info('表 "{}" 已经存在'.format(table_name))
                    else:
                        debug_logger.error('create table "{}" failed'.format(table_name))
                        error_logger.error('create table "{}" failed'.format(table_name))
                        ColorFormatter.error('创建表 "{}" 失败'.format(table_name))
                else:
                    # 将每一列的数据拼成一行
                    data_list = ', '.join(['"{}"'.format(_) for _ in all_cols])
                    database_obj.insert_data(table_name=table_name, column_list=columns_str,
                                             data_list=data_list, skip_error=skip_error)
            else:
                # 将每一列的数据拼成一行
                data_list = ', '.join(['"{}"'.format(_) for _ in all_cols])
                database_obj.insert_data(table_name=table_name, column_list=columns_str,
                                         data_list=data_list, skip_error=skip_error)

            if count % 50000 == 0:
                # 每次 5万 5万 的存
                with open(record_line_file, 'w') as f:
                    f.write("{}\r\n".format(str(count)))

            count += + 1

        with open(record_line_file, 'w') as f:
            f.write("{}\r\n".format(str(count-1)))

        # 事务提交, 插入数据
        debug_logger.debug('execute insert sql commit')
        database_obj.execute_commit()
        debug_logger.info('execute insert sql commit successful')

    r.close()
    debug_logger.info('insert data to database done')


def main():
    banner()
    opts = CmdLineParser().cmd_parser()

    python_version = platform.python_version()
    if not python_version.startswith('3'):
        ColorFormatter.fatal('此脚本只能运行在 Python3 之中')
        sys.exit(1)

    if opts.show_config:
        space = "-"
        line_len_list = []
        line_1 = "| # ['database']"
        line_2 = "| DATABASE_USER = '{}'".format(DATABASE_USER)
        line_3 = "| DATABASE_PASSWORD = '{}'".format(DATABASE_PASSWORD)
        line_4 = "| DATABASE = '{}'".format(DATABASE)
        line_5 = "| TABLE_NAME = '{}'".format(TABLE_NAME)

        line_len_list.append(len(line_2))
        line_len_list.append(len(line_3))
        line_len_list.append(len(line_4))
        line_len_list.append(len(line_5))
        line_len_list.sort(reverse=True)
        longest_line = line_len_list[0]

        print(colored('Show database config', 'white'))

        print(colored(space * (longest_line+2), 'white'))
        chajia = longest_line - len(line_1)
        print(colored("{}{} |".format(line_1, chajia*' '), 'white'))
        chajia = longest_line - len(line_2)
        print(colored("{}{} |".format(line_2, chajia*' '), 'white'))
        chajia = longest_line - len(line_3)
        print(colored("{}{} |".format(line_3, chajia*' '), 'white'))
        chajia = longest_line - len(line_4)
        print(colored("{}{} |".format(line_4, chajia*' '), 'white'))
        chajia = longest_line - len(line_5)
        print(colored("{}{} |".format(line_5, chajia*' '), 'white'))
        print(colored(space * (longest_line+2), 'white'))

        # 安全退出程序
        sys.exit(0)

    # control log level, default is 1
    log_level = str(opts.log_level)
    if log_level == '1':
        log_level = 'WARNING'
    elif log_level == '2':
        log_level = 'INFO'
    elif log_level == '3':
        log_level = 'DEBUG'
    else:
        log_level = 'DEBUG'

    # setting Logger object
    logger = Logger(log_level=log_level)
    # debug.log 应该包含所有的日志信息, 如果命令行没有指定 log_level 为3 的话, debug信息 不会出现在 debug.log 中
    debug_logger = logger.get_logger(logger_name='debug_logger')
    # error.log 只包含错误信息
    error_logger = logger.get_logger(logger_name='error_logger')

    # 初始化数据库
    try:
        database = Database(host='127.0.0.1', database=DATABASE, logger=logger)
    except pymysql.err.InternalError as e:
        ColorFormatter.error('InternalError: {}'.format(e))
        debug_logger.error('InternalError: {}'.format(e))
        error_logger.error('InternalError: {}'.format(e))
        if 'Unknown database' in str(e):
            ColorFormatter.fatal('Please open the lib/settings.py file and move to database section content, modify '
                                 'DATABASE parameter and try again, for example see below: ')
            error_string = """
----------------------------------------
| # ['database']                       |
| DATABASE_USER = 'your database user' |
| DATABASE_PASSWORD = 'your passcode'  |
| DATABASE = 'testdb'                  |
| TABLE_NAME = 'test_table'            |
----------------------------------------
            """
            ColorFormatter.fatal(error_string)
        sys.exit(1)
    except Exception as e:
        ColorFormatter.error('Unknow Error: {}'.format(e))
        debug_logger.error('Unknow Error: {}'.format(e))
        error_logger.error('Unknow Error: {}'.format(e))
        sys.exit(1)

    # 运行程序
    try:
        if not len(sys.argv[1:]):
            ColorFormatter.fatal("You failed to provide an option, redirecting to help menu")
            debug_logger.debug('You failed to provide an option, redirecting to help menu')
            # 停顿2秒之后再显示 help banner
            time.sleep(2)
            print()
            CmdLineParser().cmd_parser(get_help=True)

        else:
            if opts.drop_table:
                prompt_result = ColorFormatter.prompt('Are you sure to drop "{}" table?(y/N)'.format(opts.drop_table), opts='y/n')
                debug_logger.debug('type --drop-table option')
                if prompt_result:
                    # result = db.create_table('大爷', ['id', 'name', 'age'])
                    result = database.drop_table(opts.drop_table)
                    debug_logger.warning('input y')
                    if result:
                        ColorFormatter.info('Drop table {} successful'.format(opts.drop_table))
                        debug_logger.warning('Drop table {} successful'.format(opts.drop_table))
                    else:
                        ColorFormatter.error('Drop table {} failed'.format(opts.drop_table))
                        error_logger.error('Drop table {} failed'.format(opts.drop_table))
                        debug_logger.error('Drop table {} failed'.format(opts.drop_table))

                # 执行完 drop table 就正常退出程序
                sys.exit(0)

            if opts.csv_to_database:
                csv_file_path = opts.csv_to_database

                if not os.path.isfile(csv_file_path):
                    # 不存在 csv 文件
                    debug_logger.warning('File "{}" not exists'.format(csv_file_path))
                    ColorFormatter.fatal('File "{}" not exists'.format(csv_file_path))
                    time.sleep(1)
                    ColorFormatter.fatal('Program exit')
                    debug_logger.warning('Program exist')
                    sys.exit(1)

                else:
                    if opts.fast:
                        start_time = time.time()

                        # 使用多核前, 将大文件进行分割, 分割成 4 份, 再加上不足的那一份
                        ColorFormatter.info('启用多进程导入数据库')
                        ColorFormatter.info('开始分割原数据文件')

                        # 这里需要再分割前 删除 split 目录下的所有文件
                        cmd = 'rm -rf split && mkdir split'
                        debug_logger.debug('开始运行 {} 命令'.format(cmd))
                        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
                        (stdout, stderr) = p.communicate()
                        return_code = p.returncode
                        if return_code != 0:
                            ColorFormatter.error('删除split文件时出错, 出错信息为: {}'.format(stderr.strip()))
                            debug_logger.error('删除split文件时出错, 出错信息为: {}'.format(stderr.strip()))
                            error_logger.error('删除split文件时出错, 出错信息为: {}'.format(stderr.strip()))
                            # 异常退出
                            sys.exit(1)
                        else:
                            ColorFormatter.success('初始化split文件夹成功')
                            debug_logger.debug('初始化split文件夹成功, 返回码: {}, 输出为: {}'.format(
                                return_code, stdout))
                            debug_logger.info('初始化split文件夹成功')

                        # 开始分割
                        split_result = split_file(csv_file_path, log_level)
                        if split_result:
                            ColorFormatter.success('原数据文件分割完成')
                            debug_logger.info('原数据文件分割完成')
                        else:
                            ColorFormatter.error('原数据文件分割失败')
                            ColorFormatter.fatal('异常, 退出程序')
                            debug_logger.error('原数据文件分割失败')
                            error_logger.error('原数据文件分割失败')
                            sys.exit(1)
                        # 分割完成

                        # 多进程, 使用多核一起干
                        table_name = TABLE_NAME
                        process_list = []

                        for i in range(1, os.cpu_count()+1):
                            process = Process(target=handle_data_to_database,
                                              args=(i, database, table_name, debug_logger, error_logger, opts.skip_error, ))
                            process_list.append(process)
                            process.start()


                        # 计算 csv 文件的总行
                        cmd = "wc -l split/split_file_* | grep total | awk '{print $1}'"
                        debug_logger.debug('开始运行 {} 命令'.format(cmd))
                        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
                        (stdout, stderr) = p.communicate()
                        try:
                            total_lines = int(stdout.strip())
                        except:
                            total_lines = 0
                        return_code = p.returncode
                        if return_code != 0:
                            ColorFormatter.error('计算文件 "{}" 总行数 时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            debug_logger.error('计算文件 "{}" 总行数 时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            error_logger.error('计算文件 "{}" 总行数 时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            ColorFormatter.fatal('Please contact author or look through logs/error.log file to examine error place')
                            # 异常退出
                            sys.exit(1)
                        else:
                            ColorFormatter.info('计算文件 "{}" 总行数结束, 一共 {} 行'.format(csv_file_path, str(total_lines)))
                            debug_logger.debug('计算文件 "{}" 总行数命令结果, 返回码: {}, 输出为: {}'.format(
                                csv_file_path, return_code, str(total_lines)))
                            debug_logger.info('计算需要分割文件 "{}" 行数结束, 一共 {} 行'.format(csv_file_path, str(total_lines)))

                        # 进度条 对象
                        progress_bar = ProgressBar(total_line=total_lines, description='正在插入数据: ')
                        while True:
                            # 每 1 秒读一次总数文件
                            time.sleep(2)
                            all_progress = 0
                            for suffix_name in range(1, os.cpu_count()+1):
                                record_line_file = os.path.join(SPLIT_PATH, 'record_line_{}.txt'.format(str(suffix_name)))
                                try:
                                    with open(record_line_file, 'r') as r:
                                        per_line = r.readline()
                                    all_progress += int(per_line)
                                except:
                                    # 有可能开始还没有读到文件
                                    pass

                            # 打印进度条
                            progress_bar.handle_multiprocessing_progress(current_progress=all_progress)
                            if all_progress >= total_lines:
                                break

                        for _ in process_list:
                            _.join()

                        end_time = time.time()
                        print(colored('总共用时: {}'.format(end_time - start_time), 'white'))

                        # 清除 split 文件夹
                        cmd = "rm -rf split"
                        debug_logger.debug('开始运行 {} 命令'.format(cmd))
                        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
                        (stdout, stderr) = p.communicate()
                        return_code = p.returncode
                        if return_code != 0:
                            ColorFormatter.error('清除 split 文件夹时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            debug_logger.error('清除 split 文件夹时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            error_logger.error('清除 split 文件夹时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                            ColorFormatter.fatal('Please contact author or look through logs/error.log file to examine error place')
                            # 异常退出
                            sys.exit(1)
                        else:
                            ColorFormatter.info('清除 split 文件夹成功')
                            debug_logger.debug('清除 split 文件夹成功')

                        ColorFormatter.success('插入数据完成')
                        debug_logger.info('insert data to database done')

                        # 显示汇总信息
                        # 多进程无法显示 失败 和 成功 的条数, 暂时丢着吧

                    else:
                        # 单进程, 速度很慢
                        start_time = time.time()
                        table_name = TABLE_NAME

                        with open(csv_file_path, 'r') as r:
                            line = csv.reader(r)
                            csv_file_path = '\ '.join(csv_file_path.split())
                            columns_str = ''

                            ColorFormatter.info('开始计算csv文件 {} 的行数'.format(csv_file_path))
                            debug_logger.info('开始计算csv文件 {} 的行数'.format(csv_file_path))
                            # 计算 csv 文件的行
                            cmd = "wc -l %s | awk '{print $1}'" % csv_file_path
                            debug_logger.debug('开始运行 {} 命令'.format(cmd))
                            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
                            (stdout, stderr) = p.communicate()
                            return_code = p.returncode
                            if return_code != 0:
                                ColorFormatter.error('计算csv文件 "{}" 行数时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                                debug_logger.error('计算csv文件 "{}" 行数时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                                error_logger.error('计算csv文件 "{}" 行数时出错, 出错信息为: {}'.format(csv_file_path, stderr.strip()))
                                # 异常退出
                                sys.exit(1)
                            else:
                                ColorFormatter.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(csv_file_path, stdout.strip()))
                                debug_logger.debug('计算 "{}" 文件行数命令结果, 返回码: {}, 输出为: {}'.format(
                                    csv_file_path, return_code, stdout.strip()))
                                csv_file_line = int(stdout.strip())
                                debug_logger.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(csv_file_path, csv_file_line))

                            # 进度条 对象
                            progress_bar = ProgressBar(total_line=csv_file_line, description='正在插入数据: ')

                            count = 1
                            for all_cols in line:
                                if count == 1:
                                    # 第一行，获取所有数据的列名, 然后创建数据库表
                                    columns_str =  ", ".join(all_cols)

                                    debug_logger.debug('create table {}'.format(table_name))
                                    create_result = database.create_table(table_name=table_name, table_column_list=all_cols)
                                    if create_result == 'True':
                                        debug_logger.info('create table {} successful'.format(table_name))
                                        ColorFormatter.success('创建表 "{}" 成功'.format(table_name))
                                    elif 'already exists' or 'Duplicate column name' in create_result:
                                        debug_logger.warning('table "{}" has been existed'.format(table_name))
                                        ColorFormatter.info('表 "{}" 已经存在'.format(table_name))
                                    else:
                                        debug_logger.error('create table "{}" failed'.format(table_name))
                                        error_logger.error('create table "{}" failed'.format(table_name))
                                        ColorFormatter.error('创建表 "{}" 失败'.format(table_name))
                                else:
                                    # 将每一列的数据拼成一行
                                    data_list = ', '.join(['"{}"'.format(_) for _ in all_cols])
                                    database.insert_data(table_name=table_name, column_list=columns_str,
                                                         data_list=data_list, skip_error=opts.skip_error)

                                if count == 3000000:
                                    # 先插入300万条
                                    break

                                progress_bar.handle_progress()
                                count += + 1

                            # 事务提交, 插入数据
                            debug_logger.debug('execute insert sql commit')
                            database.execute_commit()
                            debug_logger.info('execute insert sql commit successful')

                            # 输出插入数据总共用时
                            end_time = time.time()
                            print(colored('总共用时: {:.2f}秒'.format(end_time - start_time), 'white'))

                        r.close()

                        ColorFormatter.success('插入数据完成')
                        debug_logger.info('insert data to database done')

                        # 显示汇总信息
                        ColorFormatter.info('总共插入 "{}" 条数据, 成功 "{}" 条, 失败 "{}" 条'.format(
                            database.insert_total_count,
                            database.insert_success_count,
                            database.insert_failed_count
                        ))
                        debug_logger.info('总共插入 "{}" 条数据, 成功 "{}" 条, 失败 "{}" 条'.format(
                            database.insert_total_count,
                            database.insert_success_count,
                            database.insert_failed_count
                        ))

                # 安全退出
                sys.exit(0)

            if opts.clean_cache:
                ColorFormatter.warning('Clean log cache')
                debug_logger.warning('Clean log cache')
                try:
                    os.remove(os.path.join(LOG_PATH, 'error.log'))
                    os.remove(os.path.join(LOG_PATH, 'debug.log'))
                except:
                    pass
                ColorFormatter.success('Clean log cache successful')
                debug_logger.info('Clean log cache success')

                # 执行完 clean log cache 就正常的退出程序
                sys.exit(0)

    except KeyboardInterrupt as e:
        ColorFormatter.fatal('user abort')
        debug_logger.error('user abort: {}'.format(e))
        error_logger.error('user abort: {}'.format(e))

    except Exception as e:
        ColorFormatter.error('Unknow error')
        ColorFormatter.fatal('Please contact author')
        debug_logger.error('Unknow error: {}'.format(e))
        error_logger.error('Unknow error: {}'.format(e))


if __name__ == '__main__':
    main()
