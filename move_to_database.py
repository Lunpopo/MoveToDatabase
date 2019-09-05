# encoding: utf8
#!/usr/bin/env python3
import csv
import os
import sys
import time
import pymysql
from termcolor import colored
from lib.settings import DATABASE, DATABASE_USER, DATABASE_PASSWORD, LOG_PATH, SPLIT_PATH, TABLE_NAME
from lib.Logger import Logger
from lib.progress_bar import ProgressBar
from lib.print_formatter import ColorFormatter
from lib.split_file import split_file
from lib.cmdline import CmdLineParser
from lib.database import Database
import subprocess
import platform
from multiprocessing import Process


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
        # 这里是用, 分隔的 csv文件
        line = csv.reader(r, delimiter=',', quoting=csv.QUOTE_NONE)
        columns_str = ''

        # 计算 csv 文件的行
        cmd = "wc -l %s | awk '{print $1}'" % one_split_file
        debug_logger.debug('开始运行 {} 命令'.format(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        (stdout, stderr) = p.communicate()
        return_code = p.returncode
        if return_code != 0 or stderr:
            ColorFormatter.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            debug_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            error_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(one_split_file, str(stderr.strip())))
            # 异常退出
            sys.exit(1)
        else:
            ColorFormatter.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(one_split_file, str(stdout.strip())))
            debug_logger.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(one_split_file, str(stdout.strip())))

        count = 1

        # 这里可能会出错 -> _csv.Error: field larger than field limit (131072)
        for all_cols in line:
            # 跳过空行
            if all_cols:
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
                        # 不为第一行
                        #  剔除 \r\n 的列
                        all_cols = escape_file_string(all_cols)
                        # 将每一列的数据拼成一行
                        data_list = ', '.join(['"{}"'.format(_) for _ in all_cols])
                        database_obj.insert_data(table_name=table_name, column_list=columns_str,
                                                 data_list=data_list, skip_error=skip_error)
                else:
                    # 不为第一行
                    #  剔除 \r\n 的列
                    all_cols = escape_file_string(all_cols)
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


def escape_file_string(string_list):
    """
    wash data and split / ' " \r \n \t etc bad data
    剔除和修改文件中的 \ \r \n \t " ' 的坏数据
    仅仅适用于文件 string
    :param string_list: string list
    :return: 源数据
    """

    # 剔除 \ \r \n \t " ' 的坏数据
    if isinstance(string_list, list):
        string_list = [ _.replace('\\', '\\\\').replace('\r', '').replace('\n', '').replace('\t', '').replace('"', '\\"').replace("'", "\\'") for _ in string_list ]
    else:
        string_list =  string_list.replace('\\', '\\\\').replace('\r', '').replace('\n', '').replace('\t', '').replace('"', '\\"').replace("'", "\\'")
    return string_list


def insert_txt_to_database(txt_files, separator, database_obj, column_list, debug_logger, error_logger,
                           table_name=TABLE_NAME, skip_error=False):
    cmd = 'dos2unix {}'.format(txt_files)
    debug_logger.debug('开始运行 {} 命令'.format(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()
    return_code = p.returncode

    if return_code == 0 and not 'Skipping' in str(stderr.strip()):
        ColorFormatter.info("dos2unix 命令转换成功")
        debug_logger.info("dos2unix 命令转换成功")

        # 计算文件的行
        cmd = "wc -l %s | awk '{print $1}'" % txt_files
        debug_logger.debug('开始运行 {} 命令'.format(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        (stdout, stderr) = p.communicate()
        return_code = p.returncode
        if return_code == 0 and not stderr:
            ColorFormatter.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(txt_files, str(stdout.strip())))
            debug_logger.info('计算 "{}" 文件行数结束, 一共 "{}" 行'.format(txt_files, str(stdout.strip())))

            # 处理数据
            with open(txt_files, 'r') as r:
                # 进度条
                progress_bar = ProgressBar(total_line=int(stdout.strip()), description="插入数据中")

                # 创建表
                database_obj.create_table(table_name=table_name, table_column_list=column_list)
                # 处理每一行的数据
                for line in r:
                    # 过滤掉 空行
                    if line:
                        # 去掉两头的空格
                        line = line.strip()
                        # 清洗坏行
                        data_line_list = []
                        lines = line.split(separator)
                        lines = escape_file_string(lines)
                        lines = [ _.strip() for _ in lines if _ ]

                        if len(lines) == len(column_list):
                            data_line_list = lines
                        elif len(lines) > len(column_list):
                            # split出来的长度 大于 column_list 的长度
                            for i in range(len(column_list)):
                                data_line_list.append(lines[i])
                        else:
                            # split 出来的长度 小于 column_list 的长度, 剩余的用 空字符 填充
                            data_line_list = lines
                            for _ in range(len(column_list) - len(data_line_list)):
                                data_line_list.append('')

                        # 将每一列的数据拼成一行
                        data_list = ', '.join(['"{}"'.format(_) for _ in data_line_list])

                        # 执行sql语句
                        column_str = ", ".join(column_list)
                        database_obj.insert_data(table_name=table_name, column_list=column_str, data_list=data_list, skip_error=skip_error)
                        progress_bar.handle_progress()

                # 事务提交, 插入数据
                debug_logger.debug('execute insert sql commit')
                database_obj.execute_commit()
                debug_logger.info('execute insert sql commit successful')

            r.close()
            debug_logger.info('insert data to database done')

        else:
            # 计算行数的时候出错了
            ColorFormatter.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(txt_files, str(stderr.strip())))
            debug_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(txt_files, str(stderr.strip())))
            error_logger.error('计算文件 "{}" 行数时出错, 出错信息为: {}'.format(txt_files, str(stderr.strip())))
            # 异常退出
            sys.exit(1)

    else:
        ColorFormatter.error('dos2unix转换时出错, 出错信息为: {}'.format(str(stderr.strip())))
        debug_logger.error('dos2unix转换时出错, 出错信息为: {}'.format(str(stderr.strip())))
        error_logger.error('dos2unix转换时出错, 出错信息为: {}'.format(str(stderr.strip())))
        # 异常退出
        sys.exit(1)


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
        database = Database(host='127.0.0.1', database=opts.database_name, logger=logger)
    except pymysql.err.InternalError as e:
        ColorFormatter.error('Init database InternalError: {}'.format(e))
        debug_logger.error('Init database InternalError: {}'.format(e))
        error_logger.error('Init database InternalError: {}'.format(e))
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
        ColorFormatter.error('Init database Unknow Error: {}'.format(e))
        debug_logger.error('Init database Unknow Error: {}'.format(e))
        error_logger.error('Init database Unknow Error: {}'.format(e))
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
                if prompt_result == 'y':
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
                        if return_code != 0 or stderr:
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
                        if return_code != 0 or stderr:
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
                            # 每 2 秒读一次总数文件
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
                                # 有可能这个判断不准确, 然后卡在这里
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
                        if return_code != 0 or stderr:
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
                            # 这里是用, 分隔的 csv文件
                            line = csv.reader(r, delimiter=',', quoting=csv.QUOTE_NONE)
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
                            if return_code != 0 or stderr:
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
                                # 跳过空行
                                if all_cols:
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
                                        # 不为第一行
                                        #  剔除 \r\n 的列
                                        all_cols = escape_file_string(all_cols)
                                        # 将每一列的数据拼成一行
                                        data_list = ', '.join(['"{}"'.format(_) for _ in all_cols])
                                        database.insert_data(table_name=table_name, column_list=columns_str,
                                                             data_list=data_list, skip_error=opts.skip_error)

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

            if opts.txt_to_database:
                txt_files = opts.txt_to_database
                table_name = opts.table_name
                skip_error = opts.skip_error
                # 用来分隔 txt 文件的列的
                separator = opts.separator
                column_list = opts.column_list

                if not column_list:
                    ColorFormatter.error('Must with -c option if you want to use txt-to-database option')
                    ColorFormatter.fatal('sleep 2s redirect help page')
                    time.sleep(2)
                    print()
                    CmdLineParser().cmd_parser(get_help=True)

                if not table_name:
                    table_name = TABLE_NAME

                if separator == '\\t':
                    separator = '\t'
                insert_txt_to_database(txt_files=txt_files, separator=separator, database_obj=database,
                                       column_list=column_list, debug_logger=debug_logger, error_logger=error_logger,
                                       table_name=table_name, skip_error=skip_error)

                # 正常退出程序
                sys.exit(0)

            if opts.mysql_command:
                cmd = opts.mysql_command
                dict_result = database.execute_command(cmd)
                for i in dict_result:
                    print(i)

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
        ColorFormatter.error('Total program Unknow error: {}, type: {}'.format(e, type(e)))
        ColorFormatter.fatal('Please contact author')
        debug_logger.error('Total program Unknow error: {}, type: {}'.format(e, type(e)))
        error_logger.error('Total program Unknow error: {}, type: {}'.format(e, type(e)))


if __name__ == '__main__':
    main()
