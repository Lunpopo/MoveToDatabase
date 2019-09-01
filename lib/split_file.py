# encoding: utf8
#!/usr/bin/env python3
from lib.print_formatter import ColorFormatter
from lib.Logger import Logger
from lib.settings import SPLIT_PATH
import subprocess
import sys
import os


def split_file(file_path, log_level, split_count=os.cpu_count()):
    """
    有可能每个系统的换行符不一样, 可能会造成数据分割之后的数据"变多"
    例如:
    如果有一行的数据为: CHEN^M GUIDE,,,VSA,USER,NAME, Python 会把 ^M 当成换行符, 从而数据变成两行:
    1. CHEN
    2.  GUIDE,,,VSA,USER,NAME,

    这个只能当做坏数据处理, 使用 error.log 手动导入即可, 不过这种情况应该不会很多, so do not worry about it.

    :param file_path: 需要分割文件的文件路径
    :param log_level: log level
    :param split_count: 需要分成多少份
    :return: Boolean, 成功或者失败
    """

    try:
        # setting Logger object
        logger = Logger(log_level=log_level)
        # debug.log 应该包含所有的日志信息, 如果命令行没有指定 log_level 为3 的话, debug信息 不会出现在 debug.log 中
        debug_logger = logger.get_logger(logger_name='debug_logger')
        # error.log 只包含错误信息
        error_logger = logger.get_logger(logger_name='error_logger')
        # normalization file path
        file_path = '\ '.join(file_path.split())

        ColorFormatter.info('开始计算需要分割的文件 "{}" 的行数'.format(file_path))
        debug_logger.info('开始计算需要分割的文件 "{}" 的行数'.format(file_path))
        # 计算 csv 文件的行
        cmd = "wc -l %s | awk '{print $1}'" % file_path
        debug_logger.debug('开始运行 {} 命令'.format(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        (stdout, stderr) = p.communicate()
        return_code = p.returncode
        if return_code != 0:
            ColorFormatter.error('计算需要分割文件 "{}" 行数时出错, 出错信息为: {}'.format(file_path, stderr.strip()))
            debug_logger.error('计算需要分割文件 "{}" 行数时出错, 出错信息为: {}'.format(file_path, stderr.strip()))
            error_logger.error('计算需要分割文件 "{}" 行数时出错, 出错信息为: {}'.format(file_path, stderr.strip()))
            ColorFormatter.fatal('Please contact author or look through logs/error.log file to examine error place')
            # 异常退出
            sys.exit(1)
        else:
            ColorFormatter.info('计算需要分割文件 "{}" 行数结束, 一共 {} 行'.format(file_path, str(stdout.strip())))
            debug_logger.debug('计算需要分割文件 "{}" 行数命令结果, 返回码: {}, 输出为: {}'.format(
                file_path, return_code, stdout.strip()))
            file_line = int(stdout.strip())
            debug_logger.info('计算需要分割文件 "{}" 行数结束, 一共 {} 行'.format(file_path, str(file_line)))

        if file_line > 0:
            per_file_line = int(file_line / split_count)
        else:
            per_file_line = 1

        # 计数器
        flag = 0
        # 文件名
        name = 1
        # 存放数据
        data_list = []

        ColorFormatter.info('开始分割文件')
        debug_logger.info('开始分割文件')

        file_path = ''.join(file_path.split('\\'))
        try:
            with open(file_path, 'r') as r:
                for line in r:
                    flag += 1
                    data_list.append(line)

                    # 如果等于设置的每个文件的行数, 就分一个文件, 直到全部分完
                    if flag == per_file_line:
                        split_file = os.path.join(SPLIT_PATH, 'split_file_{}.txt'.format(str(name)))
                        with open(split_file, 'w+') as f:
                            for data in data_list:
                                f.write(data)
                        ColorFormatter.info("文件{}: {} 分割完成".format(name, split_file))
                        debug_logger.info("文件{}: {} 分割完成".format(name, split_file))

                        # 分割一次 重新初始化一次
                        name += 1
                        flag = 0
                        data_list = []
        except Exception as e:
            ColorFormatter.error('分割文件 {} 时候出错, 出错信息为: {}'.format(name, e))
            ColorFormatter.fatal('Please contact author')
            debug_logger.error('分割文件 {} 时候出错, 出错信息为: {}'.format(name, e))
            error_logger.error('分割文件 {} 时候出错, 出错信息为: {}'.format(name, e))

        try:
            if name != split_count:
                # 如果不相等, 就是还有多出来的数据
                last_split_file = os.path.join(SPLIT_PATH, 'split_file_{}.txt'.format(str(name - 1)))
                # 处理最后一批
                with open(last_split_file,'a+') as f_target:
                    for data in data_list:
                        f_target.write(data)
        except Exception as e:
            ColorFormatter.error('分割文件 合并最后一个文件时出错, 出错信息为: {}'.format(e))
            ColorFormatter.fatal('Please contact author')
            debug_logger.error('分割文件 合并最后一个文件时出错, 出错信息为: {}'.format(e))
            error_logger.error('分割文件 合并最后一个文件时出错, 出错信息为: {}'.format(e))

        ColorFormatter.success("文件 {} 分割完成".format(file_path))
        debug_logger.info("文件 {} 分割完成".format(file_path))
        return True

    except:
        return False
